class ResultSet(object):
    """Result of the scan & query operation of the model."""

    def __init__(self, model, operation, kwargs):
        self.model = model
        self.operation = operation
        self.kwargs = kwargs

    def __iter__(self):
        """Result items of the operation."""
        operation = self._get_operation()
        kwargs = self.kwargs.copy()
        limit = kwargs.get('limit', None)
        while True:
            result = operation(self.model.get_table_name(), **kwargs)
            for raw_item in result.get('Items'):
                if limit is not None:
                    limit -= 1
                yield self.model.from_raw_data(raw_item)

            last_evaluated_key = result.get('LastEvaluatedKey', None)
            if not self._prepare_next_fetch(kwargs, last_evaluated_key, limit):
                raise StopIteration

    def count(self):
        """Total count of the matching items.

        It sums up the count of partial results, and returns the total count of
        matching items in the table.
        """
        count = 0
        operation = self._get_operation()
        kwargs = self.kwargs.copy()
        kwargs['select'] = 'COUNT'
        limit = kwargs.get('limit', None)
        while True:
            result = operation(self.model.get_table_name(), **kwargs)
            count += result['Count']
            if limit is not None:
                limit -= result['Count']

            last_evaluated_key = result.get('LastEvaluatedKey', None)
            if not self._prepare_next_fetch(kwargs, last_evaluated_key, limit):
                break
        return count

    def _prepare_next_fetch(self, kwargs, last_evaluated_key, limit):
        if last_evaluated_key and (limit is None or limit > 0):
            kwargs['exclusive_start_key'] = last_evaluated_key
            kwargs['limit'] = limit
            return True
        return False


    def _get_operation(self):
        return getattr(self.model._get_connection(), self.operation)
