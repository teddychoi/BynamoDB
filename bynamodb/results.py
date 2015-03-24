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
        while True:
            result = operation(self.model.get_table_name(), **kwargs)
            for raw_item in result.get('Items'):
                yield self.model.from_raw_data(raw_item)

            last_evaluated_key = result.get('LastEvaluatedKey', None)
            if last_evaluated_key:
                kwargs['exclusive_start_key'] = last_evaluated_key
            else:
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
        while True:
            result = operation(self.model.get_table_name(), **kwargs)
            count += result['Count']

            last_evaluated_key = result.get('LastEvaluatedKey', None)
            if last_evaluated_key:
                kwargs['exclusive_start_key'] = last_evaluated_key
            else:
                break
        return count

    def _get_operation(self):
        return getattr(self.model._get_connection(), self.operation)
