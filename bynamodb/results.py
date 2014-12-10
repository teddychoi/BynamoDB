class Result(object):
    """The result of the :func:`~bynamodb.Model.query` and
    :func:`~bynamodb.Model.scan`.

    """
    def __init__(self, model, result_set):
        self.model = model
        self.consumed_capacity = result_set.get('ConsumedCapacity')
        self.count = result_set.get('Count')
        self.items = [model.from_raw_data(item)
                      for item in result_set.get('Items')]
        self.last_evaluated_key = result_set.get('LastEvaluatedKey')
        self.scanned_count = result_set.get('ScannedCount')

    def __iter__(self):
        return iter(self.items)
