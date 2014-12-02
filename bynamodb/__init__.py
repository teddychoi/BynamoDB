from boto.dynamodb2.layer1 import DynamoDBConnection


def patch_dynamodb_connection(**kwargs):
    if hasattr(DynamoDBConnection, '__original_init__'):
        return

    DynamoDBConnection.__original_init__ = DynamoDBConnection.__init__

    def init(self, **fkwargs):
        fkwargs.update(kwargs)
        self.__original_init__(**fkwargs)

    DynamoDBConnection.__init__ = init
