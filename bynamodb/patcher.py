from boto.dynamodb2.layer1 import DynamoDBConnection

from .model import Model


def patch_from_config(config):
    if 'DYNAMODB_CONNECTION' in config:
        patch_dynamodb_connection(**config['DYNAMODB_CONNECTION'])
    if 'DYNAMODB_PREFIX' in config:
        patch_table_name_prefix(config['DYNAMODB_PREFIX'])


def patch_dynamodb_connection(**kwargs):
    """:class:`boto.dynamodb2.layer1.DynamoDBConnection` patcher.

    It partially applies the keyword arguments to the
    :class:`boto.dynamodb2.layer1.DynamoDBConnection` initializer method.
    The common usage of this function would be patching host and port
    to the local DynamoDB or remote DynamoDB as the project configuration
    changes.

    """
    if hasattr(DynamoDBConnection, '__original_init__'):
        return

    DynamoDBConnection.__original_init__ = DynamoDBConnection.__init__

    def init(self, **fkwargs):
        fkwargs.update(kwargs)
        self.__original_init__(**fkwargs)

    DynamoDBConnection.__init__ = init


def patch_table_name_prefix(prefix):
    """Patch the table name prefix"""

    Model._table_prefix = prefix
