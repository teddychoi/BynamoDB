class Index(object):
    """Declare the index of the model as a class."""

    # (:class:`str`) The attribute name used for the index hash key.
    hash_key = None

    # (:class:`str`) The attribute name used for the index hash key.
    range_key = None

    #: (:class:`str`) The index name.
    #: # If omitted, the Index class name will be the table name.
    index_name = None

    #: (:class:`str`) The projection type of the index.
    projection_type = None

    _keys = None

    @classmethod
    def schema(cls):
        return {
            'IndexName': cls._get_index_name(),
            'KeySchema': [key.schema() for key in cls._keys],
            'Projection': {
                'ProjectionType': cls.projection_type
            }
        }

    @classmethod
    def _get_index_name(cls):
        return cls.index_name or cls.__name__


class GlobalIndex(Index):
    read_throughput = None
    write_throughput = None

    @classmethod
    def schema(cls):
        schema = super(GlobalIndex, cls).schema()
        schema['ProvisionedThroughput'] = {
            'ReadCapacityUnits': int(cls.read_throughput),
            'WriteCapacityUnits': int(cls.write_throughput)
        }
        return schema


class AllIndex(Index):
    projection_type = 'ALL'


class GlobalAllIndex(GlobalIndex):
    projection_type = 'ALL'
