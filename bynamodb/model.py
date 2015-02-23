import copy

from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.types import Dynamizer

from .attributes import Attribute
from .conditions import KEY_CONDITIONS, build_condition
from .exceptions import NullAttributeException, ItemNotFoundException
from .indexes import Index, GlobalIndex
from .results import ResultSet


class ModelMeta(type):
    """Model meta class"""
    def __new__(mcs, clsname, bases, dct):
        for name, val in dct.items():
            if isinstance(val, Attribute):
                val.attr_name = name
            elif type(val) == type and issubclass(val, Index):
                val._keys = [HashKey(val.hash_key, dct[val.hash_key].type)]
                if val.range_key:
                    val._keys.append(RangeKey(val.range_key,
                                              dct[val.range_key].type))
        return super(ModelMeta, mcs).__new__(mcs, clsname, bases, dct)


class Model(object):
    """Defines schema of a DynamoDB table with
    :class:`~bynamodb.attributes.Attribute` and
    :class:`~bynamodb.attributes.indexes.Index`.
    You can execute APIs of the table through the model.

    """
    __metaclass__ = ModelMeta

    #: (:class:`str`) The table name.
    #: # If omitted, the Model class name will be the table name.
    table_name = None

    #: (:class:`str`) The prefix of table name.
    #: If not empty string and the model does not have defined table name,
    #: the table name would consists of the prefix and the class name
    #: of the model.
    _table_prefix = ''

    _attributes = None
    _conn = None
    _keys = None
    _indexes = None

    def __init__(self, **data):
        """An object of the Model represents an item of the model.

        :param data: key value of the item.
        :type data: :class:`collections.Mapping`

        """
        self._data = {}
        self._set_defaults()
        for name, value in data.items():
            if name in dir(self):
                setattr(self, name, value)
            else:
                continue

    def _set_defaults(self):
        for attr in self._get_attributes().values():
            if attr.default is not None:
                value = copy.copy(attr.default)
                if callable(value):
                    value = value()
                setattr(self, attr.attr_name, value)

    def validate(self):
        for name, attr in self._get_attributes().items():
            attr_value = getattr(self, name, None)
            if not attr_value:
                if not attr.null:
                    raise NullAttributeException(
                        'Attribute {0} cannot be null'.format(name))
                else:
                    continue

    def save(self):
        self._put_item(self)

    def delete(self):
        key_fields = [key.name for key in self._get_keys()]
        key = dict((key, getattr(self, key)) for key in key_fields)
        return self._get_connection().delete_item(self.get_table_name(), key)

    @classmethod
    def create_table(cls, read_throughput=5, write_throughput=5):
        """Create the table as the schema definition."""
        table_name = cls.get_table_name()

        raw_throughput = {
            'ReadCapacityUnits': read_throughput,
            'WriteCapacityUnits': write_throughput
        }

        table_schema = []
        table_definitions = []
        seen_attrs = set()
        for key in cls._get_keys():
            table_schema.append(key.schema())
            table_definitions.append(key.definition())
            seen_attrs.add(key.name)

        indexes = []
        global_indexes = []
        for index in cls._get_indexes():
            if issubclass(index, GlobalIndex):
                global_indexes.append(index.schema())
            else:
                indexes.append(index.schema())
            for key in index._keys:
                if key.name not in seen_attrs:
                    table_definitions.append(key.definition())
                    seen_attrs.add(key.name)

        cls._get_connection().create_table(
            table_name=table_name,
            key_schema=table_schema,
            attribute_definitions=table_definitions,
            provisioned_throughput=raw_throughput,
            local_secondary_indexes=indexes or None,
            global_secondary_indexes=global_indexes or None
        )

    @classmethod
    def put_item(cls, **data):
        """Put item to the table.

        :param data: key value of the item.
        :type data: :class:`collections.Mapping`

        """
        cls._put_item(cls(**data))

    @classmethod
    def _put_item(cls, item):
        item.validate()
        data = {}
        for name, attr in cls._get_attributes().items():
            attr_value = getattr(item, name, None)
            if not attr_value:
                continue
            data[attr.attr_name] = attr.encode(attr_value)
        cls._get_connection().put_item(cls.get_table_name(), data)

    @classmethod
    def get_item(cls, hash_key, range_key=None):
        """ Get item from the table."""
        key = cls._encode_key(hash_key, range_key)
        raw_data = cls._get_connection().get_item(cls.get_table_name(), key)
        if 'Item' not in raw_data:
            raise ItemNotFoundException
        return cls.from_raw_data(raw_data['Item'])

    @classmethod
    def query(cls, index_name=None, filter_builder=None, **key_conditions):
        """High level query API.

        :param key_filter: key conditions of the query.
        :type key_filter: :class:`collections.Mapping`
        :param filter_builder: filter expression builder.
        :type filter_builder: :class:`~bynamodb.filterexps.Operator`
        """
        query_kwargs = {
            'key_conditions': build_condition(key_conditions, KEY_CONDITIONS),
            'index_name': index_name
        }
        if filter_builder:
            cls._build_filter_expression(filter_builder, query_kwargs)
        return ResultSet(cls, 'query', query_kwargs)

    @classmethod
    def scan(cls, filter_builder=None, **scan_filter):
        """High level scan API.

        :param filter_builder: filter expression builder.
        :type filter_builder: :class:`~bynamodb.filterexps.Operator`

        """
        scan_kwargs = {'scan_filter': build_condition(scan_filter)}
        if filter_builder:
            cls._build_filter_expression(filter_builder, scan_kwargs)
        return ResultSet(cls, 'scan', scan_kwargs)

    @classmethod
    def batch_get(cls, *args):
        unprocessed = [cls._encode_key(*key) for key in args]
        while unprocessed:
            nexts = unprocessed[:100]
            unprocessed = unprocessed[100:]
            request_items = {
                cls.get_table_name():
                {
                    'Keys': nexts
                }
            }
            result = cls._get_connection().batch_get_item(request_items)
            for item in result['Responses'].get(cls.get_table_name()):
                yield cls.from_raw_data(item)
            for i, key in enumerate(
                    result.get('UnprocessedKeys', {}).get('Keys', [])):
                unprocessed.insert(i, key)
        raise StopIteration

    @classmethod
    def batch_write(cls):
        return BatchWrite(cls)

    @classmethod
    def from_raw_data(cls, item_raw):
        """Translate the raw item data from the DynamoDBConnection
        to the item object.

        """
        deserialized = {}
        for name, attr in item_raw.items():
            deserialized[name] = getattr(cls, name).decode(attr)
        return cls(**deserialized)

    @classmethod
    def _build_filter_expression(cls, filter_builder, kwargs):
        kwargs['filter_expression'], kwargs['expression_attribute_values'] = \
            filter_builder.build_exp()

    @classmethod
    def _encode_key(cls, hash_key, range_key=None):
        dynamizer = Dynamizer()
        encoded = {cls._get_hash_key().name: dynamizer.encode(hash_key)}
        if range_key:
            encoded.update(
                {cls._get_range_key().name: dynamizer.encode(range_key)})
        return encoded

    @classmethod
    def get_table_name(cls):
        return cls.table_name or '%s%s' % (cls._table_prefix, cls.__name__)

    @classmethod
    def _get_keys(cls):
        if cls._keys:
            return cls._keys
        hash_key = None
        range_key = None
        for attr in cls._get_attributes().values():
            if attr.hash_key:
                hash_key = HashKey(attr.attr_name, attr.type)
            elif attr.range_key:
                range_key = RangeKey(attr.attr_name, attr.type)
        cls._keys = [key for key in [hash_key, range_key] if key]
        return cls._keys

    @classmethod
    def _get_hash_key(cls):
        return cls._get_keys()[0]

    @classmethod
    def _get_range_key(cls):
        return cls._get_keys()[1]

    @classmethod
    def _get_attributes(cls):
        if cls._attributes:
            return cls._attributes
        cls._attributes = {}
        for item_name in dir(cls):
            item_cls = getattr(getattr(cls, item_name), '__class__', None)
            if item_cls is not None and issubclass(item_cls, Attribute):
                cls._attributes[item_name] = getattr(cls, item_name)
        return cls._attributes

    @classmethod
    def _get_indexes(cls):
        if cls._indexes:
            return cls._indexes
        cls._indexes = []
        for item_name in dir(cls):
            attr = getattr(cls, item_name)
            if type(attr) == type and issubclass(attr, Index):
                cls._indexes.append(attr)
        return cls._indexes

    @classmethod
    def _get_connection(cls):
        if cls._conn:
            return cls._conn
        cls._conn = DynamoDBConnection()
        return cls._conn


class BatchWrite(object):

    def __init__(self, model):
        self.model = model
        self.to_put = []
        self.to_delete = []

    def put_item(self, **data):
        self.model(**data).validate()
        self.to_put.append(
            {
                'PutRequest': {
                    'Item': data
                }
            }
        )

    def delete_item(self, *keys):
        key = self.model._encode_key(*keys)
        self.to_delete.append(
            {
                'DeleteRequest': {
                    'Key': key
                }
            }
        )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.send_request()

    def send_request(self):
        unprocessed = []
        unprocessed.extend(self.to_put)
        unprocessed.extend(self.to_delete)
        while unprocessed:
            nexts = unprocessed[:25]
            unprocessed = unprocessed[25:]
            batch_request = {
                self.model.get_table_name(): nexts
            }
            result = (self.model.
                      _get_connection().batch_write_item(batch_request))
            unprocessed.extend(result.get('UnprocessedItems', {}).
                               get(self.model.get_table_name(), []))
