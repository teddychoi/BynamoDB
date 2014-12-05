from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb.types import get_dynamodb_type
from boto.dynamodb2.types import Dynamizer

from bynamodb.exceptions import NullAttributeException
from bynamodb.results import Result


class Attribute(object):
    type = None
    attr_name = None
    hash_key = False
    range_key = False


    def __init__(self, type, hash_key=False, range_key=False, null=False):
        self.type = type
        self.hash_key = hash_key
        self.range_key = range_key
        self.null = null

    def __get__(self, obj, cls=None):
        if isinstance(obj, Model):
            return obj._data.get(self.attr_name)
        return self

    def __set__(self, obj, value):
        if isinstance(obj, Model):
            obj._data[self.attr_name] = value
            return
        raise ValueError('Cannot change the class attribute')


class ModelMeta(type):
    def __new__(cls, clsname, bases, dct):
        for name, val in dct.items():
            if isinstance(val, Attribute):
                val.attr_name = name
        return super(ModelMeta, cls).__new__(cls, clsname, bases, dct)


class Model(object):

    __metaclass__ = ModelMeta

    table_name = None
    _hash_key_name = None
    _range_key_name = None
    _attributes = None
    _conn = None

    def __init__(self, data):
        self._data = {}
        for name, value in data.items():
            if name in dir(self):
                setattr(self, name, value)
            else:
                continue

    def save(self):
        self._put_item(self)

    @classmethod
    def create_table(cls, read_throughput=5, write_throughput=5):
        table_name = cls.get_table_name()

        raw_throughput = {
            'ReadCapacityUnits': read_throughput,
            'WriteCapacityUnits': write_throughput
        }

        hash_key_name = cls._get_hash_key_name()
        hash_key_attr = cls._get_attributes()[hash_key_name]
        hash_key = HashKey(hash_key_name, hash_key_attr.type)

        range_key = None
        range_key_name = cls._get_range_key_name()
        if range_key_name:
            range_key_attr = cls._get_attributes()[range_key_name]
            range_key = RangeKey(range_key_name, range_key_attr.type)

        table_schema = [hash_key]
        if range_key:
            table_schema.append(range_key)
        cls._get_connection().create_table(
            table_name=table_name,
            key_schema=[field.schema() for field in table_schema],
            attribute_definitions=[field.definition() for field in table_schema],
            provisioned_throughput=raw_throughput
        )

    @classmethod
    def query(cls, key_filter, filter_builder=None, **kwargs):
        kwargs['key_conditions'] = cls._build_filter(key_filter)
        if 'query_filter' in kwargs:
            kwargs['query_filter'] = cls._build_filter(kwargs['query_filter'])
        if filter_builder:
            cls._build_filter_expression(filter_builder, kwargs)
        result = cls._get_connection().query(cls.get_table_name(), **kwargs)
        return Result(cls, result)

    @classmethod
    def _build_filter(cls, key_filter):
        filters = {}
        dynamizer = Dynamizer()
        for field_and_op, value in key_filter.items():
            try:
                field, op = field_and_op.split('__')
            except Exception:
                raise ValueError('key filter expression is not valid')
            # TODO: Implement multiple value encoding
            filters[field] = {
                'ComparisonOperator': op.upper(),
                'AttributeValueList': [dynamizer.encode(value)]
            }
        return filters

    @classmethod
    def scan(cls, filter_builder=None, **kwargs):
        if filter_builder:
            cls._build_filter_expression(filter_builder, kwargs)
        result = cls._get_connection().scan(cls.get_table_name(), **kwargs)
        return Result(cls, result)

    @classmethod
    def _build_filter_expression(cls, filter_builder, kwargs):
        kwargs['filter_expression'], kwargs['expression_attribute_values'] = \
            filter_builder.build_exp()

    @classmethod
    def put_item(cls, data, **kwargs):
        cls._put_item(cls(data), **kwargs)

    @classmethod
    def _put_item(cls, item, **kwargs):
        data = {}
        for name, attr in cls._get_attributes().items():
            attr_value = getattr(item, name, None)
            if attr_value is None:
                if not attr.null:
                    raise NullAttributeException('Attribute {0} cannot be null'.format(name))
                else:
                    continue
            attr_value_type = get_dynamodb_type(attr_value)
            if attr_value_type != attr.type:
                raise ValueError('Expected type for {0}: {1}, actual type: {2}'.format(
                    name, attr, attr_value_type
                ))
            data[attr.attr_name] = attr_value
        cls._get_connection().put_item(cls.get_table_name(), data, **kwargs)

    @classmethod
    def get_item(cls, hash_key=None, range_key=None, **kwargs):
        if hash_key:
            kwargs['key'] = cls._encode_key(hash_key, range_key)
        raw_data = cls._get_connection().get_item(cls.get_table_name(),
                                                  **kwargs)
        return cls.deserialize(raw_data['Item'])

    @classmethod
    def deserialize(cls, item_raw):
        dynamizer = Dynamizer()
        deserialized = {}
        for name, attr in item_raw.items():
            deserialized[name] = dynamizer.decode(attr)
        return cls(deserialized)

    @classmethod
    def _encode_key(cls, hash_key, range_key=None):
        dynamizer = Dynamizer()
        encoded = {cls._get_hash_key_name(): dynamizer.encode(hash_key)}
        if range_key:
            encoded.update({cls._get_range_key_name(): dynamizer.encode(range_key)})
        return encoded

    @classmethod
    def get_table_name(cls):
        return cls.table_name or cls.__name__

    @classmethod
    def _get_attributes(cls):
        if cls._attributes:
            return cls._attributes
        cls._attributes = {}
        for item_name in dir(cls):
            item_cls = getattr(getattr(cls, item_name), '__class__', None)
            if item_cls is None:
                continue
            if issubclass(item_cls, Attribute):
                cls._attributes[item_name] = getattr(cls, item_name)
        return cls._attributes

    @classmethod
    def _get_hash_key_name(cls):
        if cls._hash_key_name:
            return cls._hash_key_name
        for name, value in cls._get_attributes().items():
            if value.hash_key:
                cls._hash_key_name = name
                return cls._hash_key_name
        return None

    @classmethod
    def _get_range_key_name(cls):
        if cls._range_key_name:
            return cls._range_key_name
        for name, value in cls._get_attributes().items():
            if value.range_key:
                cls._range_key_name = name
                return cls._range_key_name
        return None

    @classmethod
    def _get_connection(cls):
        if cls._conn:
            return cls._conn
        cls._conn = DynamoDBConnection()
        return cls._conn
