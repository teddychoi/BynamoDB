from decimal import Decimal
from boto.dynamodb.types import Dynamizer
from boto.dynamodb2.types import (STRING, STRING_SET, BINARY, BINARY_SET,
                                  NUMBER, NUMBER_SET)


class Attribute(object):
    """Declare the attribute of the model as a descriptor."""

    # (:class:`str`) The attribute name. It is assigned by Model meta class.
    attr_name = None

    # (:class:`bool`) `True` if the attribute is the hash key of the model.
    hash_key = False

    # (:class:`bool`) `True` if the attribute is the range key of the model.
    range_key = False

    # (:class:`str`) Type string defined in :mod:`boto.dynamodb2.types`
    type = None

    def __init__(self, hash_key=False, range_key=False,
                 null=False, default=None):
        self.hash_key = hash_key
        self.range_key = range_key
        self.default = default
        if default is not None:
            self.null = True
        else:
            self.null = null

    def __get__(self, obj, cls=None):
        if obj is not None:
            return obj._data.get(self.attr_name)
        return self

    def __set__(self, obj, value):
        if obj is not None:
            obj._data[self.attr_name] = value
            return
        raise ValueError('Cannot change the class attribute')

    @classmethod
    def valid(cls, value):
        raise NotImplementedError

    def encode(self, value):
        if not self.valid(value):
            raise ValueError(
                '{0} is not valid for {1}. expected type: {2}'.format(
                    value, self.attr_name, self.type
                )
            )
        return self._encode(value)

    def _encode(self, value):
        return {self.type: value}

    def decode(self, value):
        return Dynamizer().decode(value)


class StringAttribute(Attribute):
    type = STRING

    @classmethod
    def valid(cls, value):
        return type(value) in (str, unicode)


class BinaryAttribute(Attribute):
    type = BINARY

    @classmethod
    def valid(cls, value):
        return type(value) is str


class NumberAttribute(Attribute):
    type = NUMBER

    @classmethod
    def valid(cls, value):
        return type(value) in (int, float)

    def decode(self, value):
        value = Dynamizer().decode(value)
        if isinstance(value, Decimal):
            s = str(value)
            if '.' in s:
                return float(s)
            else:
                return int(s)
        return value


class SetAttribute(Attribute):
    set_of = None

    @classmethod
    def valid(cls, value):
        return (type(value) is set and
                all(cls.set_of.valid(elem) for elem in value))

    def _encode(self, value):
        return {self.type: list(value)}


class StringSetAttribute(SetAttribute):
    type = STRING_SET
    set_of = StringAttribute


class BinarySetAttribute(SetAttribute):
    type = BINARY_SET
    set_of = BinaryAttribute


class NumberSetAttribute(SetAttribute):
    type = NUMBER_SET
    set_of = NumberAttribute
