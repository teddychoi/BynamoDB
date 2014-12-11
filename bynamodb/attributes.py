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
        self.null = null
        self.default = default

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


class StringAttribute(Attribute):
    type = STRING

    @classmethod
    def valid(cls, value):
        return type(value) == str


class StringSetAttribute(Attribute):
    type = STRING_SET

    @classmethod
    def valid(cls, value):
        return (type(value) == set and
                all(StringAttribute.valid(elem) for elem in value))


class BinaryAttribute(Attribute):
    type = BINARY

    @classmethod
    def valid(cls, value):
        return type(value) == str


class BinarySetAttribute(Attribute):
    type = BINARY_SET

    @classmethod
    def valid(cls, value):
        return (type(value) == set and
                all(BinaryAttribute.valid(elem) for elem in value))


class NumberAttribute(Attribute):
    type = NUMBER

    @classmethod
    def valid(cls, value):
        return type(value) == int


class NumberSetAttribute(Attribute):
    type = NUMBER_SET

    @classmethod
    def valid(cls, value):
        return (type(value) == set and
                all(NumberAttribute.valid(elem) for elem in value))
