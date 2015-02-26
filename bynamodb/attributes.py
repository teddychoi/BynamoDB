from boto.dynamodb.types import Dynamizer
from boto.dynamodb2.types import (STRING, STRING_SET, BINARY, BINARY_SET,
                                  NUMBER_SET, LIST, MAP, BOOLEAN, NUMBER)


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
            raise ValueError(self.get_invalidation_message(value))
        return self._encode(value)

    def get_invalidation_message(self, value):
        raise NotImplementedError

    def _encode(self, value):
        return Dynamizer().encode(value)

    def decode(self, value):
        return Dynamizer().decode(value)


class ScalarAttribute(Attribute):

    # (:class:`tuple`) Acceptable types for the value used in encoding
    accepts = None

    @classmethod
    def valid(cls, value):
        return type(value) in cls.accepts

    def get_invalidation_message(self, value):
        return (
            '{0} is not valid for {1}.'
            'The type of value must be in {2}'.format(
                value, self.attr_name, self.accepts
            ))


class StringAttribute(ScalarAttribute):
    type = STRING
    accepts = (str, unicode)


class BinaryAttribute(ScalarAttribute):
    type = BINARY
    accepts = str,


class NumberAttribute(ScalarAttribute):
    type = NUMBER
    accepts = int, float,

    def _encode(self, value):
        return Dynamizer().encode(value)

    def decode(self, value):
        value = str(Dynamizer().decode(value))
        if '.' in value:
            return float(value)
        else:
            return int(value)


class BooleanAttribute(ScalarAttribute):
    type = BOOLEAN
    accepts = bool,


class DocumentAttribute(ScalarAttribute):
    pass


class ListAttribute(DocumentAttribute):
    type = LIST
    accepts = list,


class MapAttribute(DocumentAttribute):
    type = MAP
    accepts = dict,


class SetAttribute(Attribute):

    # (:class:`~bynamodb.attributes.ScalarAttribute`)
    # The type of the elements.
    set_of = None

    @classmethod
    def valid(cls, value):
        return (type(value) is set and
                all(cls.set_of.valid(elem) for elem in value))

    def get_invalidation_message(self, value):
        return (
            '{0} is not valid for {1}.'
            'The type of value must be a set of a type in {2}'.format(
                value, self.attr_name, self.set_of.accepts
            ))


class StringSetAttribute(SetAttribute):
    type = STRING_SET
    set_of = StringAttribute


class BinarySetAttribute(SetAttribute):
    type = BINARY_SET
    set_of = BinaryAttribute


class NumberSetAttribute(SetAttribute):
    type = NUMBER_SET
    set_of = NumberAttribute
