from bynamodb.attributes import (NumberAttribute, StringAttribute,
                                 NumberSetAttribute, StringSetAttribute,
                                 ListAttribute, MapAttribute)


def test_number_attribute_validation():
    assert NumberAttribute.valid(123)
    assert NumberAttribute.valid(123.4)
    assert not NumberAttribute.valid('1234')


def test_string_attribute_validation():
    assert StringAttribute.valid('1234')
    assert StringAttribute.valid(u'1234')
    assert not StringAttribute.valid(1234)


def test_empty_set_valid_for_set_attributes():
    assert StringSetAttribute.valid(set())
    assert NumberSetAttribute.valid(set())


def test_set_attributes_valid_with_expected_types():
    assert StringSetAttribute.valid({'a', 'b'})
    assert NumberSetAttribute.valid({1, 2})


def test_set_attributes_not_valid_for_unexpected_types():
    assert not StringSetAttribute.valid({1, 2})
    assert not NumberSetAttribute.valid({'a', 'b'})


def test_list_attribute_validation():
    assert ListAttribute.valid([])
    assert ListAttribute.valid([1, 2])
    assert ListAttribute.valid([1, '2'])


def test_map_attribute_validation():
    assert MapAttribute.valid({})
    assert MapAttribute.valid({'1': 2})
