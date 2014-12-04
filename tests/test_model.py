from _pytest.python import raises, fixture
from boto.dynamodb2.types import STRING
from boto.dynamodb2.layer1 import DynamoDBConnection
from bynamodb.exceptions import NullAttributeException
from bynamodb.filter_expression import GT

from bynamodb.model import Attribute, Model


@fixture
def test_model():
    class TestModel(Model):
        hash_key_attr = Attribute(STRING, hash_key=True)
        range_key_attr = Attribute(STRING, range_key=True)
        attr_1 = Attribute(STRING)
    return TestModel


def test_create_table(test_model):
    test_model.create_table()
    table_description = DynamoDBConnection().describe_table(test_model.get_table_name())[u'Table']
    expected_key_name = {
        'HASH': 'hash_key_attr',
        'RANGE': 'range_key_attr'
    }
    for key in table_description[u'KeySchema']:
        assert expected_key_name[key[u'KeyType']] == key[u'AttributeName']


def test_get_item(test_model):
    test_model.create_table()

    hash_key_value = 'Hash Key Value'
    range_key_value = 'Range Key Value'
    attr1_value = 'Attribute1 Value'
    test_model.put_item({
        'hash_key_attr': hash_key_value,
        'range_key_attr': range_key_value,
        'attr_1': attr1_value
    })

    item = test_model.get_item(hash_key_value, range_key_value)
    assert item.hash_key_attr == hash_key_value
    assert item.range_key_attr == range_key_value
    assert item.attr_1 == attr1_value


def test_put_item(test_model):
    test_model.create_table()
    hash_key_value = 'Hash Key Value'
    range_key_value = 'Range Key Value'
    attr1_value = 'Attribute1 Value'
    attrs = {
        'hash_key_attr': hash_key_value,
        'range_key_attr': range_key_value,
        'attr_1': attr1_value
    }
    test_model.put_item(attrs)

    item = test_model.get_item(hash_key=hash_key_value, range_key=range_key_value)
    assert item.hash_key_attr == hash_key_value
    assert item.range_key_attr == range_key_value
    assert item.attr_1 == attr1_value


def test_put_item_with_missing_attr(test_model):
    test_model.create_table()
    attrs = {
        'hash_key_attr': 'hash_key',
        'range_key_attr': 'range_key'
    }
    with raises(NullAttributeException):
        test_model.put_item(attrs)


def test_save_item_with_missing_attr(test_model):
    test_model.create_table()
    item = test_model({
        'hash_key_attr': 'hash_key',
        'range_key_attr': 'range_key'
    })
    with raises(NullAttributeException):
        item.save()


@fixture
def model_with_nullable_attr():
    class TestModelWithNullable(Model):
        hash_key = Attribute(STRING, hash_key=True)
        attr = Attribute(STRING, null=True)
    return TestModelWithNullable


def test_save_item_nullable_attr_emptied(model_with_nullable_attr):
    model_with_nullable_attr.create_table()
    hash_key_value = 'value'
    model_with_nullable_attr({
        'hash_key': hash_key_value
    }).save()
    item = model_with_nullable_attr.get_item(hash_key_value)
    assert item.hash_key == hash_key_value


@fixture
def fx_query_test_model():
    class QueryTestModel(Model):
        title = Attribute(STRING, hash_key=True)
        content = Attribute(STRING)
    QueryTestModel.create_table()
    return QueryTestModel


@fixture
def fx_query_test_items(fx_query_test_model):
    for ch in ['a', 'b', 'c', 'd', 'e']:
        fx_query_test_model.put_item({'title': ch * 5, 'content': ch.upper() * 5})


def test_scan(fx_query_test_model, fx_query_test_items):
    result = fx_query_test_model.scan()
    assert len(result.items) == 5
    assert all(type(item) == fx_query_test_model for item in result)
    assert all(item.title for item in result)


def test_scan_with_filter_operator(fx_query_test_model, fx_query_test_items):
    gt = GT('content', 'B')
    result = fx_query_test_model.scan(filter_builder=gt)
    assert len(result.items) == 4
    for item in result:
        print item.content
    assert all([item.content > 'B' for item in result])
