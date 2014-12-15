from _pytest.python import raises

from bynamodb.conditions import KEY_CONDITIONS, build_condition
from bynamodb.exceptions import ConditionNotRecognizedException


def test_null_condition():
    filters = build_condition({'title__null': True})
    assert filters['title']['ComparisonOperator'] == 'NULL'
    assert 'AttributeValues' not in filters['title']


def test_condition_not_in_key_conditions():
    with raises(ConditionNotRecognizedException):
        build_condition({'title__contains': 'title'}, KEY_CONDITIONS)
