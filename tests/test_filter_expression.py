from _pytest.python import fixture

from bynamodb.filterexps import Contains, GT, OR


@fixture
def fx_test_contain_operator():
    return Contains('content', 'keyword')


@fixture
def fx_test_gt_operator():
    return GT('birth_year', 1994)


def test_build_contains(fx_test_contain_operator):
    filter_exp, attr_values = fx_test_contain_operator.build_exp()
    assert filter_exp == 'contains(content, :1)'
    assert attr_values == {':1': {'S': 'keyword'}}


def test_build_gt(fx_test_gt_operator):
    filter_exp, attr_values = fx_test_gt_operator.build_exp()
    assert filter_exp == 'birth_year > :1'
    assert attr_values == {':1': {'N': '1994'}}


def test_build_or(fx_test_contain_operator, fx_test_gt_operator):
    filter_exp, attr_values = OR(
        fx_test_contain_operator, fx_test_gt_operator).build_exp()
    assert filter_exp == '(contains(content, :1) or birth_year > :2)'
    assert attr_values[':1'] == {'S': 'keyword'}
    assert attr_values[':2'] == {'N': '1994'}


def test_nest_or(fx_test_contain_operator, fx_test_gt_operator):
    filter_exp, attr_values = \
        (fx_test_contain_operator | fx_test_gt_operator).build_exp()
    assert filter_exp == '(contains(content, :1) or birth_year > :2)'
    assert attr_values[':1'] == {'S': 'keyword'}
    assert attr_values[':2'] == {'N': '1994'}
