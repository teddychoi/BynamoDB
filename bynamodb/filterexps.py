from boto.dynamodb.types import Dynamizer


class Operator(object):
    """Abstract operators used in the filter expression.

    """
    def build_exp(self):
        """Generate the filter expression string and the attribute values
        used in :class:`boto.dynamodb2.layer1.DynamoDBConnection`.
        """
        attr_values = AttributeValues()
        return self._build_exp(attr_values), attr_values.data

    def _build_exp(self, attr_values):
        raise NotImplemented

    def __and__(self, operator):
        """Compose the operator with another operator using `and`"""
        return AND(self, operator)

    def __or__(self, operator):
        """Compose the operator with another operator using `or`"""
        return OR(self, operator)


class AttributeValues(object):
    def __init__(self):
        self.data = {}
        self._dynamizer = Dynamizer()
        self._current_key = 1

    def insert(self, value):
        key = ':' + str(self._current_key)
        attr_value = self._dynamizer.encode(value)
        self.data[key] = attr_value
        self._current_key += 1
        return key


class LogicalOperator(Operator):
    operator = None

    def __init__(self, op1, op2):
        self.op1 = op1
        self.op2 = op2

    def _build_exp(self, attr_values):
        return '({0} {1} {2})'.format(
            self.op1._build_exp(attr_values),
            self.operator,
            self.op2._build_exp(attr_values)
        )


class OR(LogicalOperator):
    operator = 'or'


class AND(LogicalOperator):
    operator = 'and'


class ComparisonOperator(Operator):
    operator = None

    def __init__(self, attr_name, comparator):
        self.attr_name = attr_name
        self.comparator = comparator

    def _build_exp(self, attr_values):
        key = attr_values.insert(self.comparator)
        return '{0} {1} {2}'.format(
            self.attr_name, self.operator, key
        )


class EQ(ComparisonOperator):
    operator = '='


class GT(ComparisonOperator):
    operator = '>'


class GTE(ComparisonOperator):
    operator = '>='


class LT(ComparisonOperator):
    operator = '<'


class LTE(ComparisonOperator):
    operator = '<='


class Contains(Operator):
    def __init__(self, path, operand):
        self.path = path
        self.operand = operand

    def _build_exp(self, attr_values):
        key = attr_values.insert(self.operand)
        return 'contains({0}, {1})'.format(self.path, key)
