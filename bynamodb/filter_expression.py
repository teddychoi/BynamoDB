from boto.dynamodb.types import Dynamizer


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


class Operator(object):
    def build_exp(self, attr_values=None):
        attr_values = attr_values or AttributeValues()
        return self._build_exp(attr_values), attr_values.data

    def _build_exp(self, attr_values):
        raise NotImplemented

    def apply_and(self, operator):
        return AND(self, operator)

    def apply_or(self, operator):
        return OR(self, operator)


class LogicalOperator(Operator):
    operator = None

    def __init__(self, op1, op2):
        self.op1 = op1
        self.op2 = op2

    def _build_exp(self, attr_values):
        return '({0} {1} {2})'.format(
            self.op1._build_exp(attr_values), self.operator, self.op2._build_exp(attr_values)
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

class LT(ComparisonOperator):
    operator = '<'


class Contains(Operator):
    def __init__(self, path, operand):
        self.path = path
        self.operand = operand

    def _build_exp(self, attr_values):
        key = attr_values.insert(self.operand)
        return 'contains({0}, {1})'.format(self.path, key)
