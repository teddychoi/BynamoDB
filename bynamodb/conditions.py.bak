from boto.dynamodb.types import Dynamizer

from .exceptions import ConditionNotRecognizedException


CONDITIONS = {
    'eq': 'EQ',
    'ne': 'NE',
    'lte': 'LE',
    'lt': 'LT',
    'gte': 'GE',
    'gt': 'GT',
    'null': 'NULL',
    'contains': 'CONTAINS',
    'ncontains': 'NOT_CONTAINS',
    'beginswith': 'BEGINS_WITH',
    'in': 'IN',
    'between': 'BETWEEN'
}


KEY_CONDITIONS = dict(
    (key, value) for key, value in CONDITIONS.items()
    if key in ['eq', 'lte', 'lt', 'gte', 'gt', 'beginswith', 'between']
)


def build_condition(filter_map, using=CONDITIONS):
    """ Implement the conditions described in
    docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html

    """
    if not filter_map:
        return

    filters = {}
    dynamizer = Dynamizer()

    for field_and_op, value in filter_map.items():
        field_bits = field_and_op.split('__')
        fieldname = '__'.join(field_bits[:-1])

        try:
            op = using[field_bits[-1]]
        except KeyError:
            raise ConditionNotRecognizedException(
                "Operator '%s' from '%s' is not recognized." % (
                    field_bits[-1],
                    field_and_op
                )
            )

        lookup = {
            'AttributeValueList': [],
            'ComparisonOperator': op,
        }

        # Special-case the ``NULL/NOT_NULL`` case.
        if field_bits[-1] == 'null':
            del lookup['AttributeValueList']

            if value is False:
                lookup['ComparisonOperator'] = 'NOT_NULL'
            else:
                lookup['ComparisonOperator'] = 'NULL'
        # Special-case the ``BETWEEN`` case.
        elif field_bits[-1] == 'between':
            if len(value) == 2 and isinstance(value, (list, tuple)):
                lookup['AttributeValueList'].append(
                    dynamizer.encode(value[0])
                )
                lookup['AttributeValueList'].append(
                    dynamizer.encode(value[1])
                )
        # Special-case the ``IN`` case
        elif field_bits[-1] == 'in':
            for val in value:
                lookup['AttributeValueList'].append(dynamizer.encode(val))
        else:
            # Fix up the value for encoding, because it was built to only work
            # with ``set``s.
            if isinstance(value, (list, tuple)):
                value = set(value)
            lookup['AttributeValueList'].append(
                dynamizer.encode(value)
            )

        # Finally, insert it into the filters.
        filters[fieldname] = lookup
    return filters
