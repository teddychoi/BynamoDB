class NullAttributeException(Exception):
    """Raised when the attribute which is not nullable is missing."""
    pass


class ItemNotFoundException(Exception):
    """Raised when the item is not found"""
    pass


class ConditionNotRecognizedException(Exception):
    """Raised when the condition is not found"""
    pass
