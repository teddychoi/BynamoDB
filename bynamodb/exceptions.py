class NullAttributeException(Exception):
    """Raised when the attribute which is not nullable is missing."""
    pass


class ItemNotFoundException(Exception):
    """Raised when the item is not found"""
    pass