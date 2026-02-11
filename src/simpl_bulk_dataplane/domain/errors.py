"""Domain exceptions for data flow operations."""


class DataFlowError(Exception):
    """Base class for data flow errors."""


class DataFlowNotFoundError(DataFlowError):
    """Raised when a data flow cannot be found."""


class DataFlowConflictError(DataFlowError):
    """Raised when an operation conflicts with current state."""


class DataFlowValidationError(DataFlowError):
    """Raised when request validation fails."""


class UnsupportedTransferTypeError(DataFlowValidationError):
    """Raised when a transfer type is unsupported by this data plane."""


__all__ = [
    "DataFlowConflictError",
    "DataFlowError",
    "DataFlowNotFoundError",
    "DataFlowValidationError",
    "UnsupportedTransferTypeError",
]
