class ExecutionError(Exception):
    """Raised when query execution fails.
    """

    pass


class CopyError(Exception):
    """Raised when COPY FROM fails.
    """

    pass
