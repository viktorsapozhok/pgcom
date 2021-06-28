import sys


class QueryExecutionError(Exception):
    """Raised when query execution fails."""

    pass


class CopyError(Exception):
    """Raised when COPY FROM fails."""

    pass


def raise_with_traceback(exc: Exception) -> None:
    """Raise exception with existing traceback."""

    _, _, traceback = sys.exc_info()

    raise exc.with_traceback(traceback)
