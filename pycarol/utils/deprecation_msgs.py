import functools
import typing as T
import warnings

import deprecation

from .. import __version__


def _deprecation_msgs(msg):
    warnings.warn(msg, DeprecationWarning, stacklevel=3)


def deprecation_msg(deprecated_in: str, removed_in: str, details: str) -> str:
    """Create a message for deprecation.

    Args:
        deprecated_in: version of deprecation.
        removed_in: version of removal.
        details: deprecation message.

    Returns:
        deprecation message.
    """
    if deprecated_in >= __version__:
        msg = f"This function will be deprecated at version {deprecated_in}. "
    else:
        msg = f"This function was deprecated at version {deprecated_in}. "

    msg += f"It will be removed at {removed_in}. "
    msg += details
    return msg


def deprecated(deprecated_in, removed_in, details) -> T.Callable:
    """Decorate deprecated functions.

    Args:
        deprecated_in: version of deprecation.
        removed_in: version of removal.
        details: deprecation message.
    """
    def _outer_func(original_func) -> T.Callable:
        func_ = deprecation.deprecated(deprecated_in, removed_in, __version__, details)
        deprecated_func = func_(original_func)

        @functools.wraps(deprecated_func)
        def _inner_func(*args, **kwargs) -> T.Any:
            msg = deprecation_msg(deprecated_in, removed_in, details)
            warnings.warn(msg)
            return deprecated_func(*args, **kwargs)

        return _inner_func

    return _outer_func
