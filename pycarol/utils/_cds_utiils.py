from google.resumable_media import DataCorruption
import functools


def retry_check_sum(func):
    """
    This is a decorator to retry when getting the
    `Checksum mismatch while downloading:` error.

    Args:
        func: pycarol.Storage.load

    Returns:

    """

    @functools.wraps(func)
    def retry(*args, **kwargs):

        while True:
            try:
                return func(*args, **kwargs)
            except DataCorruption as e:
                # TODO: Add logs in pycarol.
                continue

    return retry
