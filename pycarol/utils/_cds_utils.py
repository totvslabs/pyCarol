from google.resumable_media import DataCorruption
from google.api_core.exceptions import GatewayTimeout
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

        for i in range(5):
            try:
                return func(*args, **kwargs)
            except (GatewayTimeout, DataCorruption) as e:
                # TODO: Add logs in pycarol.
                continue

        raise Exception(f"Max retries exceeded with {func}")

    return retry
