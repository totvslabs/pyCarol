import asyncio
from concurrent.futures import ThreadPoolExecutor

from .miscellaneous import stream_data
import threading


class AtomicCounter:
    """An atomic, thread-safe incrementing counter.

    Args:
        initial: `int` default `0`
            Initial value for the counter.
        total: `int` default `None`
            If exists, the max value that will be reached.

    """

    def __init__(self, initial=0, total=None):
        """Initialize a new atomic counter to given initial value.
        """
        self.value = initial
        self.total = total
        self._lock = threading.Lock()

    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value

    def print(self):
        # Guarantee to print only one thread each time.
        with self._lock:
            print(f'{self.value}/{self.total} sent', end='\r')


def send_a(carol, session, url, data_json, extra_headers, content_type, counter):
    """
    Helper function to be used when sending data async.

    Args:
        carol: requests.Session
            Carol object
        session: `requests.Session`
            Session object to handle multiple API calls.
        url: `str`
            end point to be called.
        data_json: `dict`
            The json to be send.
        extra_headers: `dict`
            Extra headers to be used in the API call
        content_type: `dict`
            Content type of the call.
        :return: None
    """
    carol.call_api(url, data=data_json, extra_headers=extra_headers,
                   content_type=content_type, session=session)

    counter.increment(len(data_json))
    counter.print()


async def send_data_asynchronous(carol, data, step_size, url, extra_headers,
                                 content_type, max_workers, compress_gzip):
    """
    Helper function to send data asynchronous.

    Args:
        carol: `pycarol.carol.Carol`.
            Carol object
        data: `pandas.DataFrame` or `list of dict`,
            Data to be sent.
        step_size: 'int'
            Number of records per slice.
        url: 'str'
            API URI
        extra_headers: `dict`
            Extra headers to be used in the API call
        content_type:  `dict`
            Content type of the call.
        max_workers:  `int`
            Max number of workers of the async job
        compress_gzip: 'bool'
            If to compress the data to send
        :return:
    """

    counter = AtomicCounter(total=len(data))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        session = carol._retry_session(status_forcelist=[502, 429, 524, 408, 504, 598, 520, 503, 500],
                                       method_whitelist=frozenset(['POST']))
        # Set any session parameters here before calling `send_a`
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                send_a,
                *(carol, session, url, data_json, extra_headers, content_type, counter)
                # Allows us to pass in multiple arguments to `send_a`
            )
            for data_json, _ in stream_data(data=data,
                                            step_size=step_size,
                                            compress_gzip=compress_gzip)
        ]

        for _ in await asyncio.gather(*tasks):
            pass
