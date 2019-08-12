import asyncio
from concurrent.futures import ThreadPoolExecutor

from .miscellaneous import stream_data


def send_a(carol, session, url, data_json, extra_headers, content_type):
    """
    Helper function to be used when sending data async.


    :param carol: requests.Session
        Carol object
    :param session: `requests.Session`
        Session object to handle multiple API calls.
    :param url: `str`
        end point to be called.
    :param data_json: `dict`
        The json to be send.
    :param extra_headers: `dict`
        Extra headers to be used in the API call
    :param content_type: `dict`
        Content type of the call.
    :return: None
    """
    carol.call_api(url, data=data_json, extra_headers=extra_headers,
                   content_type=content_type, session=session)


async def send_data_asynchronous(carol, data, step_size, url, extra_headers,
                                 content_type, max_workers, compress_gzip):
    """

    :param carol: `pycarol.carol.Carol`.
        Carol object
    :param data: `pandas.DataFrame` or `list of dict`,
        Data to be sent.
    :param step_size: 'int'
        Number of records per slice.
    :param url: 'str'
        API URI
    :param extra_headers: `dict`
        Extra headers to be used in the API call
    :param content_type:  `dict`
        Content type of the call.
    :param max_workers:  `int`
        Max number of workers of the async job
    :param compress_gzip: 'bool'
        If to compress the data to send
    :return:
    """
    # based on https://hackernoon.com/how-to-run-asynchronous-web-requests-in-parallel-with-python-3-5-without-aiohttp-264dc0f8546
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        session = carol._retry_session(status_forcelist=[502, 429, 524, 408, 504, 598, 520, 503, 500],
                                       method_whitelist=frozenset(['POST']))
        # Set any session parameters here before calling `send_a`
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                send_a,
                *(carol, session, url, data_json, extra_headers, content_type)
                # Allows us to pass in multiple arguments to `send_a`
            )
            for data_json, _ in stream_data(data=data,
                                            step_size=step_size,
                                            compress_gzip=compress_gzip)
        ]
        for _ in await asyncio.gather(*tasks):
            pass
