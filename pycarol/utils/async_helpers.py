import asyncio
from concurrent.futures import ThreadPoolExecutor

from .miscellaneous import stream_data


def send_a(carol, session, url, data_json, extra_headers, content_type):
    """
    Helper funcion to be used when sendind data async.


    :param carol:
    :param session:
    :param url:
    :param data_json:
    :param extra_headers:
    :param content_type:
    :return:
    """
    carol.call_api(url, data=data_json, extra_headers=extra_headers,
                   content_type=content_type, session=session)


async def send_data_asynchronous(carol, data, data_size, step_size, is_df, url, extra_headers,
                                 content_type, max_workers, compress_gzip):
    # based on https://hackernoon.com/how-to-run-asynchronous-web-requests-in-parallel-with-python-3-5-without-aiohttp-264dc0f8546
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        session = carol._retry_session()
        # Set any session parameters here before calling `send_a`
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                send_a,
                *(carol, session, url, data_json, extra_headers, content_type)
                # Allows us to pass in multiple arguments to `send_a`
            )
            for data_json, _ in stream_data(data=data, data_size=data_size,
                                            step_size=step_size, is_df=is_df,
                                            compress_gzip=compress_gzip)
        ]
        for _ in await asyncio.gather(*tasks):
            pass
