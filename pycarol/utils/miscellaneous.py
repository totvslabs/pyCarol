import json
import gzip, io
import pandas as pd

def ranges(min_v, max_v, nb):
    if min_v == max_v:
        max_v += 1
    step = int((max_v - min_v) / nb) + 1
    step = list(range(min_v, max_v, step))
    if step[-1] != max_v:
        step.append(max_v)
    step = [[step[i], step[i + 1] - 1] for i in range(len(step) - 1)]
    step.append([max_v, None])
    return step

# TODO: reused from staging. Should I put in utils/?
def stream_data(data, step_size, compress_gzip):
    """

    :param data:  `pandas.DataFrame` or `list of dict`,
        Data to be sliced.
    :param step_size: 'int'
        Number of records per slice.
    :param compress_gzip: 'bool'
        If to compress the data to send
    :return: Generator, cont
        Return a slice of `data` and the count of records until that moment.
    """

    if isinstance(data, pd.DataFrame):
        is_df = True
    else:
        is_df = False
        assert isinstance(data, list)

    data_size = len(data)
    cont = 0
    for i in range(0, data_size, step_size):
        if is_df:
            data_to_send = data.iloc[i:i + step_size]
            cont += len(data_to_send)
            print('Sending {}/{}'.format(cont, data_size), end='\r')
            data_to_send = data_to_send.to_json(orient='records', date_format='iso', lines=False)
            if compress_gzip:
                out = io.BytesIO()
                with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                    f.write(data_to_send.encode('utf-8'))
                yield out.getvalue(), cont
            else:
                yield json.loads(data_to_send), cont
        else:
            data_to_send = data[i:i + step_size]
            cont += len(data_to_send)
            print('Sending {}/{}'.format(cont, data_size), end='\r')
            if compress_gzip:
                out = io.BytesIO()
                with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                    f.write(json.dumps(data_to_send).encode('utf-8'))
                yield out.getvalue(), cont
            else:
                yield data_to_send, cont
    return None, None