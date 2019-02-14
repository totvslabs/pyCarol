import json
import gzip, io

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
def stream_data(data, data_size, step_size, is_df, compress_gzip):
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