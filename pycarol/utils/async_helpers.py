import json
import gzip, io


def send_a(carol, session, url, data_json, extra_headers, content_type):

    carol.call_api(url, data=data_json, extra_headers=extra_headers,
                   content_type=content_type, session=session)


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
