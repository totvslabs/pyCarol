def send_a(carol, session, url, data_json, extra_headers, content_type):

    carol.call_api(url, data=data_json, extra_headers=extra_headers,
                   content_type=content_type, session=session)
