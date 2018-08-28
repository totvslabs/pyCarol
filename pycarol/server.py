from flask import Flask, Response, current_app, json, request


class Server:
    def __init__(self, carol):
        self.carol = carol
        self.app = None

    def init(self):
        self.app = Flask(self.carol.app_name)
        self.app.response_class = JSONResponse
        return self

    def run(self):
        self.app.run()


class JSONResponse(Response):
    """Extend flask.Response with support for list/dict conversion to JSON."""
    def __init__(self, content=None, *args, **kargs):
        if isinstance(content, (list, dict)):
            kargs['mimetype'] = 'application/json'
            content = to_json(content)

        super(Response, self).__init__(content, *args, **kargs)

    @classmethod
    def force_type(cls, response, environ=None):
        """Override with support for list/dict."""
        if isinstance(response, (list, dict)):
            return cls(response)
        else:
            return super(Response, cls).force_type(response, environ)


def to_json(content):
    """Converts content to json while respecting config options."""
    indent = None
    separators = (',', ':')

    if (current_app.config['JSONIFY_PRETTYPRINT_REGULAR']
            and not request.is_xhr):
        indent = 2
        separators = (', ', ': ')

    return json.dumps(content, indent=indent, separators=separators), '\n'
