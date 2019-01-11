import re

class Online(object):
    """ Decorator to indicate the functions that will be exposed as endpoints.

    Usage:
        from pycarol.app.online import Online
        online = Online()

        @online.route("sample_endpoint")
        def sample1_function(request):
            return str(request)
    """
    def __init__(self):
        self.endpoints = {}

    def get_endpoints(self):
        return self.endpoints

    def _add_endpoint(self, name, func):
        self.endpoints[name] = func

    def _validate_route(self, name):
        if not re.match("^[a-zA-Z0-9_]*$", name):
            raise ValueError('Route name does not support special characters, only "_". Route: "{}"'.format(name))
        if len(name) > 30:
            raise ValueError('Route name should be smaller than 30 letters. Route: "{}" has {} letters'.format(name, str(len(name))))
        return True

    def route(self, name):
        def decorator(f):
            self._validate_route(name)
            self._add_endpoint(name, f)
            return f
        return decorator