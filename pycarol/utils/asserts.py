import os
from contextlib import contextmanager


@contextmanager
def dotenv_context():
    """``with`` context to temporarily modify the environment variables"""
    import os, dotenv
    _environ = os.environ.copy()
    try:
        dotenv.load_dotenv()
        yield
    finally:
        os.environ.clear()
        os.environ.update(_environ)


def assert_env_is_defined(needed_keys=frozenset(['CAROLTENANT', 'CAROLAPPVERSION', 'CAROLAPPNAME',
                                                 'CAROLAPPOAUTH', 'CAROLCONNECTORID'])
                          ):
    """Raises KeyError if obligatory env variables were not found"""
    key_exists = {k: False for k in needed_keys}
    for k in needed_keys:
        key_exists[k] = k in os.environ.keys()
    if not all(key_exists.values()):
        raise KeyError(f"some env variables were not defined: {key_exists}")
