import os
def assert_env_is_defined(needed_keys =[
    'CAROLTENANT',
    'CAROLAPPVERSION',
    'CAROLAPPNAME',
    'CAROLAPPOAUTH',
    'CAROLCONNECTORID',
    ]):
    """Raises KeyError if obligatory env variables were not found"""
    key_exists = {k: False for k in needed_keys}
    for k in needed_keys:
        key_exists[k] = k in os.environ.keys()
    if not all(key_exists.values()):
        raise KeyError(f"some env variables were not defined: {key_exists}")