import warnings
def _deprecation_msgs(msg):
    warnings.warn(
        msg,
        DeprecationWarning, stacklevel=3
    )
