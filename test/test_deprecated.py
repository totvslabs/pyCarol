""" Tests for deprecated features inside pyCarol

"""
from unittest import mock
import warnings

import deprecation

import pycarol


def test_luigi_extension_deprecated():
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("default", category=DeprecationWarning)
        import pycarol.luigi_extension
        assert "luigi_extension is deprecated" in str(w[0].message)
