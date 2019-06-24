""" Tests for deprecated features inside pyCarol

"""
import warnings
import pytest


def test_luigi_extension_deprecated():
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("default", category=DeprecationWarning)
        import pycarol.luigi_extension
        assert "luigi_extension is deprecated" in str(w[0].message)
