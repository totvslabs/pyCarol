from pathlib import Path
from unittest import mock

import os
import pycarol



def test_empty_query_scrollable_query_handler_empty() -> None:
    query_mock = mock.MagicMock()
    pycarol.Query._scrollable_query_handler(query_mock)



if __name__ == "__main__":
    test_empty_query_scrollable_query_handler_empty()
