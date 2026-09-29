from unittest import mock

import pandas as pd
import pytest

from pycarol.staging import Staging


def _make_staging():
    carol = mock.MagicMock()
    carol._current_env.return_value = {'mdmId': 'env'}
    return Staging(carol)


def _summary_payload(carol):
    summary_calls = [c for c in carol.call_api.call_args_list
                     if '/summary' in c.args[0]]
    assert len(summary_calls) == 1
    return summary_calls[0].kwargs['data']


@pytest.mark.parametrize('async_send', [False, True])
@pytest.mark.parametrize('gzip', [True, False])
@pytest.mark.parametrize('as_df', [False, True])
@pytest.mark.parametrize('step_size', [3, 10, 50])
def test_send_data_batch_summary_counts_records(step_size, as_df, gzip, async_send):
    records = [{'id': i, 'value': f'v{i}'} for i in range(10)]
    data = pd.DataFrame(records) if as_df else records

    staging = _make_staging()
    staging.send_data('stg', data=data, connector_id='conn', step_size=step_size,
                      gzip=gzip, async_send=async_send, force=True, print_stats=False)

    payload = _summary_payload(staging.carol)
    assert payload['totalRecords'] == len(records)
    assert payload['totalRequests'] == -(-len(records) // step_size)
