"""
The main Carol's storage is called CDS (Carol Data Storage). Any data received or created in Carol is sent to CDS.
Inside CDS one can have three kinds of data. Data coming from the
Staging Area, data processed and mapped to a DataModel (Golden Record), and any other file that the user could send.
The `pycarol.cds.CDSStaging` and
the `pycarol.cds.CDSGolden` classes are used to manipulate the data inside the first two cases.

"""
from .connectors import Connectors
from .data_models import DataModel
from .utils.deprecation_msgs import _deprecation_msgs, deprecated
import warnings

_MACHINE_FLAVORS = [
    'n1-standard-1',
    'n1-standard-2',
    'n1-standard-4',
    'n1-standard-8',
    'n1-standard-16',
    'n1-highmem-2',
    'n1-highmem-4',
    'n1-highmem-8',
    'n1-highmem-16',
    'n2-standard-2',
    'n2-standard-4',
    'n2-standard-8',
    'n2-standard-16',
    'n2-highmem-2',
    'n2-highmem-4',
    'n2-highmem-8',
    'n2-highmem-16',
    'n2-highmem-32',
    'n2-highmem-64',
]


def check_worker_type(worker_type):
    if worker_type not in _MACHINE_FLAVORS and worker_type is not None:
        warnings.warn(
            f'worker_type {worker_type} is unknown. It might not work. Use one of the known machines {_MACHINE_FLAVORS}',
            Warning, stacklevel=3
        )

@deprecated('2.55.9', '2.56.0', 'CDS Data reading is deprecated - Use Big Query layer to read data from Carol.')
class CDSStaging:
    """
    Class to handle all CDS Staging iterations. All methods are deprecated.

    """

    def __init__(self, carol):
        self.carol = carol



@deprecated('2.55.9', '2.56.0', 'CDS Data reading is deprecated - Use Big Query layer to read data from Carol.')
class CDSGolden:
    """
    Class to handle all CDS Staging iterations. All methods are deprecated, only process_bigquery is not.

    Args:

        carol: 'pycarol.Carol`
            Carol() instance.

    """

    def __init__(self, carol):
        """
        Args:
            carol:
        """

        self.carol = carol


    def process_bigquery(
        self, query, dm_name=None, dm_id=None,
        save_cds_data=True, delete_target_folder=False,
        send_subscriptions=True, use_dataflow=False, delete_realtime_records=False,
        send_realtime_records=False, save_big_query=False, clear_big_query=False, deduplicate_results=False, **extra_params
    ):
        """
        Process CDS using bigquery engine.

        Args:
            query: `str`
                BigQuery query.
            dm_name: `str`,
                Data Model name.
            dm_id: `str`, default `None`
                Data Model id.
            save_cds_data: `bool`, default `True`
                Save result in CDS.
            delete_target_folder: `bool`, default `False`
                Delete target folder.
            send_subscriptions: `bool`, default `True`  
                Send subscriptions.
            use_dataflow: `bool`, default `False`
                Use Dataflow.
            delete_realtime_records: `bool`, default `False`
                Delete realtime records.
            send_realtime_records: `bool`, default `False`
                Send realtime records.
            save_big_query: `bool` default `False`
                save the result to BigQuery table
            clear_big_query: `bool` default `False`
                clean BigQuery first
            deduplicate_results: `bool` default `False`
                If results should be deduplicated (forced to true if send_realtime_records is True)
            extra_params: `dict`
                If a new parameter is added on carol, it is a way to make possible to add this new parameter without updating pycarol

        :return: `dict`
            Task created in Carol. 
        """

        if dm_id:
            dm_name = DataModel(self.carol).get_by_id(dm_id)['mdmName']
        else:
            if dm_name is None:
                raise ValueError('dm_name or dm_id should be set.')

        query_params = {
            'entityTemplateName': dm_name,
            'saveCds': save_cds_data,
            'clearCds': delete_target_folder,
            'sendSubscriptions': send_subscriptions,
            'clearRealtime': delete_realtime_records,
            'sendRealtime': send_realtime_records,
            'useDataflow': use_dataflow,
            'saveBigQuery': save_big_query,
            'clearBigQuery': clear_big_query,
            'deduplicateResults': deduplicate_results
        }
        query_params.update(extra_params)
        
        content_type = 'text/plain'

        return self.carol.call_api(path='v2/bigQuery/processQuery', method='POST', data=query, params=query_params, content_type=content_type)
