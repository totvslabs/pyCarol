"""
The main Carol's storage is called CDS (Carol Data Storage). Any data received or created in Carol is sent to CDS.
Inside CDS one can have three kinds of data. Data coming from the
Staging Area, data processed and mapped to a DataModel (Golden Record), and any other file that the user could send.
The `pycarol.cds.CDSStaging` and
the `pycarol.cds.CDSGolden` classes are used to manipulate the data inside the first two cases.

"""
from .connectors import Connectors
from .data_models import DataModel
from .utils.deprecation_msgs import _deprecation_msgs
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

class CDSStaging:
    """
    Class to handle all CDS Staging iterations.

    """

    def __init__(self, carol):
        self.carol = carol

    def process_data(
            self, staging_name, connector_id=None, connector_name=None,
            worker_type=None, max_number_workers=-1, number_shards=-1, num_records=-1,
            delete_target_folder=False, enable_realtime=None, delete_realtime_records=False,
            send_realtime=None, file_pattern='*', filter_query=None, skip_consolidation=False,
            force_dataflow=False, recursive_processing=True, dm_name=None, auto_scaling=True,
    ):
        """
        Process CDS staging data.

        Args:

            staging_name: `str`,
                Staging name.
            connector_id: `str`, default `None`
                Connector id.
            connector_name: `str`, default `None`
                Connector name.
            worker_type: `str`, default None
                Machine flavor to be used. If `None` Carol will decide the machine to use.
            max_number_workers: `int`, default `-1`
                Max number of workers to be used during the process. '-1' means all the available.
            number_shards: `int`, default `-1`
                Number of shards.
            num_records: `int`, default `-1`
                Number of records to be processed. '-1' means all the records.
            delete_target_folder: `bool`, default `False`
                If delete the previous processed records.
            enable_realtime: `bool`, default `False`
                DEPRECATED. Removed from Carol.
                Enable this staging table to send the processed data to realtime layer.
            delete_realtime_records: `bool`, default `False`
                Delete previous processed data in realtime.
            send_realtime: `bool`, default `None`
                Send the processed data to realtime layer.
            file_pattern: `str`, default `*`
                File pattern of the files in CDS to be processed. The pattern in  `YYYY-MM-DDTHH_mm_ss*.parquet`.
                One can use this to filter data in CDS received in a given date.
            filter_query: `dict`, default `None`
                Query to be used to filter the data to be processed.
            skip_consolidation: `bool` default `False
                If consolidation process should be skipped
            force_dataflow: `bool`  default `False`
                If Dataflow job should be spinned even for small datasets
                (by default, small datasets are processed directly inside Carol)
            recursive_processing: `bool`  default `True`
                If processing should be chained/recursed in target entities. e.g., If a staging has 3 ETLs and each ETL
                maps to a data model. If we process this staging it will trigger the whole tree to be processed.
            dm_name: `str` default `None`
                If not None, it will reprocess the rejected records from the selected staging table.
            auto_scaling: `bool` default `True`
                Use auto scaling. It `False` Carol will use max_number_workers for the whole process.

        :return: dict
            Task definition.

        """

        check_worker_type(worker_type=worker_type)

        if enable_realtime is not None:
            _deprecation_msgs(
                "`enable_realtime` is deprecated and it is not used in Carol. ")

        filter_query = filter_query if filter_query else {}

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError(
                    'connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name, "workerType": worker_type,
            "maxNumberOfWorkers": max_number_workers, "numberOfShards": number_shards,
            "numRecords": num_records,
            "deleteTargetFolder": delete_target_folder,
            "deleteRealtimeRecords": delete_realtime_records,
            "sendToRealtime": send_realtime, "filePattern": file_pattern,
            "skipConsolidation": skip_consolidation,
            "forceDataflow": force_dataflow,
            "recursiveProcessing": recursive_processing,
            "rejectedType": dm_name, "autoScaling": auto_scaling,
        }

        return self.carol.call_api(path='v1/cds/staging/processData', method='POST', params=query_params,
                                   data=filter_query)

    def sync_data(
            self, staging_name, connector_id=None, connector_name=None, num_records=-1,
            delete_realtime_records=False, enable_realtime=None,
            file_pattern='*', filter_query=None, force_dataflow=False, records_percentage=100
    ):
        """
        Sync data to realtime layer.

        Args:

            staging_name: `str`,
                Staging name.
            connector_id: `str`, default `None`
                Connector id.
            connector_name: `str`, default `None`
                Connector name.
            num_records: `int`, default `-1`
                Number of records to be processed. '-1' means all the records.
            enable_realtime: `bool`, default `False`
                DEPRECATED. Removed from Carol.
                Enable this staging table to send the processed data to realtime layer.
            delete_realtime_records: `bool`, default `False`
                Delete previous processed data in realtime.
            file_pattern: `str`, default `*`
                File pattern of the files in CDS to be processed. The pattern in  `YYYY-MM-DDTHH_mm_ss*.parquet`.
                One can use this to filter data in CDS received in a given date.
            filter_query: `dict`, default `None`
                Query to be used to filter the data to be processed.
            force_dataflow: `bool`  default `False`
                If Dataflow job should be spinned even for small datasets
                (by default, small datasets are processed directly inside Carol)
            records_percentage" `int` default `100`
                The percentage of records (0-100) to import

        :return: None
        """

        if enable_realtime is not None:
            _deprecation_msgs(
                "`enable_realtime` is deprecated and it is not used in Carol. ")

        filter_query = filter_query if filter_query else {}

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError(
                    'connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name,
            "numRecords": num_records,
            "clearStagingRealtime": delete_realtime_records, "filePattern": file_pattern,
            "forceDataflow": force_dataflow, "recordsPercentage": records_percentage,
        }

        return self.carol.call_api(path='v1/cds/staging/fetchData', method='POST', params=query_params,
                                   data=filter_query)

    def consolidate(
            self, staging_name, connector_id=None, connector_name=None,
            worker_type=None, max_number_workers=-1, number_shards=-1, force_dataflow=False,
            rehash_ids=False, file_pattern="*.parquet", compute_transformations=False,
            auto_scaling=True, file_size_limit=-1,
    ):
        """
        Process staging CDS data.

        Args:

            staging_name: `str`,
                Staging name.
            connector_id: `str`, default `None`
                Connector id.
            connector_name: `str`, default `None`
                Connector name.
            worker_type: `str`, default `None`
                Machine flavor to be used. If `None` Carol will decide the machine to use.
            max_number_workers: `int`, default `-1`
                Max number of workers to be used during the process. '-1' means all the available.
            number_shards: `int`, default `-1`
                Number of shards.
            force_dataflow: `bool`  default `False`
                If Dataflow job should be spinned even for small datasets
                (by default, small datasets are processed directly inside Carol)
            rehash_ids" `bool` default `False`
                If all ids should be regenerated from the crosswalk
            file_pattern: `str`, default `*.parquet`
                File pattern of the files in CDS to be consolidated. The pattern is `YYYY-MM-DDTHH_mm_ss*.parquet`.
                One can use this to filter data in CDS received in a given date.
            compute_transformations: `bool` default False
                If staging transformations are defined, this will apply the transformations during the consolidate.
            auto_scaling: `bool` default `True`
                Use auto scaling. It `False` Carol will use max_number_workers for the whole process.
            file_size_limit: `int` default `-1`
                Ignore files larger than "file_size_limit" bytes during consolidation. 


        :return: `dict`
            Task created in Carol. 

        """

        check_worker_type(worker_type=worker_type)

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError(
                    'connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name,
            "workerType": worker_type, "maxNumberOfWorkers": max_number_workers,
            "numberOfShards": number_shards, "filePattern": file_pattern,
            "rehashIds": rehash_ids, "forceDataflow": force_dataflow,
            "computeTransformations": compute_transformations,
            "autoScaling": auto_scaling, "fileSizeLimitBytes": file_size_limit,
        }

        return self.carol.call_api(path='v1/cds/staging/consolidate', method='POST', params=query_params)

    def delete(self, staging_name, connector_id=None, connector_name=None):
        """
        Delete all CDS staging data.

        Args:

            staging_name: `str`,
                Staging name.
            connector_id: `str`, default `None`
                Connector id.
            connector_name: `str`, default `None`
                Connector name

        :return: None

        """

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError(
                    'connector_id or connector_name should be set.')

        query_params = {"connectorId": connector_id,
                        "stagingType": staging_name}

        return self.carol.call_api(path='v1/cds/staging/clearData', method='POST', params=query_params)

    def count(self, staging_name, connector_id=None, connector_name=None):
        """

        Count number of messages in CDS.

        Args:

            staging_name: `str`,
                Staging name.
            connector_id: `str`, default `None`
                Connector id.
            connector_name: `str`, default `None`
                Connector name

        :return: `int`
            Count

        """

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError(
                    'connector_id or connector_name should be set.')

        query_params = {"connectorId": connector_id,
                        "stagingType": staging_name}
        return self.carol.call_api(path='v1/cds/staging/fetchCount', method='POST', params=query_params).get('count')


class CDSGolden:
    """
    Class to handle all CDS Staging iterations.

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

    def sync_data(self, dm_name, dm_id=None, num_records=-1, file_pattern='*', filter_query=None,
                  skip_consolidation=False, force_dataflow=False, records_percentage=100, worker_type=None,
                  max_number_workers=-1, clear_golden_realtime=False,
                  ):
        """
        Sync data to realtime layer.

        Args:

            dm_name: `str`,
                Data model name.
            dm_id: `str`, default `None`
                Data model id.
            num_records: `int`, default `-1`
                Number of records to be processed. '-1' means all the records.
            file_pattern: `str`, default `*`
                File pattern of the files in CDS to be processed. The pattern in  `YYYY-MM-DDTHH_mm_ss*.parquet`.
                One can use this to filter data in CDS received in a given date.
            filter_query: `dict`, default `None`
                Query to be used to filter the data to be processed.
            skip_consolidation: `bool` default `False
                If consolidation process should be skipped
            force_dataflow: `bool`  default `False`
                If Dataflow job should be spinned even for small datasets
                (by default, small datasets are processed directly inside Carol)
            records_percentage" `int` default `100`
                The percentage of records (0-100) to import
            worker_type: `str`, default `None`
                Machine flavor to be used. If `None` Carol will decide the machine to use.
            max_number_workers: `int`, default `-1`
                Max number of workers to be used during the process. '-1' means all the available.
            clear_golden_realtime: `bool`, default `False`
                If the records on realtime should be deleted first

        :return: None

        """

        filter_query = filter_query if filter_query else {}

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        query_params = {
            "entityTemplateId": dm_id, "numRecords": num_records, "filePattern": file_pattern,
            "skipConsolidation": skip_consolidation, "forceDataflow": force_dataflow,
            "clearGoldenRealtime": clear_golden_realtime, "maxNumberOfWorkers": max_number_workers,
            "workerType": worker_type, "recordsPercentage": records_percentage,

        }

        return self.carol.call_api(path='v1/cds/golden/fetchData', method='POST', params=query_params,
                                   data=filter_query)

    def delete_rejected(self, dm_name=None, dm_id=None, connector_name=None, connector_id=None, staging_name=None):
        """
        Delete CDS DataModel rejected data

        Args:
            dm_name: `str`,
                Data Model name.
            dm_id: `str`, default `None`
                Data Model id.
            connector_name: `str`, default `None`
                Connector name. Used if delete only records from a given connector/staging table
            connector_id:  `str`, default `None`
                Connector id. Used if delete only records from a given connector/staging table
            staging_name:  `str`, default `None`
                Staging name. Used if delete only records from a given connector/staging table

        Returns: dict
            Carol task created.


        """

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(
                connector_name)['mdmId']

        if (staging_name is not None) and (connector_id is None):
            raise ValueError(
                'connector_id or connector_name must be set when using staging_name.')

        params = {'entityTemplateId': dm_id,
                  'connectorId': connector_id,
                  'stagingTable': staging_name}

        return self.carol.call_api("v2/cds/rejected/clearData", method='POST',
                                   params=params)

    def delete(self, dm_name=None, dm_id=None, ):
        """
        Delete all CDS data model data.

        Args:

            dm_name: `str`,
                Data Model name.
            dm_id: `str`, default `None`
                Data Model id.

        :return: None
        """

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        query_params = {"entityTemplateId": dm_id}

        return self.carol.call_api(path='v1/cds/golden/clearData', method='POST', params=query_params)

    def count(self, dm_name=None, dm_id=None):
        """
        Count number of messages in CDS.

        Args:

            dm_name: `str`,
                Data Model name.
            dm_id: `str`, default `None`
                Data Model id.

        :return: `int`
            Count
        """

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        query_params = {"entityTemplateId": dm_id}
        return self.carol.call_api(path='v1/cds/golden/fetchCount', method='POST', params=query_params).get('count')

    def consolidate(
        self, dm_name=None, dm_id=None,
        worker_type=None, max_number_workers=-1, number_shards=-1, force_dataflow=False,
        ignore_merge=False, file_pattern="*.parquet",  auto_scaling=True,
        file_size_limit=-1,
    ):
        """

        Process staging CDS data.

        Args:

            dm_name: `str`,
                Data Model name.
            dm_id: `str`, default `None`
                Data Model id.
            worker_type: `str`, default `None`
                Machine flavor to be used. If `None` Carol will decide the machine to use.
            max_number_workers: `int`, default `-1`
                Max number of workers to be used during the process. '-1' means all the available.
            number_shards: `int`, default `-1`
                Number of shards.
            ignore_merge: `bool` default `False
                If merge rules should be ignored when consolidating the records
            force_dataflow: `bool`  default `False`
                If Dataflow job should be spinned even for small datasets
                (by default, small datasets are processed directly inside Carol)
            file_pattern: `str`, default `*.parquet`
                File pattern of the files in CDS to be consolidated. The pattern is `YYYY-MM-DDTHH_mm_ss*.parquet`.
                One can use this to filter data in CDS received in a given date.
            auto_scaling: `bool` default `True`
                Use auto scaling. It `False` Carol will use max_number_workers for the whole process.
            file_size_limit: `int` default `-1`
                Ignore files larger than "file_size_limit" bytes during consolidation. 

        :return: `dict`
            Task created in Carol. 
        """

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        check_worker_type(worker_type=worker_type)

        query_params = {
            "entityTemplateId": dm_id,
            "workerType": worker_type, "maxNumberOfWorkers": max_number_workers,
            "numberOfShards": number_shards,
            "forceDataflow": force_dataflow, "ignoreMerge": ignore_merge, "filePattern": file_pattern,
            "autoScaling": auto_scaling, "fileSizeLimitBytes": file_size_limit,
        }

        return self.carol.call_api(path='v1/cds/golden/consolidate', method='POST', params=query_params)
