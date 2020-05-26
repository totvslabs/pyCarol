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
]


class CDSStaging:
    """
    Class to handle all CDS Staging iterations.

    """

    def __init__(self, carol):
        self.carol = carol

    def process_data(self, staging_name, connector_id=None, connector_name=None,
                     worker_type=None, max_number_workers=-1, number_shards=-1, num_records=-1,
                     delete_target_folder=False, enable_realtime=False, delete_realtime_records=False,
                     send_realtime=False, file_pattern='*', filter_query=None, skip_consolidation=False,
                     force_dataflow=False):

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
                Enable this staging table to send the processed data to realtime layer.
            delete_realtime_records: `bool`, default `False`
                Delete previous processed data in realtime.
            send_realtime: `bool`, default `False`
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

        :return: None

        """

        if worker_type not in _MACHINE_FLAVORS and worker_type is not None:
            raise ValueError(f'worker_type should be: {_MACHINE_FLAVORS}\n, you used {worker_type}')

        filter_query = filter_query if filter_query else {}

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError('connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name, "workerType": worker_type,
            "maxNumberOfWorkers": max_number_workers, "numberOfShards": number_shards,
            "numRecords": num_records,
            "deleteTargetFolder": delete_target_folder, "enableStagingRealtime": enable_realtime,
            "deleteRealtimeRecords": delete_realtime_records,
            "sendToRealtime": send_realtime, "filePattern": file_pattern,
            "skipConsolidation": skip_consolidation,
            "forceDataflow": force_dataflow,
        }

        return self.carol.call_api(path='v1/cds/staging/processData', method='POST', params=query_params,
                                   data=filter_query)

    def sync_data(self, staging_name, connector_id=None, connector_name=None, num_records=-1,
                  delete_realtime_records=False, enable_realtime=None,
                  file_pattern='*', filter_query=None, force_dataflow=False, records_percentage=100):

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
            _deprecation_msgs("`enable_realtime` is deprecated and it is not used in Carol. ")

        filter_query = filter_query if filter_query else {}

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError('connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name,
            "numRecords": num_records,
            "clearStagingRealtime": delete_realtime_records, "filePattern": file_pattern,
            "forceDataflow": force_dataflow, "recordsPercentage": records_percentage,
        }

        return self.carol.call_api(path='v1/cds/staging/fetchData', method='POST', params=query_params,
                                   data=filter_query)

    def consolidate(self, staging_name, connector_id=None, connector_name=None,
                    worker_type=None, max_number_workers=-1, number_shards=-1, force_dataflow=False,
                    rehash_ids=False
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

        :return: None

        """

        if worker_type not in _MACHINE_FLAVORS and worker_type is not None:
            raise ValueError(f'worker_type should be: {_MACHINE_FLAVORS}\n, you used {worker_type}')

        if connector_name:
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError('connector_id or connector_name should be set.')

        query_params = {
            "connectorId": connector_id, "stagingType": staging_name,
            "workerType": worker_type, "maxNumberOfWorkers": max_number_workers,
            "numberOfShards": number_shards,
            "rehashIds": rehash_ids, "forceDataflow": force_dataflow,
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
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError('connector_id or connector_name should be set.')

        query_params = {"connectorId": connector_id, "stagingType": staging_name}

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
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']
        else:
            if connector_id is None:
                raise ValueError('connector_id or connector_name should be set.')

        query_params = {"connectorId": connector_id, "stagingType": staging_name}
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

    def delete(self, dm_name=None, dm_id=None):

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

    def consolidate(self, dm_name=None, dm_id=None,
                    worker_type=None, max_number_workers=-1, number_shards=-1, force_dataflow=False,
                    ignore_merge=False
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

        :return: None
        """

        if dm_name:
            dm_id = DataModel(self.carol).get_by_name(dm_name)['mdmId']
        else:
            if dm_id is None:
                raise ValueError('dm_name or dm_id should be set.')

        if worker_type not in _MACHINE_FLAVORS and worker_type is not None:
            raise ValueError(f'worker_type should be: {_MACHINE_FLAVORS}\n, you used {worker_type}')

        query_params = {
            "entityTemplateId": dm_id,
            "workerType": worker_type, "maxNumberOfWorkers": max_number_workers,
            "numberOfShards": number_shards,
            "forceDataflow": force_dataflow, "ignoreMerge": ignore_merge
        }

        return self.carol.call_api(path='v1/cds/golden/consolidate', method='POST', params=query_params)
