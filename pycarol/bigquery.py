"""Back-end for BigQuery-related code."""
import copy
from datetime import datetime
import json
from pathlib import Path
import sys
import typing as T

from google.cloud import bigquery, bigquery_storage, bigquery_storage_v1
from google.cloud.bigquery_storage import types
from google.oauth2.service_account import Credentials

try:
    import pandas
except ImportError:
    pass

from . import __TEMP_STORAGE__
from . import exceptions
from .carol import Carol
from .storage import Storage


class Token:

    """Stores Token/Service Account information.

    Args:
        service_account: service account.
        env: dict from Carol.get_current().

    Attributes:
        expiration_time: datetime of service account expiration.
        env: dict from Carol.get_current().
        service_account: provided by Carol.
    """

    def __init__(
        self,
        service_account: T.Dict[str, T.Any],
        env: T.Dict[str, str],
    ):
        self.expiration_time = service_account["expiration_time"]
        self._env = env
        self.service_account = service_account

    def to_dict(self) -> T.Dict[str, T.Any]:
        """Convert Token to dictionary.

        Returns: dict Token.
        """
        return {
            "service_account": self.service_account,
            "env": copy.deepcopy(self._env),
            "expiration_time": self.expiration_time,
        }

    def expired(self) -> bool:
        """Check if token has expired.

        Return True if has expired.
        """
        dt_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        expiration_time_ = datetime.strptime(self.expiration_time, dt_format)
        return expiration_time_ < datetime.utcnow()


class TokenManager:

    """Manages Google service accounts to allow BigQuery operations.

    Args:
        carol: Carol object.
        provided_sa: if not provided, it will automatically generate.
        cache_cds: either cache service account to google cloud or not so that other
                   uses may share it. This may be helpful to reduce the number of
                   generated service accounts.
        expiration_window: Number of hours for credentials to expire. Max value 24.

    Attributes:
        expiration_window: Number of hours for credentials to expire. Max value 24.
        cache_cds: either cache service account to google cloud or not so that other
                   uses may share it. This may be helpful to reduce the number of
                   generated service accounts.
        token: Token object containing service account and metadata.
    """

    def __init__(
        self,
        carol: Carol,
        provided_sa: T.Optional[T.Dict[str, T.Any]] = None,
        cache_cds: bool = True,
        expiration_window: int = 24,
    ):
        self._carol = carol
        self.expiration_window = expiration_window
        self._env = carol.get_current()
        temp_file_name = f".pycarol_temp_{self._env['env_id']}.json"
        self._tmp_filepath = Path(__TEMP_STORAGE__) / temp_file_name
        self.cache_cds = cache_cds
        self._storage = Storage(self._carol)

        self.token: T.Optional[Token] = None
        if provided_sa is not None:
            self.token = Token(provided_sa, self._env)
        else:
            self.token = self.get_token()

    def _issue_new_key(self) -> T.Dict[str, T.Any]:
        url = "v1/create_temporary_key"
        prefix_path = "/sql/v1/api/"
        env = self._carol.get_current()
        payload = {
            "expirationTime": self.expiration_window,
            "mdmOrgId": env["org_id"],
            "mdmTenantId": env["env_id"],
        }
        call_ret = self._carol.call_api(
            method="POST", path=url, prefix_path=prefix_path, data=payload
        )
        if call_ret is None:
            msg = "No response from service account."
            raise exceptions.NoServiceAccountException(msg)

        if not isinstance(call_ret, dict):
            msg = "Response from call must be a dict."
            raise exceptions.NoServiceAccountException(msg)

        return call_ret

    def _save_token_file(self, token: Token) -> None:
        self._tmp_filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self._tmp_filepath, "w", encoding="utf-8") as file:
            json.dump(token.to_dict(), file)

    def _save_token_cloud(self) -> None:
        if not self._tmp_filepath.exists():
            return

        self._storage.save(
            name=self._tmp_filepath.name,
            obj=str(self._tmp_filepath),
            format="file",
            storage_space="pycarol",
        )

    def _load_token_file(self) -> T.Optional[Token]:
        if not self._tmp_filepath.exists():
            return None
        with open(self._tmp_filepath, "r", encoding="utf-8") as file:
            token_dict = json.load(file)

        if "service_account" not in token_dict or "env" not in token_dict:
            return None

        token = Token(token_dict["service_account"], token_dict["env"])
        return token

    def _load_token_cloud(self) -> T.Optional[Token]:
        filename = self._tmp_filepath.name
        cache_exists = self._storage.exists(name=filename, storage_space="pycarol")
        if cache_exists is True:
            sa_path = self._storage.load(
                name=filename,
                format="file",
                storage_space="pycarol",
                cache=False,
            )
            Path(sa_path).rename(self._tmp_filepath)
            return self._load_token_file()
        return None

    def get_forced_token(self) -> Token:
        """Get new service account for BigQuery and cache it.

        Returns:
            Service account

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import TokenManager

            tm = TokenManager(Carol())
            service_account = tm.get_forced_token().service_account
        """
        service_account = self._issue_new_key()
        self.token = Token(service_account, self._env)
        self._save_token_file(self.token)
        if self.cache_cds is True:
            self._save_token_cloud()
        return self.token

    def get_token(self) -> Token:
        """Get service account for BigQuery and cache it.

        Returns:
            Service account

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import TokenManager

            tm = TokenManager(Carol())
            service_account = tm.get_token().service_account
        """
        if self.token is not None and not self.token.expired():
            return self.token

        local_token = self._load_token_file()
        if local_token is not None and not local_token.expired():
            self.token = local_token
            return self.token

        cloud_token = self._load_token_cloud()
        if cloud_token is not None and not cloud_token.expired():
            self.token = cloud_token
            return self.token

        self.token = self.get_forced_token()
        return self.token


class BQ:

    """Handles BigQuery queries.

    Args:
        carol: object from Carol class.
        service_account: Google Cloud SA with access to BQ.
        cache_cds: if SA should be cached for subsequent uses.
    """

    def __init__(
        self,
        carol: Carol,
        service_account: T.Optional[T.Dict[str, T.Any]] = None,
        cache_cds: bool = True,
    ):
        self._env = carol.get_current()
        self._project_id = f"carol-{self._env['env_id'][0:20]}"
        self._dataset_id = f"{self._project_id}.{self._env['env_id']}"
        self._token_manager = TokenManager(carol, service_account, cache_cds)

    @staticmethod
    def _generate_client(service_account: T.Dict) -> bigquery.Client:
        credentials = Credentials.from_service_account_info(service_account)
        project = service_account["project_id"]
        client = bigquery.Client(project=project, credentials=credentials)
        return client

    def query(
        self,
        query: str,
        dataset_id: T.Optional[str] = None,
        return_dataframe: bool = True,
    ) -> T.Union["pandas.DataFrame", T.List[T.Dict[str, T.Any]]]:
        """Run query. This will generate a SA if necessary.

        Args:
            query: BigQuery SQL query.
            dataset_id: BigQuery dataset ID, if not provided, it will use the default
                        one.
            return_dataframe: Return dataframe if True.

        Returns:
            Query result.

        Usage:

        .. code:: python


            from pycarol import BQ, Carol

            bq = BQ(Carol())
            query = 'select * from invoice limit 10'
            df = bq.query(query, return_dataframe=True)
        """
        service_account = self._token_manager.get_token().service_account
        client = self._generate_client(service_account)

        dataset_id = dataset_id or self._dataset_id
        job_config = bigquery.QueryJobConfig(default_dataset=dataset_id)
        results_job = client.query(query, job_config=job_config)

        results = [dict(row) for row in results_job]

        if return_dataframe is False:
            return results

        if "pandas" not in sys.modules and return_dataframe is True:
            raise exceptions.PandasNotFoundException

        return pandas.DataFrame(results)


class BQStorage:

    """Handles BigQuery Storage API queries.

    Args:
        carol: object from Carol class.
        service_account: Google Cloud SA with access to BQ.
        cache_cds: if SA should be cached for subsequent uses.
    """

    def __init__(
        self,
        carol: Carol,
        service_account: T.Optional[T.Dict[str, T.Any]] = None,
        cache_cds: bool = True,
    ):
        self._env = carol.get_current()
        self._project_id = f"carol-{self._env['env_id'][0:20]}"
        self._dataset_id = f"{self._env['env_id']}"
        self._token_manager = TokenManager(carol, service_account, cache_cds)

    @staticmethod
    def _generate_client(service_account) -> bigquery_storage.BigQueryReadClient:
        credentials = Credentials.from_service_account_info(service_account)
        return bigquery_storage.BigQueryReadClient(credentials=credentials)

    def _get_read_session(
        self,
        client: bigquery_storage.BigQueryReadClient,
        table_name: str,
        columns_names: T.Optional[T.List[str]] = None,
    ) -> bigquery_storage_v1.types.ReadSession:
        read_options = None
        if columns_names is not None:
            read_options = types.ReadSession.TableReadOptions(  # type:ignore # noqa:E501 pylint:disable=no-member
                selected_fields=columns_names
            )

        table_path = f"projects/{self._project_id}/datasets/{self._dataset_id}/tables/{table_name}"  # noqa:E501
        requested_session = types.ReadSession(  # type:ignore # noqa:E501 pylint:disable=no-member
            table=table_path,
            data_format=types.DataFormat.ARROW,  # type:ignore # noqa:E501 pylint:disable=no-member
            read_options=read_options,
        )
        parent = f"projects/{self._project_id}"
        read_session = client.create_read_session(
            parent=parent,
            read_session=requested_session,
            max_stream_count=4,
        )
        return read_session

    def query(
        self,
        table_name: str,
        columns_names: T.Optional[T.List[str]] = None,
        return_dataframe: bool = True,
    ) -> T.Union["pandas.DataFrame", T.List[bigquery_storage_v1.reader.ReadRowsPage]]:
        """Read from BigQuery Storage API.

        Args:
            table_name: name of the table (views are not supported).
            columns_names: names of columns to return.
            return_dataframe: if True, return a pandas DataFrame.

        Returns:
            Query result.

        Usage:

        .. code:: python


            from pycarol import BQStorage, Carol

            bq = BQStorage(Carol())
            table_name = "ingestion_stg_model_deep_audit"
            col_names = ["request_id", "version"]
            df = bq.query(table_name, col_names, return_dataframe=True)
        """
        service_account = self._token_manager.get_token().service_account
        client = self._generate_client(service_account)
        read_session = self._get_read_session(client, table_name, columns_names)

        stream = read_session.streams[0]
        reader = client.read_rows(stream.name)

        frames = []
        for frame in reader.rows().pages:
            frames.append(frame)

        if return_dataframe is False:
            return frames

        if "pandas" not in sys.modules and return_dataframe is True:
            raise exceptions.PandasNotFoundException

        dataframe = pandas.concat([frame.to_dataframe() for frame in frames])
        dataframe = dataframe.reset_index(drop=True)
        return dataframe
