"""Back-end for BigQuery-related code."""
import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import typing as T
import warnings

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

try:
    import pandas
except ImportError:
    pass

from . import __TEMP_STORAGE__
from . import exceptions
from .carol import Carol
from .storage import Storage


class BQ:

    """Handles BigQuery queries.

    Args:
        carol: object from Carol class.
        service_account: Google Cloud SA with access to BQ.
        cache_cds: if SA should be cached for subsequent uses.
    """

    token: T.Optional[T.Dict[str, T.Any]] = None

    def __init__(
        self,
        carol: Carol,
        service_account: T.Optional[T.Dict[str, T.Any]] = None,
        cache_cds: bool = True,
    ):
        self.carol = carol
        self.service_account = service_account
        self._provided_sa = service_account is not None
        self.env = carol.get_current()
        self._temp_file_name = f".pycarol_temp_{self.env['env_id']}.json"
        self._temp_file_path = Path(__TEMP_STORAGE__) / self._temp_file_name
        self.client: T.Optional[bigquery.Client] = None
        self.dataset_id = f"carol-{self.env['env_id'][0:20]}.{self.env['env_id']}"
        self.cache_cds = cache_cds
        self.storage: T.Optional[Storage] = None
        self._fetch_cache()

    def _fetch_cache(self) -> None:
        if not self.is_expired():
            return

        if self._temp_file_path.exists():
            service_account = _load_local_cache(self._temp_file_path)
            service_account = _format_sa(service_account, self.env)
            if service_account["expiration_time"] < datetime.utcnow():
                return

            BQ.token = service_account
            self.service_account = service_account["sa"]
            return

        if self.cache_cds:
            if self.storage is None:
                self.storage = Storage(self.carol)

            try:
                # there is an issue with the storage.load() credentials sometimes
                # we cannot reproduce, this will ignore the cache check if it happens.
                cache_exists = self.storage.exists(
                    name=self._temp_file_name, storage_space="pycarol"
                )
            except Exception as e:
                warnings.warn(f"Failed to check cache {e}", UserWarning, stacklevel=3)
                return
            if cache_exists is True:
                sa_path = Path(
                    self.storage.load(
                        name=str(self._temp_file_name),
                        format="file",
                        storage_space="pycarol",
                        cache=False,
                    )
                )
                service_account = _load_local_cache(self._temp_file_path, sa_path)
                service_account = _format_sa(service_account, self.env)

                if service_account["expiration_time"] < datetime.utcnow():
                    return

                BQ.token = service_account
                self.service_account = service_account["sa"]
                _save_local_cache(self._temp_file_path, service_account["sa"])
        return

    def is_expired(self) -> bool:
        """Check if Service Accout has expired.

        Returns:
            bool
        """
        if self._provided_sa is True:
            return False
        if BQ.token is None:
            return True
        if BQ.token["expiration_time"] < datetime.utcnow():
            return True
        if BQ.token["env"]["env_id"] != self.env["env_id"]:
            return True
        return False

    def get_credential(
        self, expiration_time: int = 24, force: bool = False
    ) -> T.Dict[str, T.Any]:
        """Get service account for BigQuery and cache it.

        Args:
            expiration_time: Time in hours for credentials to expire. Max value 24.
            force: Force to get new credentials skiping any cache.

        Returns:
            Service account

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import BQ

            bq = BQ(Carol())
            service_account = bq.get_credential(expiration_time=120)
        """
        if force is True or self.is_expired():
            self.service_account = _get_tmp_key(expiration_time, self.carol)

            if "expiration_time" not in self.service_account:
                expiration_time_ = datetime.utcnow() + timedelta(hours=expiration_time)
                self.service_account["expiration_time"] = datetime.strftime(
                    expiration_time_,
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                )

            BQ.token = _format_sa(self.service_account, self.env)

            if self.storage is None:
                self.storage = Storage(self.carol)

            try:
                _save_local_cache(self._temp_file_path, self.service_account)
                if self.cache_cds:
                    _save_cds_cache(self.storage, self._temp_file_path)
            except Exception as e:
                warnings.warn(f"Failed to save cache {e}", UserWarning, stacklevel=3)

        if self.service_account is None and BQ.token is not None:
            self.service_account = BQ.token["sa"]

        if self.service_account is None:
            raise exceptions.NoServiceAccountException("No service account is set.")

        return self.service_account

    def query(
        self,
        query: str,
        dataset_id: T.Optional[str] = None,
        return_dataframe: bool = True,
    ):
        """Run query for datamodel. This will generate a SA if necessary.

        Args:
            query: BigQuery SQL query.
            dataset_id: BigQuery dataset ID.
                if None it will use the default dataset_id.
            return_dataframe: Return dataframe.
                Return dataframe if True.

        Returns:
            Query result.

        Usage:

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import BQ

            bq = BQ(Carol())
            query = 'select * from invoice limit 10'
            df = bq.query(query, return_dataframe=True)

        Raises:
            NoServiceAccountException if no service account was set
        """
        self.service_account = self.get_credential()
        if BQ.token is None:
            raise exceptions.NoServiceAccountException("No service account is set.")

        self.client = _generate_client(
            self.service_account, self._provided_sa, BQ.token["sa"]
        )

        dataset_id = dataset_id or self.dataset_id
        job_config = bigquery.QueryJobConfig(default_dataset=dataset_id)
        results_job = self.client.query(query, job_config=job_config)

        results = [dict(row) for row in results_job]

        if "pandas" not in sys.modules:
            return results

        if return_dataframe:
            return pandas.DataFrame(results)
        return results


def _generate_client(
    service_account: T.Dict, provided_sa: bool, token: T.Optional[T.Dict] = None
) -> bigquery.Client:
    if provided_sa is False:
        if token is None:
            raise exceptions.NoServiceAccountException("Token must be provided.")
        service_account = token

    credentials = Credentials.from_service_account_info(service_account)
    project = service_account["project_id"]
    client = bigquery.Client(project=project, credentials=credentials)
    return client


def _save_local_cache(tmp_filepath: Path, service_account: T.Dict) -> None:
    tmp_filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_filepath, "w", encoding="utf-8") as file:
        json.dump(service_account, file)


def _save_cds_cache(storage: Storage, tmp_filepath: Path) -> None:
    if not tmp_filepath.exists():
        return

    storage.save(
        name=tmp_filepath.name,
        obj=str(tmp_filepath),
        format="file",
        storage_space="pycarol",
    )


def _format_sa(sa: T.Dict, env: T.Dict) -> T.Dict:
    expiration_estimate = datetime.strptime(
        sa["expiration_time"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    sa = {
        "sa": sa,
        "expiration_time": expiration_estimate,
        "env": copy.deepcopy(env),
    }
    return sa


def _load_local_cache(
    tmp_filepath: Path, local_cache: T.Optional[Path] = None
) -> T.Dict:
    local_cache = local_cache or tmp_filepath
    with open(local_cache, "r", encoding="utf-8") as file:
        service_account = json.load(file)
    return service_account


def _get_tmp_key(expiration_time: int, carol: Carol) -> T.Dict[str, T.Any]:
    url = "v1/create_temporary_key"
    prefix_path = "/sql/v1/api/"
    env = carol.get_current()
    payload = {
        "expirationTime": expiration_time,
        "mdmOrgId": env["org_id"],
        "mdmTenantId": env["env_id"],
    }
    call_ret = carol.call_api(
        method="POST", path=url, prefix_path=prefix_path, data=payload
    )
    if call_ret is None:
        raise exceptions.NoServiceAccountException("No response from service account.")

    if not isinstance(call_ret, dict):
        raise exceptions.NoServiceAccountException("Response from call must be a dict.")

    return call_ret
