import contextlib
from datetime import datetime
from typing import Callable, ContextManager, Iterator, List, Optional, Union

import pandas as pd
import pyarrow
from feast.data_source import DataSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from pydantic import StrictBool, StrictInt, StrictStr
from pydantic.typing import Literal

import feast_dremio.config as cfg

from .client import Client
from .source import DremioSource


class DremioOfflineStoreConfig(FeastConfigBaseModel):
    """Custom offline store config for Dremio"""

    type: Literal[
        "feast_dremio.store.DremioOfflineStore"
    ] = "feast_dremio.store.DremioOfflineStore"

    host: StrictStr
    port: StrictInt
    user: StrictStr
    tls: bool
    certs: str
    server_verification: bool


class DremioRetrievalJob(RetrievalJob):
    def __init__(self,
                 query: Union[str, Callable[[], ContextManager[str]]],
                 config: RepoConfig,
                 client: Client,
                 full_feature_names: bool,
                 ):
        """Initialize a lazy historical retrieval job"""
        if not isinstance(query, str):
            self._query_generator = query
        else:
            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator

        self.config = config
        self.client = client
        self._full_feature_names = full_feature_names

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self):
        return None

    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        print("Getting a pandas DataFrame from a Dremio is easy!")
        with self._query_generator() as query:
            return self._execute_query(query).read_pandas()

    def _to_arrow_internal(self) -> pyarrow.Table:
        print("Getting an arrow Table from a Dremio is easy!")
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        with self._query_generator() as query:
            return self._execute_query(query).read_all()

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in Dremio to build the historical feature table.
        """
        with self._query_generator() as query:
            return query

    def to_dremio(self) -> Optional[str]:
        return ""

    def _execute_query(self, query, job_config=None):
        return self.client.execute_query(query)


class DremioOfflineStore(OfflineStore):
    def __init__(self):
        super().__init__()

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        print("Getting historical features from my offline store")
        assert isinstance(config.offline_store, DremioOfflineStoreConfig)

        client = _get_dremio_client(config.offline_store, cfg.DREMIO_PASS)

        query = """
            SELECT * FROM demo.feast.driver_stats
        """

        return DremioRetrievalJob(query=query, client=client, full_feature_names=None, config=config)

    def pull_latest_from_table_or_query(
        self,
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        print("Pulling latest features from my offline store")
        assert isinstance(data_source, DremioSource)
        assert isinstance(config.offline_store, DremioOfflineStoreConfig)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )

        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        client = _get_dremio_client(config.offline_store, cfg.DREMIO_PASS)

        sd = start_date.strftime('%Y-%m-%d %H:%M:%S%z')
        ed = end_date.strftime('%Y-%m-%d %H:%M:%S%z')

        query = f"""
            SELECT
               {field_string}
               {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TO_TIMESTAMP('{sd}', 'YYYY-MM-DD HH24:MI:SSTZO') AND TO_TIMESTAMP('{ed}', 'YYYY-MM-DD HH24:MI:SSTZO')
            )
            WHERE _feast_row = 1
        """

        return DremioRetrievalJob(query=query, client=client, full_feature_names=None, config=None)


def _get_dremio_client(cf, password: str):
    client = Client(cf.host, cf.port, cf.user, password, cf.tls, cf.certs, cf.server_verification)
    client.connect()

    return client
