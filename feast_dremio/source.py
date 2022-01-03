import json
from typing import Callable, Dict, Optional

from feast import type_map
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.value_type import ValueType


class DremioSource(DataSource):
    def __init__(
            self,
            event_timestamp_column: Optional[str] = "",
            table_ref: Optional[str] = None,
            created_timestamp_column: Optional[str] = "",
            field_mapping: Optional[Dict[str, str]] = None,
            date_partition_column: Optional[str] = "",
            query: Optional[str] = None,
    ):
        """
        Create a DremioSource object.
        """

        self._table_ref = table_ref
        self._query = query
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    @property
    def table_ref(self):
        return self._table_ref

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"{self.table_ref}"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.bq_to_feast_value_type

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        """
        Creates a `DremioSource` object from a DataSource proto, by
        parsing the CustomSourceOptions which is encoded as a binary json string.
        """
        custom_source_options = str(
            data_source.custom_options.configuration, encoding="utf8"
        )

        table_ref = json.loads(custom_source_options)["table_ref"]
        return DremioSource(
            field_mapping=dict(data_source.field_mapping),
            table_ref=table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        """
        Creates a DataSource proto representation of this object, by serializing some
        custom options into the custom_options field as a binary encoded json string.
        """
        config_json = json.dumps({"table_ref": self.table_ref})
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto
