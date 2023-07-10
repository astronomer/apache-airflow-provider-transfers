from __future__ import annotations

import logging
import random
import string
from typing import ClassVar, Iterator

import pandas as pd
from universal_transfer_operator import settings
from universal_transfer_operator.constants import FileType
from universal_transfer_operator.data_providers.base import DataStream
from universal_transfer_operator.data_providers.dataframe.base import DataframeProvider
from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File

logger = logging.getLogger(__name__)


def convert_dataframe_to_file(df: pd.DataFrame) -> File:
    """
    Passes a dataframe into a File using parquet as an efficient storage format. This allows us to use
    Json as a storage method without filling the metadata database. the values for conn_id and bucket path can
    be found in the airflow.cfg as follows:

    [universal_transfer_operator]
    dataframe_storage_conn_id=...
    dataframe_storage_url=///
    :param df: Dataframe to convert to file
    :return: File object with reference to stored dataframe file
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(64)
    )

    if settings.DATAFRAME_STORAGE_CONN_ID is None:
        raise ValueError(
            "Missing conn_id. Please specify it in airflow's config"
            " `universal_transfer_operator.xcom_storage_conn_id`"
        )

    file = File(
        path=settings.DATAFRAME_STORAGE_URL + "/" + unique_id + ".parquet",
        conn_id=settings.DATAFRAME_STORAGE_CONN_ID,
        filetype=FileType.PARQUET,
        is_dataframe=True,
    )
    file.create_from_dataframe(df)
    return file


class PandasdataframeDataProvider(DataframeProvider):
    version: ClassVar[int] = 1

    def read(self) -> Iterator[pd.DataFrame]:
        """Read from dataframe dataset and write to local reference locations or dataframes"""
        yield self.dataset.dataframe

    def write(self, source_ref: pd.DataFrame | DataStream):
        """Write the data to the dataframe dataset or filesystem dataset"""
        if isinstance(source_ref, pd.DataFrame):
            return PandasdataframeDataProvider(dataset=Dataframe(dataframe=source_ref))
        else:
            return PandasdataframeDataProvider(
                dataset=Dataframe(
                    dataframe=source_ref.actual_file.type.export_to_dataframe(
                        stream=source_ref.remote_obj_buffer
                    )
                )
            )

    def equals(self, other: PandasdataframeDataProvider):
        """Check equality of two PandasdataframeDataProvider"""
        if isinstance(other, PandasdataframeDataProvider):
            return self.dataset.dataframe.equals(other.dataset.dataframe)
        if isinstance(other, pd.DataFrame):
            return self.dataset.equals(other)
        return False

    def __eq__(self, other):
        return self.equals(other)

    def serialize(self):
        # Store in the metadata DB if Dataframe < 100 kb
        df_size = self.memory_usage(deep=True).sum()
        if df_size < (settings.MAX_DATAFRAME_MEMORY_FOR_XCOM_DB * 1024):
            logger.info("Dataframe size: %s bytes. Storing it in Airflow's metadata DB", df_size)
            return {"data": self.to_json()}
        else:
            logger.info(
                "Dataframe size: %s bytes. Storing it in Remote Storage (conn_id: %s | URL: %s)",
                df_size,
                settings.DATAFRAME_STORAGE_CONN_ID,
                settings.DATAFRAME_STORAGE_URL,
            )
            return convert_dataframe_to_file(self).to_json()

    @staticmethod
    def deserialize(data: dict, version: int):
        if version > 1:
            raise TypeError(f"version > {PandasdataframeDataProvider.version}")
        if isinstance(data, dict) and data.get("class", "") == "File":
            file = File.from_json(data)
            if file.is_dataframe:
                logger.info("Retrieving file from %s using %s conn_id ", file.path, file.conn_id)
                return file.export_to_dataframe()
            return file
        return PandasdataframeDataProvider.from_pandas_df(pd.read_json(data["data"]))

    @classmethod
    def from_pandas_df(cls, df: pd.DataFrame) -> pd.DataFrame | PandasdataframeDataProvider:
        if not settings.NEED_CUSTOM_SERIALIZATION:
            return df
        return cls(df)
