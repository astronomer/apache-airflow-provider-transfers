from __future__ import annotations

import importlib
from typing import Type, cast

from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.integrations.base import TransferIntegrationOptions
from universal_transfer_operator.utils import get_class_name

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {
    ("s3", File): "universal_transfer_operator.data_providers.filesystem.aws.s3",
    ("aws", File): "universal_transfer_operator.data_providers.filesystem.aws.s3",
    ("gs", File): "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
    ("google_cloud_platform", File): "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
    ("sftp", File): "universal_transfer_operator.data_providers.filesystem.sftp",
    ("google_cloud_platform", Table): "universal_transfer_operator.data_providers.database.google.bigquery",
    ("gs", Table): "universal_transfer_operator.data_providers.database.google.bigquery",
    ("sqlite", Table): "universal_transfer_operator.data_providers.database.sqlite",
    ("snowflake", Table): "universal_transfer_operator.data_providers.database.snowflake",
    (None, File): "universal_transfer_operator.data_providers.filesystem.local",
    (None, Dataframe): "universal_transfer_operator.data_providers.dataframe.Pandasdataframe",
}


def create_dataprovider(
    dataset: Table | File | Dataframe,
    transfer_params: TransferIntegrationOptions | None = None,
    transfer_mode: TransferMode = TransferMode.NONNATIVE,
) -> DataProviders:
    if transfer_params is None:
        transfer_params = TransferIntegrationOptions()

    class_ref: type[DataProviders] = get_dataprovider_class(dataset=dataset)
    if isinstance(dataset, (Table, File)):
        data_provider = class_ref(  # type: ignore
            dataset=dataset,
            transfer_params=transfer_params,
            transfer_mode=transfer_mode,
        )
    elif isinstance(dataset, Dataframe):
        data_provider = class_ref(dataset=dataset)  # type: ignore
    return data_provider


def get_dataprovider_class(dataset: Table | File | Dataframe) -> type[DataProviders]:
    """
    Get dataprovider class based on the dataset
    """
    conn_type = None
    if isinstance(dataset, (Table, File)) and getattr(dataset, "conn_id", None):
        conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[(conn_type, type(dataset))]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    return cast(Type[DataProviders], getattr(module, class_name))


def get_dataprovider_options_class(dataset: Table | File) -> type[TransferIntegrationOptions]:
    """
    Get options class based on the dataset
    """
    class_ref = get_dataprovider_class(dataset=dataset)
    return getattr(class_ref, "OPTIONS_CLASS", TransferIntegrationOptions)
