from __future__ import annotations

import os
from pathlib import Path

import attr
from airflow.hooks.dbapi import DbApiHook

from universal_transfer_operator.constants import Location
from universal_transfer_operator.data_providers.base import DataProviders, contextmanager
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.universal_transfer_operator import TransferParameters
from universal_transfer_operator.utils import get_dataset_connection_type


@attr.define
class TempFile:
    tmp_file: Path | None
    actual_filename: Path


class BaseFilesystemProviders(DataProviders):
    """BaseFilesystemProviders represent all the DataProviders interactions with File system."""

    def __init__(
        self,
        dataset: Dataset,
        transfer_mode,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = {}
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def check_if_transfer_supported(self, source_dataset: Dataset) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return Location(source_connection_type) in self.transfer_mapping

    @contextmanager
    def read(self) -> list[TempFile]:
        """Read the file dataset and write to local file location"""
        raise NotImplementedError

    def write(self, source_ref) -> list[TempFile]:
        """Write the source data from local file location to the dataset"""
        raise NotImplementedError

    @staticmethod
    def cleanup(file_list: list[TempFile]) -> None:
        """Cleans up the temporary files created"""
        for file in file_list:
            if os.path.exists(file.tmp_file.name):
                os.remove(file.tmp_file.name)

    def load_data_from_source_natively(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using data provider
        """
        if not self.check_if_transfer_supported(source_dataset=source_dataset):
            raise ValueError("Transfer not supported yet.")

        source_connection_type = get_dataset_connection_type(source_dataset)
        method_name = self.LOAD_DATA_NATIVELY_FROM_SOURCE.get(source_connection_type)
        if method_name:
            transfer_method = self.__getattribute__(method_name)
            return transfer_method(
                source_dataset=source_dataset,
                destination_dataset=destination_dataset,
            )
        else:
            raise ValueError(f"No transfer performed from {source_connection_type} to S3.")

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError
