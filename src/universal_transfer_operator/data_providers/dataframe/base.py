from __future__ import annotations

from typing import Iterator

import pandas as pd
from universal_transfer_operator.data_providers.base import DataProviders, DataStream
from universal_transfer_operator.datasets.dataframe.base import Dataframe


class DataframeProvider(DataProviders[Dataframe]):
    """Base class to import dataframe implementation. We intend to support different implementation of dataframes."""

    def __init__(
        self,
        dataset: Dataframe,
    ):
        self.dataset = dataset
        super().__init__(dataset=self.dataset)

    def read(self) -> Iterator[Dataframe]:
        """Read from dataframe dataset and write to local reference locations or dataframes"""
        raise NotImplementedError

    def write(self, source_ref: pd.DataFrame | DataStream) -> str:  # type: ignore
        """Write the data to the dataframe dataset or filesystem dataset"""
        raise NotImplementedError

    def serialize(self):
        # Store in the metadata DB if Dataframe
        raise NotImplementedError

    @staticmethod
    def deserialize(data: dict, version: int):
        # Extract from metadata DB
        raise NotImplementedError

    def is_native_path_available(
        self,
        source_dataset: Dataframe,  # skipcq PYL-W0613, PYL-R0201
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_dataset: Dataframe from which we need to transfer data
        """
        return False
