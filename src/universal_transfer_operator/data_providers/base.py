from __future__ import annotations

import io
from abc import ABC
from pathlib import Path
from typing import Generic, Iterator, TypeVar

import attr
import pandas as pd

from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

DatasetType = TypeVar("DatasetType", File, Table, Dataframe)


@attr.define
class DataStream:
    remote_obj_buffer: io.IOBase
    actual_filename: Path
    actual_file: File


class DataProviders(ABC, Generic[DatasetType]):
    """
    Base class to represent all the DataProviders interactions with Dataset.

    The goal is to be able to support new dataset by adding
    a new module to the `uto/data_providers` directory, without the need of
    changing other modules and classes.
    """

    def __init__(
        self,
        dataset: DatasetType,
    ):
        self.dataset: DatasetType = dataset

    def read(self) -> Iterator[pd.DataFrame] | Iterator[DataStream]:
        """Read from filesystem dataset or databases dataset and write to local reference locations or dataframes"""
        raise NotImplementedError

    def write(self, source_ref: pd.DataFrame | DataStream) -> str:  # type: ignore
        """Write the data from local reference location or a dataframe to the database dataset or filesystem dataset

        :param source_ref: Stream of data to be loaded into output table or a pandas dataframe.
        """
        raise NotImplementedError
