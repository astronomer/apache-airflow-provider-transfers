from __future__ import annotations

from typing import Any

from attr import define, field

from universal_transfer_operator.datasets.base import Dataset


@define
class Dataframe(Dataset):
    """
    Repersents all dataframe dataset.
    Intended to be used within library.
    :param name: name of dataframe

    """

    name: str = field(default="")
    dataframe: Any = field(default=None)
    uri: str = field(init=None)  # type: ignore

    # TODO: define the name and namespace for dataframe

    @uri.default
    def _path_to_dataset_uri(self) -> str:
        """Build a URI to be passed to Dataset obj introduced in Airflow 2.4"""
        return self.name
