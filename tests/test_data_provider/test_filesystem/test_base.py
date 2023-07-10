import pathlib

import pytest

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

CWD = pathlib.Path(__file__).parent


class BaseFilesystemProvidersSubclass(BaseFilesystemProviders):
    def paths(self):
        pass

    def size(self):
        pass


def test_openlineage_database_dataset_namespace():
    """
    Test the open lineage dataset namespace for base class
    """
    dp = BaseFilesystemProvidersSubclass(dataset=File("/tmp/test.csv"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        dp.openlineage_dataset_namespace()


def test_openlineage_database_dataset_name():
    """
    Test the open lineage dataset names for the base class
    """
    dp = BaseFilesystemProvidersSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        dp.openlineage_dataset_name(table=Table)


def test_raising_of_NotImplementedError_by_subclass_of_dataProviders():
    """
    Test that the class inheriting from DataProviders should implement methods
    """

    methods = [
        "openlineage_dataset_namespace",
        "openlineage_dataset_name",
        "openlineage_dataset_uri",
        "check_if_exists",
        "hook",
    ]

    test = BaseFilesystemProvidersSubclass(
        dataset=File("/tmp/test.csv"), transfer_mode=TransferMode.NONNATIVE
    )
    for method in methods:
        with pytest.raises(NotImplementedError):
            m = test.__getattribute__(method)
            m()

    with pytest.raises(NotImplementedError):
        test.delete(path="")
