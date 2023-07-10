import pandas as pd
import pytest

from universal_transfer_operator.data_providers import create_dataprovider, get_dataprovider_options_class
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.data_providers.database.snowflake import (
    SnowflakeDataProvider,
    SnowflakeOptions,
)
from universal_transfer_operator.data_providers.database.sqlite import SqliteDataProvider
from universal_transfer_operator.data_providers.dataframe.Pandasdataframe import PandasdataframeDataProvider
from universal_transfer_operator.data_providers.filesystem.aws.s3 import S3DataProvider
from universal_transfer_operator.data_providers.filesystem.google.cloud.gcs import GCSDataProvider
from universal_transfer_operator.data_providers.filesystem.sftp import SFTPDataProvider
from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.integrations.base import TransferIntegrationOptions


@pytest.mark.parametrize(
    "datasets",
    [
        {"dataset": File("s3://astro-sdk-test/uto/", conn_id="aws_default"), "expected": S3DataProvider},
        {"dataset": File("gs://uto-test/uto/", conn_id="google_cloud_default"), "expected": GCSDataProvider},
        {"dataset": File("sftp://upload/sample.csv", conn_id="sftp_default"), "expected": SFTPDataProvider},
        {"dataset": Table("some_table", conn_id="sqlite_default"), "expected": SqliteDataProvider},
        {"dataset": Table("some_table", conn_id="snowflake_conn"), "expected": SnowflakeDataProvider},
        {"dataset": Dataframe(dataframe=pd.DataFrame()), "expected": PandasdataframeDataProvider},
    ],
    ids=lambda d: d["expected"],
)
def test_create_dataprovider(datasets):
    """Test that the correct data-provider is created for a dataset"""
    data_provider = create_dataprovider(dataset=datasets["dataset"])
    assert isinstance(data_provider, datasets["expected"])


@pytest.mark.parametrize(
    "datasets",
    [
        {
            "dataset": File("s3://astro-sdk-test/uto/", conn_id="aws_default"),
            "expected": TransferIntegrationOptions,
        },
        {
            "dataset": File("gs://uto-test/uto/", conn_id="google_cloud_default"),
            "expected": TransferIntegrationOptions,
        },
        {
            "dataset": File("sftp://upload/sample.csv", conn_id="sftp_default"),
            "expected": TransferIntegrationOptions,
        },
        {"dataset": Table("some_table", conn_id="sqlite_default"), "expected": TransferIntegrationOptions},
        {"dataset": Table("some_table", conn_id="snowflake_conn"), "expected": SnowflakeOptions},
    ],
    ids=lambda d: d["dataset"].conn_id,
)
def test_get_option_class(datasets):
    """Test that the correct options class is created for a dataset"""
    option_class = get_dataprovider_options_class(dataset=datasets["dataset"])
    assert isinstance(option_class, type(datasets["expected"]))


def test_raising_of_not_implemented_error_by_subclass_of_data_providers():
    """
    Test that the class inheriting from DataProviders should implement methods
    """

    class Test(DataProviders):
        pass

    test = Test(dataset=File("/tmp/test.csv"))

    with pytest.raises(NotImplementedError):
        test.read()

    with pytest.raises(NotImplementedError):
        test.write(source_ref=pd.DataFrame())
