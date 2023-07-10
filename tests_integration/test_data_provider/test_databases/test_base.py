import io
import os
import pathlib

import pandas as pd
import pytest
import smart_open
from universal_transfer_operator.data_providers.base import DataStream
from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from utils.test_utils import create_unique_str, export_to_dataframe

CWD = pathlib.Path(__file__).parent

dataset_name = create_unique_str(10)


@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {"name": "SqliteDataProvider", "local_file_path": f"{str(CWD)}/../../data/sample.csv"},
        {
            "name": "SnowflakeDataProvider",
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "BigqueryDataProvider",
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "S3DataProvider",
            "object": File(path=f"s3://tmp9/{dataset_name}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "GCSDataProvider",
            "object": File(path=f"gs://uto-test/{dataset_name}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "LocalDataProvider",
            "object": File(path=f"/tmp/{dataset_name}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "SFTPDataProvider",
            "object": File(path=f"sftp://upload/{dataset_name}.csv"),
            "local_file_path": f"{str(CWD)}/../../data/sample.csv",
        },
        {
            "name": "PandasdataframeDataProvider",
            "object": Dataframe(dataframe=pd.read_csv(f"{str(CWD)}/../../data/sample.csv")),
        },
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider", "object": Table(name=dataset_name)},
        {"name": "BigqueryDataProvider", "object": Table(name=dataset_name)},
        {"name": "SnowflakeDataProvider", "object": Table(name=dataset_name)},
        {
            "name": "S3DataProvider",
            "object": File(path=f"s3://tmp9/{dataset_name}.csv"),
        },
        {
            "name": "GCSDataProvider",
            "object": File(path=f"gs://uto-test/{dataset_name}.csv"),
        },
        {
            "name": "LocalDataProvider",
            "object": File(path=f"/tmp/{dataset_name}.csv"),
        },
        {
            "name": "SFTPDataProvider",
            "object": File(path=f"sftp://upload/{dataset_name}.csv"),
        },
        {
            "name": "PandasdataframeDataProvider",
            "object": Dataframe(dataframe=pd.read_csv(f"{str(CWD)}/../../data/sample.csv")),
        },
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
def test_read_write_methods_of_datasets(src_dataset_fixture, dst_dataset_fixture):
    """
    Test datasets read and write methods of all datasets
    """
    src_dp, _ = src_dataset_fixture
    dst_dp, dataset_object = dst_dataset_fixture
    result = []
    for source_data in src_dp.read():
        result.append(dst_dp.write(source_data))
    output_df = get_dataframe(dst_dp, dataset_object)
    input_df = pd.read_csv(f"{str(CWD)}/../../data/sample.csv")

    assert result == get_results(dst_dp, dataset_object)
    assert output_df.equals(input_df)


def get_dataframe(dst_dp, dataset_object):
    if isinstance(dataset_object, (Table, File)):
        return export_to_dataframe(dst_dp)
    elif isinstance(dataset_object, Dataframe):
        return dataset_object.dataframe


def get_results(dst_dp, dataset_object):
    if isinstance(dataset_object, Table):
        return [dst_dp.get_table_qualified_name(dataset_object)]
    elif isinstance(dataset_object, File):
        return [dataset_object.path]
    elif isinstance(dataset_object, Dataframe):
        return [dst_dp]


# Creating a temp dir for below test, since it's a pattern test, we need to control the files
# that are treated as input.
os.mkdir(f"/tmp/{dataset_name}/")


@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {
            "name": "LocalDataProvider",
            "object": File(path=f"/tmp/{dataset_name}/"),
            "local_file_path": f"{str(CWD)}/../../data/pattern_transfer/",
        }
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {
            "name": "SFTPDataProvider",
            "object": File(path="sftp://upload/"),
        },
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
def test_read_write_methods_of_datasets_with_pattern(src_dataset_fixture, dst_dataset_fixture):
    """
    Test datasets read and write methods when a directory is passed. Expected all the files created are returned as
     result of write() method.
     /data/pattern_transfer/ contains three files - sample_1.csv, sample_2.csv and sample_3.csv
    """
    src_dp, _ = src_dataset_fixture
    dst_dp, _ = dst_dataset_fixture
    result = []
    for source_data in src_dp.read():
        result.append(dst_dp.write(source_data))
    assert dst_dp.check_if_exists("sftp://upload/sample_1.csv")
    assert dst_dp.check_if_exists("sftp://upload/sample_2.csv")
    assert dst_dp.check_if_exists("sftp://upload/sample_3.csv")
    assert dst_dp.check_if_exists("sftp://upload/some_image.png")


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {"name": "SnowflakeDataProvider"},
        {"name": "BigqueryDataProvider"},
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_load_pandas_dataframe_to_table_with_replace(dst_dataset_fixture):
    """Load Pandas Dataframe to a SQL table with replace strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2, 3]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 3
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    pandas_dataframe = pd.DataFrame(data={"id": [3, 4]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (3,)
    assert rows[1] == (4,)

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_load_pandas_dataframe_to_table_with_append(dst_dataset_fixture):
    """Load Pandas Dataframe to a SQL table with append strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
        if_exists="append",
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=dataset,
        if_exists="append",
    )

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 4
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_write_dataframe_to_table_with_replace(dst_dataset_fixture):
    """Write dataframe to a SQL table with replace strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    dst_dp.write(pandas_dataframe)

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    pandas_dataframe = pd.DataFrame(data={"id": [3, 4]})
    dst_dp.write(pandas_dataframe)
    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (3,)
    assert rows[1] == (4,)

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_write_dataframe_to_table_with_append(dst_dataset_fixture):
    """Write dataframe to a SQL table with append strategy"""
    dst_dp, dataset = dst_dataset_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    dst_dp.write(pandas_dataframe)

    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.if_exists = "append"
    dst_dp.write(pandas_dataframe)
    rows = dst_dp.fetch_all_rows(dataset)
    assert len(rows) == 4
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_write_file_datastream_to_table_with_replace(dst_dataset_fixture):
    """Write file datastream to a SQL table with replace strategy"""
    dst_dp, dataset = dst_dataset_fixture

    filepath = f"{str(CWD)}/../../data/sample.csv"
    remote_obj_buffer = io.StringIO()
    with smart_open.open(filepath, mode="r", transport_params=None) as stream:
        remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)

        data_stream = DataStream(
            remote_obj_buffer=remote_obj_buffer,
            actual_filename="sample.csv",
            actual_file=File(path=f"{str(CWD)}/../../data/sample.csv"),
        )

        dst_dp.write(data_stream)

        rows = dst_dp.fetch_all_rows(dataset)
        assert len(rows) == 3
        assert rows[0] == (1, "First")
        assert rows[1] == (2, "Second")
        assert rows[2] == (3, "Third with unicode पांचाल")

        dst_dp.write(data_stream)

        rows = dst_dp.fetch_all_rows(dataset)
        assert len(rows) == 3
        assert rows[0] == (1, "First")
        assert rows[1] == (2, "Second")
        assert rows[2] == (3, "Third with unicode पांचाल")

    dst_dp.drop_table(dataset)


@pytest.mark.parametrize(
    "dst_dataset_fixture",
    [
        {"name": "SqliteDataProvider"},
        {
            "name": "SnowflakeDataProvider",
        },
        {
            "name": "BigqueryDataProvider",
        },
    ],
    indirect=True,
    ids=lambda db: db["name"],
)
def test_write_file_datastream_to_table_with_append(dst_dataset_fixture):
    """Write file datastream to a SQL table with append strategy"""
    dst_dp, dataset = dst_dataset_fixture

    filepath = f"{str(CWD)}/../../data/sample.csv"
    remote_obj_buffer = io.StringIO()
    with smart_open.open(filepath, mode="r", transport_params=None) as stream:
        remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)

        data_stream = DataStream(
            remote_obj_buffer=remote_obj_buffer,
            actual_filename="sample.csv",
            actual_file=File(path=f"{str(CWD)}/../../data/sample.csv"),
        )

        dst_dp.write(data_stream)

        rows = dst_dp.fetch_all_rows(dataset)
        assert len(rows) == 3
        assert rows[0] == (1, "First")
        assert rows[1] == (2, "Second")
        assert rows[2] == (3, "Third with unicode पांचाल")

        dst_dp.if_exists = "append"
        dst_dp.write(data_stream)

        rows = dst_dp.fetch_all_rows(dataset)
        assert len(rows) == 6
        assert rows[3] == (1, "First")
        assert rows[4] == (2, "Second")
        assert rows[5] == (3, "Third with unicode पांचाल")

    dst_dp.drop_table(dataset)
