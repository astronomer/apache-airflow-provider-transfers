import io
import pathlib
from pathlib import Path

import pandas as pd
import smart_open
from universal_transfer_operator.data_providers.base import DataStream
from universal_transfer_operator.data_providers.dataframe.Pandasdataframe import PandasdataframeDataProvider
from universal_transfer_operator.datasets.dataframe.base import Dataframe
from universal_transfer_operator.datasets.file.base import File

CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"


def test_read():
    df = pd.read_csv(DATA_DIR + "sample.csv")
    dp = PandasdataframeDataProvider(Dataframe(dataframe=df, name="test"))
    returned_dfs = dp.read()
    for returned_df in returned_dfs:
        assert returned_df.equals(df)


def test_write_with_dataframe():
    df = pd.read_csv(DATA_DIR + "sample.csv")
    dp = PandasdataframeDataProvider(Dataframe(dataframe=df, name="test"))
    returned_df = dp.write(df)
    assert returned_df.dataset.dataframe.equals(df)


def get_datastream_object(path: str, mode):
    remote_obj_buffer = io.BytesIO() if mode == "rb" else io.StringIO()
    with smart_open.open(path, mode=mode) as stream:
        remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)
        return DataStream(
            remote_obj_buffer=remote_obj_buffer,
            actual_filename=Path(path),
            actual_file=File(path),
        )


def test_write_with_io_bytesio():
    df = pd.read_csv(DATA_DIR + "sample.csv")
    dp = PandasdataframeDataProvider(Dataframe(name="test"))
    ds = get_datastream_object(path=DATA_DIR + "sample.csv", mode="r")
    returned_df = dp.write(ds)
    assert returned_df.dataset.dataframe.equals(df)
