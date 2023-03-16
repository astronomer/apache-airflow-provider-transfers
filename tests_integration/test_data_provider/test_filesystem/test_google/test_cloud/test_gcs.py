import pathlib

import pytest
from utils.test_utils import create_unique_str

from universal_transfer_operator.datasets.file.base import File

CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "src_dataset_fixture",
    [
        {
            "name": "GCSDataProvider",
            "local_file_path": f"{str(CWD)}/../../../../data/sample.csv",
            "object": File(path=f"gs://uto-test/{create_unique_str(10)}.csv"),
        }
    ],
    indirect=True,
    ids=lambda dp: dp["name"],
)
def test_delete_gcs_object(src_dataset_fixture):
    dp, _ = src_dataset_fixture
    assert dp.check_if_exists()
    dp.delete()
    assert not dp.check_if_exists()
