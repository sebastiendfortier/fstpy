import pytest
import fstpy.all as fstpy
from test import TEST_PATH
import os
pytestmark = [pytest.mark.regressions]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + '/WriterStd/testsFiles/UUVV5x5_fileSrc.std'

def test_1(plugin_test_dir):
    df = fstpy.StandardFileReader(plugin_test_dir,decode_metadata=False).to_pandas()
    df = fstpy.compute(df)
    fstpy.CsvFileWriter(path ="/home/zak000/src/WriterCsvFiles/test1.csv",df = df)
    assert(os.path.exists("/home/zak000/src/WriterCsvFiles/test1.csv"))
