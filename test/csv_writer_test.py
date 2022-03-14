import pytest
from fstpy.csv_writer import CsvFileWriter
from fstpy.std_reader import StandardFileReader, StandardFileReaderError
from test import TEST_PATH
pytestmark = [pytest.mark.unit_tests]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + '/WriterStd/testsFiles/UUVV5x5_fileSrc.std'

def test_1(plugin_test_dir):

    df = StandardFileReader(plugin_test_dir)
    CsvFileWriter(path ="/home/zak000/src/WriterCsvFiles/test1.csv",df = df)
