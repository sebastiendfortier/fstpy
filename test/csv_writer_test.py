from tabnanny import verbose
import pytest
import fstpy.all as fstpy
from test import TEST_PATH, TMP_PATH
import os
import secrets
from ci_fstcomp import fstcomp
pytestmark = [pytest.mark.regressions]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + '/WriterStd/testsFiles/'




def test_1(plugin_test_dir):
    df = fstpy.StandardFileReader(plugin_test_dir+"UUVV5x5_fileSrc.std").to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_1.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_1.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    res = fstcomp(plugin_test_dir+"UUVV5x5_fileSrc.std",results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)


def test_2(plugin_test_dir):
    df = fstpy.StandardFileReader(plugin_test_dir+"UUVVTT5x5_fileSrc.std").to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_2.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_2.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    res = fstcomp(plugin_test_dir+"UUVVTT5x5_fileSrc.std",results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_3(plugin_test_dir):
    df = fstpy.StandardFileReader(plugin_test_dir+"UUVVTT5x5x2_fileSrc_PF.std").to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_3.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_3.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    res = fstcomp(plugin_test_dir+"UUVVTT5x5x2_fileSrc_PF.std",results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)
