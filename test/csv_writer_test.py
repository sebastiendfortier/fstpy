from tabnanny import verbose
import pytest
import fstpy
from fstpy.utils import get_file_list
from test import TEST_PATH, TMP_PATH
import secrets
from ci_fstcomp import fstcomp
pytestmark = [pytest.mark.regressions]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + '/WriterStd/testsFiles/'




def test_1(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVV5x5_fileSrc.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_1.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()

    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_1.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()

    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_2(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 

    src_file = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_2.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()

    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_2.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()

    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_3(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PF.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_3.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_3.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    
    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_4(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PI.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_4.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_4.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    
    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_5(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PM.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_5.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_5.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    
    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_6(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PU.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_6.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_6.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    
    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



def test_7(plugin_test_dir):
    """Test that compare the new standard file created with the help of the csv writer and the old std file that already exists""" 
    
    src_file = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PZ.std"
    df = fstpy.StandardFileReader(src_file).to_pandas()
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_7.csv"])
    fstpy.delete_file(results_file)
    fstpy.CsvFileWriter(results_file,df).to_csv()
    
    df1 = fstpy.CsvFileReader(results_file).to_pandas()
    results_file2 = ''.join([TMP_PATH, secrets.token_hex(16), "test_7.std"])
    fstpy.StandardFileWriter(results_file2,df1).to_fst()
    
    res = fstcomp(src_file,results_file2,e_max=0.001)
    fstpy.delete_file(results_file2)
    assert(res)



    