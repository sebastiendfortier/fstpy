# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import pytest
import fstpy.all as fstpy
from ci_fstcomp import fstcomp
from test import TEST_PATH, TMP_PATH
pytestmark = [pytest.mark.csv_reader, pytest.mark.unit_tests]


@pytest.fixture
def input_file():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'
@pytest.fixture
def input_file2():
    return '/home/zak000/src/notebooks/readerCv_notebook/test2_src.csv'

@pytest.fixture
def input_file_without_minimum_cl():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test4_src.csv'
@pytest.fixture
def input_file_with_wrong_column_name():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test5_src.csv'
@pytest.fixture
def input_file_nbits_void():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test6_src.csv'
@pytest.fixture
def input_file_nbits_24bits():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test6_src_2.csv'

@pytest.fixture
def df1():
    df = pd.read_csv('/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv',comment="#")
    return df

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"ReaderCsv/testsFiles/"


    
"""Test if my to_pandas function works and if my class CsvFileReader works"""
def test_1(input_file,df1):
    csv_file = fstpy.CsvFileReader(path = input_file )
    assert (type(csv_file) == fstpy.CsvFileReader)
    assert (df1.equals(csv_file.to_pandas_no_condition()))

""" Test the exception CsvFileReaderError"""
def test_2(input_file2):
    with pytest.raises(fstpy.CsvFileReaderError):
        csv_file = fstpy.CsvFileReader(path = input_file2)

""" Test the exception NoHeaderInFileError"""
def test_3(input_file):
    with pytest.raises(fstpy.NoHeaderInFileError):
        csv_file = fstpy.CsvFileReader(path = input_file)
        csv_file.to_pandas_no_hdr()

""" Test the exception MinimumHeadersError"""
def test_4(input_file_without_minimum_cl):
    with pytest.raises(fstpy.MinimumHeadersError):
        csv_file = fstpy.CsvFileReader(path = input_file_without_minimum_cl)
        csv_file.to_pandas()

""" Test the exception HeadersAreNotValidError"""
def test_5(input_file_with_wrong_column_name):
    with pytest.raises(fstpy.HeadersAreNotValidError):
        csv_file = fstpy.CsvFileReader(path = input_file_with_wrong_column_name)
        csv_file.to_pandas()

""" Test checkColumns Function"""
def test_7(input_file_nbits_void,input_file_nbits_24bits):
    csv_file2 = fstpy.CsvFileReader(path = input_file_nbits_24bits)
    file2_df= csv_file2.to_pandas_no_condition()
    file2_df["dateo"] = fstpy.create_encoded_dateo(datetime.datetime.utcnow())
    csv_file = fstpy.CsvFileReader(path = input_file_nbits_void)
    csv_file.to_pandas_no_condition()
    # print(dfd.to_string())
    csv_file.checkColumns()
    #csv_file.change_columns_type()
    print(csv_file.df.to_string())
    print(csv_file2.df.to_string())
    print(csv_file.df.dtypes)
    assert (csv_file.df.equals(csv_file2.df))

""" Test to_pandas Function with check columns and verify headers"""
def test_8(input_file_nbits_void,input_file_nbits_24bits):
    csv_file2 = fstpy.CsvFileReader(path = input_file_nbits_24bits)
    csv_file2.to_pandas_no_condition()
    csv_file2.df["dateo"] = fstpy.create_encoded_dateo(datetime.datetime.utcnow())
    csv_file = fstpy.CsvFileReader(path = input_file_nbits_void)
    csv_file.to_pandas()
    print(csv_file.df)
    print(csv_file2.df)
    assert (csv_file.df.equals(csv_file2.df))


def test_9(plugin_test_dir):
    src = "/home/sbf000/csv/pds1_level2.new.csv.format"
    df = fstpy.CsvFileReader(src).to_pandas()
    df["dateo"] =  360451883
    df = fstpy.add_grid_column(df)
    print(df.to_string())
    print(df.dtypes)
    
    results_file = TMP_PATH + "test_9.std"
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file,df).to_fst()
    #open and read comparison file
    file_to_compare = plugin_test_dir + "pds1_level2_file2cmp.std"
    #compare results
    res = fstcomp(results_file,file_to_compare)
    fstpy.delete_file(results_file)
    assert(res)





    






    
            




