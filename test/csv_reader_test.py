from array import array
from ast import Try
import copy
from email import header
from importlib.resources import path
import itertools
from lib2to3.pgen2.pgen import DFAState
import multiprocessing as mp
from mimetypes import init
import re
import pandas as pd
import os
import numpy as np
import csv
import datetime
from fstpy import std_enc
import pytest
import fstpy.csv_reader

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


    
"""Test if my to_pandas function works and if my class CsvFileReader works"""
def test_1(input_file,df1):
    csv_file = fstpy.csv_reader.CsvFileReader(path = input_file )
    assert (type(csv_file) == fstpy.csv_reader.CsvFileReader)
    assert (df1.equals(csv_file.to_pandas_no_condition()))

""" Test the exception CsvFileReaderError"""
def test_2(input_file2):
    with pytest.raises(fstpy.csv_reader.CsvFileReaderError):
        csv_file = fstpy.csv_reader.CsvFileReader(path = input_file2)

""" Test the exception NoHeaderInFileError"""
def test_3(input_file):
    with pytest.raises(fstpy.csv_reader.NoHeaderInFileError):
        csv_file = fstpy.csv_reader.CsvFileReader(path = input_file)
        csv_file.to_pandas_no_hdr()

""" Test the exception MinimumHeadersError"""
def test_4(input_file_without_minimum_cl):
    with pytest.raises(fstpy.csv_reader.MinimumHeadersError):
        csv_file = fstpy.csv_reader.CsvFileReader(path = input_file_without_minimum_cl)
        csv_file.to_pandas()

""" Test the exception HeadersAreNotValidError"""
def test_5(input_file_with_wrong_column_name):
    with pytest.raises(fstpy.csv_reader.HeadersAreNotValidError):
        csv_file = fstpy.csv_reader.CsvFileReader(path = input_file_with_wrong_column_name)
        csv_file.to_pandas()

""" Test checkColumns Functions"""
def test_6(input_file_nbits_void,input_file_nbits_24bits):
    csv_file = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_void)
    csv_file.to_pandas_no_condition()
    csv_file.checkNbits()
    csv_file.checkDatyp()
    csv_file.checkTypVar()
    csv_file.checkIp2EtIp3()
    csv_file.checkIg()
    csv_file.checkEticket()
    csv_file.checkLevel()
    print(csv_file.df)
    csv_file2 = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_24bits)
    csv_file2.to_pandas_no_condition()
    print(csv_file2.df)
    assert (csv_file.df.equals(csv_file2.df))

""" Test checkColumns Function"""
def test_7(input_file_nbits_void,input_file_nbits_24bits):
    csv_file = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_void)
    csv_file.to_pandas_no_condition()
    csv_file.checkColumns()
    print(csv_file.df)
    csv_file2 = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_24bits)
    csv_file2.to_pandas_no_condition()
    print(csv_file2.df)
    assert (csv_file.df.equals(csv_file2.df))

""" Test to_pandas Function with check columns and verify headers"""
def test_7(input_file_nbits_void,input_file_nbits_24bits):
    csv_file = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_void)
    csv_file.to_pandas()
    print(csv_file.df)
    csv_file2 = fstpy.csv_reader.CsvFileReader(path = input_file_nbits_24bits)
    csv_file2.to_pandas_no_condition()
    print(csv_file2.df)
    assert (csv_file.df.equals(csv_file2.df))








    






    
            




