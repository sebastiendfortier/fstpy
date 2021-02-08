# -*- coding: utf-8 -*-
from pandas.core.dtypes.missing import isnull
import pytest
from fstpy.standardfile import *
import pandas as pd
from test import TEST_PATH

pytestmark = [pytest.mark.std_reader, pytest.mark.unit_tests]

@pytest.fixture
def input_file():
    return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'


def test_open(input_file):
    std_file = StandardFileReader(input_file)
    assert type(std_file) == StandardFileReader   
    assert std_file.meta_data == StandardFileReader.meta_data

def test_params_read_meta_fields_only(input_file):
    std_file = StandardFileReader(input_file,read_meta_fields_only=True)
    df = std_file.to_pandas()
    assert len(df.index) == 9
    assert len(df.columns) == 51


def test_params_add_extra_columns_false(input_file):
    std_file = StandardFileReader(input_file,add_extra_columns=False)
    df = std_file.to_pandas()
    assert len(df.index) == 1874
    assert len(df.columns) == 34


def test_params_add_extra_columns_false_materialize(input_file):
    std_file = StandardFileReader(input_file,materialize=True,add_extra_columns=False,subset={'nomvar':'UU'})
    df = std_file.to_pandas()
    assert len(df.index) == 85
    assert len(df.columns) == 34
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()


def test_params_materialize_true(input_file):
    std_file = StandardFileReader(input_file,materialize=True,subset={'nomvar':'UU'})
    df = std_file.to_pandas()
    assert len(df.index) == 85
    assert len(df.columns) == 51   
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()


def test_params_subset(input_file):
    std_file = StandardFileReader(input_file,subset={'nomvar':'TT'})
    df = std_file.to_pandas()
    assert len(df.index) == 85
    assert len(df.columns) == 51   


def test_params_materialize_true_subset(input_file):
    std_file = StandardFileReader(input_file,materialize=True,subset={'nomvar':'TT'})
    df = std_file.to_pandas()
    assert len(df.index) == 85
    assert len(df.columns) == 51   
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()


def test_params_subset_all(input_file):
    std_file = StandardFileReader(input_file)
    df = std_file.to_pandas()

    nomvars = df.nomvar.unique()

    dfs = []
    for nv in nomvars:
        dfs.append(StandardFileReader(input_file,subset={'nomvar':'%s'%nv})())

    full_df = pd.concat(dfs)

    assert df.columns.equals(full_df.columns)
    assert len(df.index) == len(full_df.index)

    df = reorder_dataframe(df)
    full_df = reorder_dataframe(full_df)
    assert df.equals(full_df)
