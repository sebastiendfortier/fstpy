# -*- coding: utf-8 -*-
# from fstpy.dataframe import sort_dataframe
import pytest
from fstpy.std_reader import *
from test import TEST_PATH

pytestmark = [pytest.mark.std_reader, pytest.mark.unit_tests]

@pytest.fixture
def input_file():
    return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'

@pytest.fixture
def input_file2():
    return TEST_PATH + '/ReaderStd/testsFiles/input_big_fileSrc.std'
    
def test_1(input_file):
    """Test open a file"""
    std_file = StandardFileReader(input_file)
    assert type(std_file) == StandardFileReader
    assert std_file.meta_data == StandardFileReader.meta_data


def test_2(input_file):
    """Test decode_metadata=False"""
    std_file = StandardFileReader(input_file,decode_metadata=False)
    df = std_file.to_pandas()
    assert len(df.index) == 1874
    assert len(df.columns) == 26
    assert df.d.isna().all()

def test_3(input_file):
    """Test decode_metadata=True"""
    std_file = StandardFileReader(input_file,decode_metadata=True)
    df = std_file.to_pandas()
    assert len(df.index) == 1874
    assert len(df.columns) == 58
    assert df.d.isna().all()

# def test_4(input_file):
#     """Test load_data=True,decode_metadata=False"""
#     std_file = StandardFileReader(input_file,load_data=True,decode_metadata=False,query='nomvar=="UU"')
#     df = std_file.to_pandas()
#     assert len(df.index) == 90
#     assert len(df.columns) == 26
#     assert 'd' in df.columns
#     assert not df.d.isna().all()

# def test_5(input_file):
#     """Test load_data=True,decode_metadata=True"""
#     std_file = StandardFileReader(input_file,load_data=True,decode_metadata=True,query='nomvar=="UU"')
#     df = std_file.to_pandas()
#     assert len(df.index) == 90
#     assert len(df.columns) == 58
#     assert 'd' in df.columns
#     assert not df.d.isna().all()

# def test_6(input_file):
#     """Test load_data=True"""
#     std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="UU"')
#     df = std_file.to_pandas()
#     assert len(df.index) == 90
#     assert len(df.columns) == 26
#     assert 'd' in df.columns
#     assert not df.d.isna().all()


def test_7(input_file):
    """Test query='nomvar=='TT'"""
    std_file = StandardFileReader(input_file,query='nomvar=="TT"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 26
    assert df.d.isna().all()

# def test_8(input_file):
#     """Test load_data=True,query='nomvar=='TT'"""
#     std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="TT"')
#     df = std_file.to_pandas()
#     assert len(df.index) == 90
#     assert len(df.columns) == 26
#     assert 'd' in df.columns
#     assert not df.d.isna().all()


def test_9(input_file):
    """Test query all and reasssemble"""
    std_file = StandardFileReader(input_file)
    df = std_file.to_pandas()

    nomvars = {f"{n}" for n in df['nomvar']}
    full_df = StandardFileReader(input_file).to_pandas()
    full_df = full_df.loc[full_df.nomvar.isin(list(nomvars))]

    assert df.columns.equals(full_df.columns)
    assert len(df.index) == len(full_df.index)

    assert full_df.columns.all() == df.columns.all()
    assert full_df.nomvar.unique().all() == df.nomvar.unique().all()
    assert full_df.typvar.unique().all() == df.typvar.unique().all()
    assert full_df.ni.all() == df.ni.all()
    assert full_df.nj.all() == df.nj.all()
    assert full_df.nk.all() == df.nk.all()
    assert full_df.dateo.all() == df.dateo.all()
    assert full_df.ip1.all() == df.ip1.all()
    assert full_df.ip2.all() == df.ip2.all()
    assert full_df.ip3.all() == df.ip3.all()
    assert full_df.deet.all() == df.deet.all()
    assert full_df.npas.all() == df.npas.all()
    assert full_df.ig1.all() == df.ig1.all()
    assert full_df.ig2.all() == df.ig2.all()
    assert full_df.ig3.all() == df.ig3.all()
    assert full_df.ig4.all() == df.ig4.all()
    assert full_df.grtyp.all() == df.grtyp.all()
    assert full_df.datyp.all() == df.datyp.all()
    assert full_df.nbits.all() == df.nbits.all()

def test_10(input_file,input_file2):
    """Test opening multiple files"""
    std_file = StandardFileReader([input_file,input_file2])
    df = std_file.to_pandas()
    assert len(df.index) == 2009
    assert len(df.columns) == 26
    assert df.d.isna().all()
    