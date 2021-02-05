# -*- coding: utf-8 -*-
import pytest
from fstpy.standardfile import *
import pandas as pd
from test import TEST_PATH




@pytest.mark.std_reader
class TestStandardFileReader:

    @pytest.fixture
    def input_file(self):
        return TEST_PATH + '/ReaderStd_WriterStd/testsFiles/source_data_5005.std'
        
    def test_open(self,input_file):
        std_file = StandardFileReader(input_file)
        assert type(std_file) == StandardFileReader   
        assert std_file.meta_data == StandardFileReader.meta_data


    def test_params_read_meta_fields_only(self,input_file):
        std_file = StandardFileReader(input_file,read_meta_fields_only=True)
        df = std_file.to_pandas()
        assert len(df.index) == 9
        assert len(df.columns) == 51


    def test_params_add_extra_columns_false(self,input_file):
        std_file = StandardFileReader(input_file,add_extra_columns=False)
        df = std_file.to_pandas()
        assert len(df.index) == 1865
        assert len(df.columns) == 34


    def test_params_add_extra_columns_false_materialize(self,input_file):
        std_file = StandardFileReader(input_file,materialize=True,add_extra_columns=False,subset={'nomvar':'UU'})
        df = std_file.to_pandas()
        assert len(df.index) == 85
        assert len(df.columns) == 34
        assert 'd' in df.columns 
        assert (df['d'] is None) == False


    def test_params_materialize_true(self,input_file):
        std_file = StandardFileReader(input_file,materialize=True,subset={'nomvar':'UU'})
        df = std_file.to_pandas()
        assert len(df.index) == 85
        assert len(df.columns) == 51   
        assert 'd' in df.columns 
        assert (df['d'] is None) == False


    def test_params_subset(self,input_file):
        std_file = StandardFileReader(input_file,subset={'nomvar':'TT'})
        df = std_file.to_pandas()
        assert len(df.index) == 85
        assert len(df.columns) == 51   


    def test_params_materialize_true_subset(self,input_file):
        std_file = StandardFileReader(input_file,materialize=True,subset={'nomvar':'TT'})
        df = std_file.to_pandas()
        assert len(df.index) == 85
        assert len(df.columns) == 51   
        assert 'd' in df.columns 
        assert (df['d'] is None) == False


    def test_params_subset_all(self,input_file):
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
        