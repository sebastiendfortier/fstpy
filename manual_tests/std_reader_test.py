# -*- coding: utf-8 -*-
from fstpy.std_reader import *
from manual_tests import TEST_PATH


input_file = TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'


def test_open():
    std_file = StandardFileReader(input_file)
    assert type(std_file) == StandardFileReader   
    assert std_file.meta_data == StandardFileReader.meta_data


def test_params_decode_metadata_false():
    std_file = StandardFileReader(input_file,decode_metadata=False)
    df = std_file.to_pandas()
    assert len(df.index) == 1874
    assert len(df.columns) == 26

def test_params_decode_metadata_True():
    std_file = StandardFileReader(input_file,decode_metadata=True)
    df = std_file.to_pandas()
    assert len(df.index) == 1874
    assert len(df.columns) == 58

def test_params_decode_metadata_false_load_data():
    std_file = StandardFileReader(input_file,load_data=True,decode_metadata=False,query='nomvar=="UU"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 26
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()

def test_params_decode_metadata_true_load_data():
    std_file = StandardFileReader(input_file,load_data=True,decode_metadata=True,query='nomvar=="UU"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 58
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()

def test_params_load_data_true():
    std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="UU"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 26   
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()


def test_params_query():
    std_file = StandardFileReader(input_file,query='nomvar=="TT"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 26   


def test_params_load_data_true_query():
    std_file = StandardFileReader(input_file,load_data=True,query='nomvar=="TT"')
    df = std_file.to_pandas()
    assert len(df.index) == 90
    assert len(df.columns) == 26   
    assert 'd' in df.columns 
    assert not df['d'].isnull().all()


def test_params_query_all():
    std_file = StandardFileReader(input_file)
    df = std_file.to_pandas()

    nomvars = {f"{n}" for n in df['nomvar']}
    full_df = StandardFileReader(input_file).to_pandas().query("nomvar in %s"%list(nomvars))
    
    assert df.columns.equals(full_df.columns)
    assert len(df.index) == len(full_df.index)

    df = sort_dataframe(df)
    full_df = sort_dataframe(full_df)
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



def main():
    test_open()
    test_params_decode_metadata_false()
    test_params_decode_metadata_True()
    test_params_decode_metadata_false_load_data()
    test_params_decode_metadata_true_load_data()
    test_params_load_data_true()
    test_params_query()
    test_params_load_data_true_query()
    test_params_query_all()


if __name__ == "__main__":
    main()



