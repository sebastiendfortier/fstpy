# -*- coding: utf-8 -*-
import pytest
from fstpy.standardfile import *
import pandas as pd
from test import TMP_PATH, TEST_PATH


@pytest.mark.std_functions
class TestStandardFileFunctions:

    @pytest.fixture
    def input_file(self):
        return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'
        
    @pytest.fixture
    def raw_dict(self):
        return {
            'nomvar': 'TT',
            'typvar': 'P',
            'etiket': 'R1_V710_N',
            'ni': 1108,
            'nj': 1082,
            'nk': 1,
            'ip1': 76696048,
            'ip2': 6,
            'ip3': 0,
            'deet': 300,
            'npas': 72,
            'nbits': 16,
            'grtyp': 'Z',
            'ig1': 33792,
            'ig2': 77761,
            'ig3': 1,
            'ig4': 0,
            'datev': 443004200,
            'swa': 41837002,
            'lng': 333402,
            'dltf': 0,
            'ubc': 0,
            'xtra1': 443004200,
            'xtra2': 0,
            'xtra3': 0,
            'kind': 4,
            'dateo': 442998800,
            'datyp': 134,
            'key': 659457,
            'shape': (1108, 1082, 1),
            }

    @pytest.fixture
    def file_mod_time(self):
        return '446325:47:42'

    @pytest.fixture
    def full_data_frame(raw_dict):
        import datetime
        rec_dict_to_add = {
            'pdateo': datetime.datetime(2020, 7, 14, 12, 0),
            'level': 1.5,
            'pkind': 'M',
            'pdatyp': 'f',
            'e_run': 'R1',
            'e_implementation': 'N',
            'e_ensemble_member': None,
            'd': None,
            'pdatev': datetime.datetime(2020, 7, 14, 18, 0),
            'path': '/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std',
            'file_modification_time': '446325:47:42',
            'surface': True,
            'follow_topography': True,
            'dirty': False,
            'vctype': '',
            'fhour': 6.0,
            'e_label': '_V710_',
            'materialize_info': None,
            'unit_converted': False
            }
        final_dict = dict(rec_dict_to_add, **raw_dict)    
        df = pd.DataFrame(final_dict)   
        del datetime
        return df

    @pytest.fixture
    def no_extra_data_frame(self,raw_dict):
        rec_dict_to_add = {
            'path': '/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std',
            'file_modification_time': '446325:47:42',
            'materialize_info': None,
            }
        final_dict = dict(rec_dict_to_add, **raw_dict)    
        df = pd.DataFrame(final_dict)        
        return df

    @pytest.fixture
    def raw_materialized_data_frame(self,raw_dict):
        rec_dict_to_add = {
            'd': None,
            }
        final_dict = dict(rec_dict_to_add, **raw_dict)    
        df = pd.DataFrame(final_dict)       
        return df

    @pytest.fixture
    def raw_data_frame(self,raw_dict):
        df = pd.DataFrame(raw_dict)    
        return df

    def test_add_path_and_modification_time(self,raw_data_frame, input_file, file_mod_time):
        assert 'path' not in raw_data_frame.columns
        assert 'file_modification_time' not in raw_data_frame.columns
        df = add_path_and_modification_time(raw_data_frame, input_file, file_mod_time)
        assert 'path' in df.columns
        assert 'file_modification_time' in df.columns
        assert df.iloc[0]['path'] == input_file
        assert df.iloc[0]['file_modification_time'] == file_mod_time

    
    def test_add_path_and_modification_time_wrong(self,raw_data_frame, input_file, file_mod_time):
        assert 'path' not in raw_data_frame.columns
        assert 'file_modification_time' not in raw_data_frame.columns
        df = add_path_and_modification_time(raw_data_frame, input_file, file_mod_time)
        assert 'path' in df.columns
        assert 'file_modification_time' in df.columns
        assert df.iloc[0]['path'] != 'dummy_file'
        assert df.iloc[0]['file_modification_time'] != '12345'

    #materialize, add_extra_columns
    def test_add_missing_columns(self,raw_data_frame, input_file, file_mod_time):
        assert 'path' not in raw_data_frame.columns
        assert 'file_modification_time' not in raw_data_frame.columns
        df = add_path_and_modification_time(raw_data_frame, input_file, file_mod_time)
        assert 'path' in df.columns
        assert 'file_modification_time' in df.columns


    
    # def test_strip_string_columns(self,df):
    #     pass

    
    # def test_get_2d_lat_lon(self,df:pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_update_meta_data(self,df, i):
    #     pass

    
    # def test_identical_destination_and_record_path(self,record_path, filename):
    #     pass

    
    # def test_reshape_data_to_original_shape(self,df, i):
    #     pass

    
    # def test_change_etiket_if_a_plugin_name(self,df, i):
    #     pass

    
    # def test_remove_df_columns(self,df,keys_to_keep = {'key','dateo', 'deet', 'npas', 'ni', 'nj', 'nk', 'datyp', 'nbits', 'ip1', 'ip2', 'ip3', 'typvar', 'nomvar', 'etiket', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'}):
    #     pass

    
    # def test_fst_to_df(self,file_id, exception_class, materialize, subset, read_meta_fields_only):
    #     pass

    
    # def test_get_meta_record_keys(file_id):
    #     pass

    
    # def test_get_all_record_keys(self,file_id, subset):
    #     pass

    
    # def test_get_file_modification_time(self,path:str,caller,exception_class):
    #     pass

    
    # def test_get_std_etiket(self,plugin_name:str):
    #     pass

    
    # def test_add_columns(self,df:pd.DataFrame):
    #     pass

    
    # def test_set_level_and_kind(self,df, i):
    #     pass

    
    # def test_get_unit(self,df, i, nomvar):
    #     pass

    
    # def test_strip_df_strings(self,df, i):
    #     pass

    
    # def test_set_follow_topography(self,df, i):
    #     pass

    
    # def test_set_surface(self,df, i, meter_levels):
    #     pass

    
    # def test_get_level_and_kind(self,ip1:int):
    #     pass

    
    # def test_materialize(self,df:pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_reorder_dataframe(self,df):
    #     pass

    
    # def test_compare_modification_times(self,df, path, caller):
    #     pass

    
    # def test_resize_data(self,df:pd.DataFrame, dim1:int,dim2:int) -> pd.DataFrame:
    #     pass

    
    # def test_create_grid_identifier(self,nomvar:str,ip1:int,ip2:int,ig1:int,ig2:int) -> str:
    #     pass

    
    # def test_create_printable_date_of_observation(self,date:int):
    #     pass

    
    # def test_rmn_dateo_to_datetime_object(self,dateo:int):
    #     pass

    
    # def test_remove_from_df(self,df_to_remove_from:pd.DataFrame, df_to_remove) -> pd.DataFrame:
    #     pass

    
    # def test_select(self,df:pd.DataFrame, query_str:str, exclude:bool=False, no_meta_data=False, loose_match=False, no_fail=False, engine=None) -> pd.DataFrame:
    #     pass

    
    # def test_select_zap(self,df:pd.DataFrame, query:str, **kwargs:dict) -> pd.DataFrame:
    #     pass

    
    # def test_get_intersecting_levels(self,df:pd.DataFrame, names:list) -> pd.DataFrame:
    #     pass

    
    # def test_add_empty_columns(self,df, columns, init):
    #     pass

    
    # def test_compute_stats(self,df:pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_validate_zap_keys(self,**kwargs):
    #     pass

    
    # def test_zap_ip1(self,df:pd.DataFrame, ip1_value:int) -> pd.DataFrame:
    #     pass

    
    # def test_zap_level(self,df:pd.DataFrame, level_value:float, kind_value:int) -> pd.DataFrame:
    #     pass

    
    # def test_zap_kind(self,df:pd.DataFrame, kind_value:int) -> pd.DataFrame:
    #     pass

    
    # def test_zap_pkind(self,df:pd.DataFrame, pkind_value:str) -> pd.DataFrame:
    #     pass

    
    # def test_zap_npas(self,df:pd.DataFrame, npas_value:int) -> pd.DataFrame:
    #     pass

    
    # def test_zap_fhour(self,df:pd.DataFrame, fhour_value:int) -> pd.DataFrame:
    #     pass

    
    # def test_create_materialize_info(self,df:pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_zap(self,df:pd.DataFrame, mark:bool=True, validate_keys=True,**kwargs:dict ) -> pd.DataFrame:
    #     pass

    
    # def test_add_fstcomp_columns(self,diff: pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_del_fstcomp_columns(self,diff: pd.DataFrame) -> pd.DataFrame:
    #     pass

    
    # def test_compute_fstcomp_stats(self,common: pd.DataFrame, diff: pd.DataFrame) -> bool:
    #     pass

    
    # def test_remove_meta_for_fstcomp(self,df: pd.DataFrame):
    #     pass

    
    # def test_keys_to_remove(self,keys, the_dict):
    #     pass

    
    # def test_get_lat_lon(self,df):
    #     pass

    
    # def test_get_meta_data_fields(self,df,latitude_and_longitude=True, pressure=True, vertical_descriptors=True):
    #     pass

    
    # def test_add_metadata_fields(self,df:pd.DataFrame, latitude_and_longitude=True, pressure=True, vertical_descriptors=True) -> pd.DataFrame:
    #     pass

    
    # def test_parse_etiket(self,raw_etiket:str):
    #     pass

