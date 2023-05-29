# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from test import TMP_PATH, TEST_PATH
import datetime
import fstpy
from datetime import datetime, timedelta
import numpy as np
import pathlib

pytestmark = [pytest.mark.unit_tests]

@pytest.fixture
def input_file():
    return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'
    
@pytest.fixture
def simple_df(input_file):
    df = fstpy.StandardFileReader(input_file).to_pandas()
    df = df.loc[df.ip1.isin([95178882,95154915,95529009,97351772,96219256]) & (df.nomvar == 'TT')]
    return df
    
@pytest.fixture
def mixed_df(simple_df, full_df):
    df = pd.concat([simple_df, full_df],ignore_index=True)
    return df


def test_1(input_file):
    """Test adding a localized date of validity in Canada/Eastern"""
    src_df0 = fstpy.StandardFileReader(input_file).to_pandas()

    src_df0 = fstpy.add_columns(src_df0,'datev')

    src_df0 = fstpy.add_timezone_column(src_df0,'date_of_validity','Canada/Eastern')

    assert('date_of_validity_Canada_Eastern' in src_df0.columns)

    assert(not src_df0.loc[src_df0.date_of_validity_Canada_Eastern==datetime(2020,7,14,8)].empty)

def test_2(simple_df):
    """check that add_grid_column does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df.drop('grid',axis=1,inplace=True)

    assert(len(simple_df.columns) == 21)

    simple_df = fstpy.add_grid_column(simple_df)

    assert(len(simple_df.columns) == 22)
    assert(simple_df.grid.unique().size == 1)
    assert(simple_df.grid.unique()[0] == '3379277761')

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('grid',axis=1)
    simple_df = simple_df.loc[simple_df.ip1!=95529009]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)

    assert(simple_df.grid.unique().size == 2)
    
    if not simple_df.loc[simple_df.grid.isna()].empty:
        simple_df.loc[simple_df.grid.isna(), 'ig1'] = 1234567
    simple_df = fstpy.add_grid_column(simple_df)
    
    assert('123456777761' in list(simple_df.grid.unique())) 

    pass

def test_3(simple_df, input_file):
    """Check that add_path_and_key_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df.drop(['path','key'],axis=1, inplace=True, errors='ignore')
    
    assert(len(simple_df.columns) == 22)
    simple_df = fstpy.add_path_and_key_columns(simple_df)
    
    assert(len(simple_df.columns) == 24)
    assert(simple_df.path.unique().size == 1)
    assert(simple_df.key.unique().size == 5)
    
    removed_df = simple_df.loc[simple_df.ip1==95529009].drop(['path','key'],axis=1)
    simple_df = simple_df.loc[simple_df.ip1!=95529009]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)

    assert(simple_df.path.unique().size == 2)
    assert(simple_df.key.unique().size == 5)
    
    simple_df = fstpy.add_path_and_key_columns(simple_df)

    assert(pathlib.Path(simple_df.path.unique()[0]) == pathlib.Path(input_file))
    assert(list(simple_df.key.unique()).sort() == [22529,4097,5121,23553,54273].sort())


def test_4(simple_df):
    """Check that add_timezone_column does not replace existing values"""
    assert(len(simple_df.columns) == 22)
    
    simple_df.drop('date_of_validity_Canada_Eastern',axis=1, inplace=True, errors='ignore')
    
    assert(len(simple_df.columns) == 22)
    
    simple_df = fstpy.add_columns(simple_df,'datev')
    
    assert(len(simple_df.columns) == 23)
    
    simple_df = fstpy.add_timezone_column(simple_df,'date_of_validity','Canada/Eastern')
    
    assert(len(simple_df.columns) == 24)
    assert('date_of_validity_Canada_Eastern' in simple_df.columns)
    
    simple_df.date_of_validity_Canada_Eastern.unique().size
    
    assert(simple_df.date_of_validity_Canada_Eastern.unique().size == 1)

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('date_of_validity_Canada_Eastern',axis=1)
    removed_df['date_of_validity'] = removed_df['date_of_validity'] + timedelta(hours=2)
    simple_df = simple_df.loc[simple_df.ip1!=95529009]
    simple_df.loc[simple_df.ip1==97351772,'date_of_validity'] = simple_df.loc[simple_df.ip1==97351772].iloc[0].date_of_validity + timedelta(hours=3)
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)

    assert(simple_df.date_of_validity_Canada_Eastern.unique().size == 2)

    simple_df = fstpy.add_timezone_column(simple_df,'date_of_validity','Canada/Eastern')
    
    assert(simple_df.date_of_validity_Canada_Eastern.unique().size == 2)
    assert(np.array_equal(simple_df.date_of_validity_Canada_Eastern.unique(), [np.datetime64('2020-07-14T16'), np.datetime64('2020-07-14T14')]))
    

def test_5(simple_df):
    """Check that add_flag_values does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_flag_values(simple_df)

    assert(len(simple_df.columns) == 32)

    for col in ['multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('unit_converted',axis=1)
    removed_df['typvar'] = 'PU'
    simple_df = simple_df.loc[simple_df.ip1!=95529009]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'typvar'] = 'PZ'

    assert(simple_df.unit_converted.unique().size == 2)

    simple_df = fstpy.add_flag_values(simple_df)

    assert(simple_df.unit_converted.unique().size == 2)

def test_6(simple_df):
    """Check that add_parsed_etiket_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_parsed_etiket_columns(simple_df)

    assert(len(simple_df.columns) == 27)

    for col in ['label', 'run', 'implementation', 'ensemble_member']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('label',axis=1)

    removed_df.drop('d',axis=1)
    removed_df['etiket'] = 'R1_V810_N'
    simple_df = simple_df.loc[simple_df.ip1!=95529009]

    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'etiket'] = 'R1_V910_N'
    assert(simple_df.label.unique().size == 2)

    simple_df = fstpy.add_parsed_etiket_columns(simple_df)

    assert(simple_df.label.unique().size == 2)

def test_7(simple_df):
    """Check that add_unit_and_description_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_unit_and_description_columns(simple_df)

    assert(len(simple_df.columns) == 24)

    for col in ['unit', 'description']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('unit',axis=1)

    removed_df.drop('d',axis=1)
    removed_df['nomvar'] = 'UV'
    simple_df = simple_df.loc[simple_df.ip1!=95529009]

    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'nomvar'] = 'WW'
    simple_df.loc[simple_df.ip1 == 97351772, 'unit'] = 'kelvin'

    assert(simple_df.unit.unique().size == 3)

    simple_df = fstpy.add_unit_and_description_columns(simple_df)

    assert(simple_df.unit.unique().size == 3)

    simple_df = fstpy.unit_convert(simple_df.loc[simple_df.nomvar=='TT'],'kelvin')

    assert(simple_df.unit.unique().size == 1)

    simple_df = fstpy.add_unit_and_description_columns(simple_df)

    assert(simple_df.unit.unique()[0] == 'kelvin')

def test_8(simple_df):
    """Check that add_decoded_date_column does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df.drop('date_of_observation',axis=1,errors='ignore',inplace=True)
    simple_df.drop('date_of_validity',axis=1,errors='ignore',inplace=True)

    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_decoded_date_column(simple_df,'dateo')
    simple_df = fstpy.add_decoded_date_column(simple_df,'datev')

    assert(len(simple_df.columns) == 24)
    assert(simple_df.date_of_observation.unique().size == 1)
    assert(simple_df.date_of_validity.unique().size == 1)
    assert(simple_df.date_of_observation.unique()[0] == np.datetime64('2020-07-14T12'))
    assert(simple_df.date_of_validity.unique()[0] == np.datetime64('2020-07-14T18'))

    removed_df1 = simple_df.loc[simple_df.ip1==95529009].drop('date_of_observation',axis=1)
    removed_df2 = simple_df.loc[simple_df.ip1==95178882].drop('date_of_validity',axis=1)
    removed_df1.dateo = [442998801]
    removed_df2.datev = [443004201]
    simple_df = simple_df.loc[~simple_df.ip1.isin([95529009,95178882])]
    simple_df = pd.concat([removed_df1,removed_df2,simple_df],ignore_index = True)

    assert(simple_df.date_of_observation.unique().size == 2)
    assert(simple_df.date_of_validity.unique().size == 2)


    simple_df = fstpy.add_decoded_date_column(simple_df,'dateo')
    simple_df = fstpy.add_decoded_date_column(simple_df,'datev')

    assert(np.datetime64('2020-07-14T12:00:05') in list(simple_df.date_of_observation.unique())) 
    assert(np.datetime64('2020-07-14T18:00:05') in list(simple_df.date_of_validity.unique()))     


def test_9(simple_df):
    """Check that add_forecats_hour_column does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df.drop('forecast_hour',axis=1,errors='ignore',inplace=True)

    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_forecast_hour_column(simple_df)


    assert(len(simple_df.columns) == 23)
    assert(simple_df.forecast_hour.unique().size == 1)


    assert(simple_df.forecast_hour.unique()[0] == np.timedelta64(21600000000000,'ns'))


    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('forecast_hour',axis=1)

    removed_df.npas = [74]

    simple_df = simple_df.loc[~simple_df.ip1.isin([95529009])]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)

    assert(simple_df.forecast_hour.unique().size == 2)

    simple_df.loc[simple_df.ip1==96219256,'npas'] = 76
    simple_df = fstpy.add_forecast_hour_column(simple_df)

    assert(np.timedelta64(22200000000000,'ns') in list(simple_df.forecast_hour.unique()))     


def test_10(simple_df):
    """Check that add_data_type_str_column does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df.drop('data_type_str',axis=1,errors='ignore',inplace=True)

    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_data_type_str_column(simple_df)

    assert(len(simple_df.columns) == 23)
    assert(simple_df.data_type_str.unique().size == 1)
    assert(simple_df.data_type_str.unique()[0] == 'f')

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('data_type_str',axis=1)
    removed_df.datyp = [5]
    simple_df = simple_df.loc[~simple_df.ip1.isin([95529009])]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)
    
    assert(simple_df.data_type_str.unique().size == 2)

    simple_df.loc[simple_df.ip1==96219256,'datyp'] = 3
    simple_df = fstpy.add_data_type_str_column(simple_df)

    assert('E' in list(simple_df.data_type_str.unique()))     

def test_11(simple_df):
    """Check that add_ip_info_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df = fstpy.add_ip_info_columns(simple_df)
    
    assert(len(simple_df.columns) == 35)

    for col,value in {'level':5, 'ip1_kind':1, 'ip1_pkind':1, 'ip2_dec':1, 'ip2_kind':1, 'ip2_pkind':1, 'ip3_dec':1, 'ip3_kind':1, 'ip3_pkind':1, 'surface':1, 'follow_topography':1, 'ascending':1, 'interval':1}.items():
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == value)

    removed_df = simple_df.loc[simple_df.ip1==95529009].drop('level',axis=1)
    removed_df['ip1'] = 97351773
    simple_df = simple_df.loc[simple_df.ip1!=95529009]
    simple_df = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'ip1'] = 96219256
    
    assert(simple_df.level.unique().size == 5)

    simple_df = fstpy.add_ip_info_columns(simple_df)

    assert(simple_df.level.unique().size == 5)
    

def test_12(simple_df):
    """Check that add_flag_values does not replace existing values"""

    simple_df = fstpy.add_flag_values(simple_df)

    simple_df.typvar = 'PZ'
    simple_df.bounded = True
    simple_df.zapped  = True
    cols  = ['nomvar', 'typvar', 'zapped', 'bounded','filtered','interpolated']

    simple_df_modified = simple_df.drop(labels=['zapped'], axis=1)

    simple_df_modified = fstpy.add_flag_values(simple_df_modified)

    simple_df          = simple_df.loc         [:, cols]
    simple_df_modified = simple_df_modified.loc[:, cols]

    assert((simple_df_modified == simple_df).all().all())

def test_13(simple_df):
    """Check that reduce_flag_values interprets the values of the different flags and updates the typvar accordingly"""
    
    simple_df_new = fstpy.add_flag_values(simple_df)
    simple_df_new.typvar = 'P'

    # Ajouts de 2 rows a la fin pour fins de tests
    simple_df_new.reset_index(inplace=True)
    row = simple_df_new.loc[simple_df_new.index == 0]
    simple_df_new = pd.concat([simple_df_new, row, row], ignore_index=True)

    # Cas 0, masked et ensemble
    simple_df_new.loc[simple_df_new.index == 0, 'masked'] = True
    simple_df_new.loc[simple_df_new.index == 0, 'ensemble_extra_info'] = True

    # Cas 1 masked seulement
    simple_df_new.loc[simple_df_new.index == 1, 'masked'] = True

    # Cas 2 masks seulement
    simple_df_new.loc[simple_df_new.index == 2, 'masks'] = True

    # Cas 3 donnees manquantes
    simple_df_new.loc[simple_df_new.index == 3, 'missing_data'] = True

    # Cas 4, modifications multiples
    simple_df_new.loc[simple_df_new.index == 4,'zapped'] = True
    simple_df_new.loc[simple_df_new.index == 4, 'bounded'] = True

    # Cas 5 - modif simple, interpolation
    simple_df_new.loc[simple_df_new.index == 5, 'interpolated'] = True

    # Cas 6 - masked ET multiple_modifications
    simple_df_new.loc[simple_df_new.index == 6, 'multiple_modifications'] = True
    simple_df_new.loc[simple_df_new.index == 6, 'masked'] = True

    simple_df_new = fstpy.reduce_flag_values(simple_df_new)

    assert simple_df_new.loc[simple_df_new.index == 0, 'typvar'].item() == '!@'
    assert simple_df_new.loc[simple_df_new.index == 1, 'typvar'].item() == 'P@'
    assert simple_df_new.loc[simple_df_new.index == 2, 'typvar'].item() == '@@'
    assert simple_df_new.loc[simple_df_new.index == 3, 'typvar'].item() == 'PH'
    assert simple_df_new.loc[simple_df_new.index == 4, 'typvar'].item() == 'PM'
    assert simple_df_new.loc[simple_df_new.index == 5, 'typvar'].item() == 'PI'
    assert simple_df_new.loc[simple_df_new.index == 6, 'typvar'].item() == 'P@'
