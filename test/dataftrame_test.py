# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from test import TMP_PATH, TEST_PATH
import datetime
import rpnpy.librmn.all as rmn
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
def simple_df2(input_file):
    df = fstpy.StandardFileReader(input_file).to_pandas()
    df = df.loc[df.ip1.isin([95178882,95154915,95529009,97351772,96219256])]
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

    simple_df            = fstpy.add_flag_values(simple_df)
    assert(len(simple_df.columns) == 32)

    for col in ['multiple_modifications', 'zapped', 'filtered', 'interpolated', 'unit_converted', 'bounded', 'missing_data', 'ensemble_extra_info']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df           = simple_df.loc[simple_df.ip1==95529009].drop('unit_converted',axis=1)
    removed_df['typvar'] = 'PU'
    simple_df            = simple_df.loc[simple_df.ip1!=95529009]
    simple_df            = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'typvar'] = 'PZ'
    assert(simple_df.unit_converted.unique().size == 2)

    simple_df            = fstpy.add_flag_values(simple_df)

    assert(simple_df.unit_converted.unique().size == 2)

def test_6(simple_df):
    """Check that add_parsed_etiket_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df            = fstpy.add_parsed_etiket_columns(simple_df)
    assert(len(simple_df.columns) == 27)

    for col in ['label', 'run', 'implementation', 'ensemble_member']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df           = simple_df.loc[simple_df.ip1==95529009].drop('label',axis=1)
    removed_df.drop('d',axis=1)
    removed_df['etiket'] = 'R1_V810_N'
    simple_df            = simple_df.loc[simple_df.ip1!=95529009]

    simple_df            = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'etiket'] = 'R1_V910_N'
    assert(simple_df.label.unique().size == 2)

    simple_df            = fstpy.add_parsed_etiket_columns(simple_df)

    assert(simple_df.label.unique().size == 2)

def test_7(simple_df):
    """Check that add_unit_and_description_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df            = fstpy.add_unit_and_description_columns(simple_df)
    assert(len(simple_df.columns) == 24)

    for col in ['unit', 'description']:
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == 1)

    removed_df           = simple_df.loc[simple_df.ip1==95529009].drop('unit',axis=1)
    removed_df.drop('d',axis=1)
    removed_df['nomvar'] = 'UV'
    simple_df            = simple_df.loc[simple_df.ip1!=95529009]

    simple_df            = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'nomvar'] = 'WW'
    simple_df.loc[simple_df.ip1 == 97351772, 'unit'] = 'kelvin'
    assert(simple_df.unit.unique().size == 3)

    simple_df            = fstpy.add_unit_and_description_columns(simple_df)
    assert(simple_df.unit.unique().size == 3)

    simple_df            = fstpy.unit_convert(simple_df.loc[simple_df.nomvar=='TT'],'kelvin')
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
    assert(simple_df.date_of_validity.unique().size    == 1)
    assert(simple_df.date_of_observation.unique()[0]   == np.datetime64('2020-07-14T12'))
    assert(simple_df.date_of_validity.unique()[0]      == np.datetime64('2020-07-14T18'))

    removed_df1       = simple_df.loc[simple_df.ip1==95529009].drop('date_of_observation',axis=1)
    removed_df2       = simple_df.loc[simple_df.ip1==95178882].drop('date_of_validity',axis=1)
    removed_df1.dateo = [442998801]
    removed_df2.datev = [443004201]
    simple_df         = simple_df.loc[~simple_df.ip1.isin([95529009,95178882])]
    simple_df         = pd.concat([removed_df1,removed_df2,simple_df],ignore_index = True)

    assert(simple_df.date_of_observation.unique().size == 2)
    assert(simple_df.date_of_validity.unique().size    == 2)


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

    simple_df        = fstpy.add_data_type_str_column(simple_df)

    assert(len(simple_df.columns) == 23)
    assert(simple_df.data_type_str.unique().size == 1)
    assert(simple_df.data_type_str.unique()[0] == 'f')

    removed_df       = simple_df.loc[simple_df.ip1==95529009].drop('data_type_str',axis=1)
    removed_df.datyp = [5]
    simple_df        = simple_df.loc[~simple_df.ip1.isin([95529009])]
    simple_df        = pd.concat([removed_df,simple_df],ignore_index = True)
    
    assert(simple_df.data_type_str.unique().size == 2)

    simple_df.loc[simple_df.ip1==96219256,'datyp'] = 3
    simple_df        = fstpy.add_data_type_str_column(simple_df)

    assert('E' in list(simple_df.data_type_str.unique()))     

def test_11(simple_df):
    """Check that add_ip_info_columns does not replace existing values"""
    assert(len(simple_df.columns) == 22)

    simple_df          = fstpy.add_ip_info_columns(simple_df)
    
    assert(len(simple_df.columns) == 35)

    for col,value in {'level':5, 'ip1_kind':1, 'ip1_pkind':1, 'ip2_dec':1, 'ip2_kind':1, 'ip2_pkind':1, 'ip3_dec':1, 'ip3_kind':1, 'ip3_pkind':1, 'surface':1, 'follow_topography':1, 'ascending':1, 'interval':1}.items():
        assert(col in simple_df.columns)
        assert(simple_df[col].unique().size == value)

    removed_df        = simple_df.loc[simple_df.ip1==95529009].drop('level',axis=1)
    removed_df['ip1'] = 97351773
    simple_df         = simple_df.loc[simple_df.ip1!=95529009]
    simple_df         = pd.concat([removed_df,simple_df],ignore_index = True)
    simple_df.loc[simple_df.ip1 == 97351772, 'ip1'] = 96219256
    
    assert(simple_df.level.unique().size == 5)

    simple_df         = fstpy.add_ip_info_columns(simple_df)

    assert(simple_df.level.unique().size == 5)
    
def test_12(simple_df):
    """Check that add_flag_values does not replace existing values"""

    simple_df         = fstpy.add_flag_values(simple_df)
    simple_df.typvar  = 'PZ'
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
    
    simple_df_new        = fstpy.add_flag_values(simple_df)
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

def test_14(simple_df2):
    """Check that add_decoded_date_column works when called twice, when metadata information is present"""
    
    assert(len(simple_df2.columns) == 22)

    simple_df2 = fstpy.add_decoded_date_column(simple_df2, 'dateo')
    simple_df2 = fstpy.add_decoded_date_column(simple_df2, 'datev')
    
    # 2ieme appel
    simple_df2 = fstpy.add_decoded_date_column(simple_df2, 'dateo')
    simple_df2 = fstpy.add_decoded_date_column(simple_df2, 'datev')

    assert(len(simple_df2.columns) == 24)

def test_15(simple_df):
    """Check that add_ip_info_columns does not replace existing values"""
    
    simple_df = simple_df.loc[simple_df.ip1==95178882]

    simple_df = fstpy.add_ip_info_columns(simple_df)

    ip1 = 95178882
    ip3 = 96219256
    kind = int(simple_df.iloc[0].ip1_kind)
    simple_df.interval = fstpy.Interval('ip1', ip1, ip3, kind)

    cols = ['nomvar', 'ip1', 'ip2', 'ip3', 'level', 'ip1_kind', 'ip1_pkind', 'ip2_dec', 'ip2_kind', 'ip2_pkind','ip3_dec', 'ip3_kind', 'follow_topography', 'ascending', 'interval']
    simple_df_modified = simple_df.drop(labels=['level'], axis=1)

    simple_df_modified = fstpy.add_ip_info_columns(simple_df_modified)

    simple_df          = simple_df.loc         [:, cols]
    simple_df_modified = simple_df_modified.loc[:, cols]

    assert((simple_df_modified == simple_df).all().all())

def test_16(simple_df):
    """Check that add_parsed_etiket_columns does not replace existing values"""
    # from pandas.testing import assert_frame_equal
    simple_df = simple_df.loc[(simple_df['ip1'] == 95529009) & (simple_df['nomvar'] == 'TT')]
    simple_df = fstpy.add_parsed_etiket_columns(simple_df)

    # Modification de la valeur du label
    simple_df.loc[simple_df.nomvar == 'TT', 'label'] = "_TEST_"
    simple_df_modified = simple_df.drop(labels=['run'], axis=1)

    # Appel a la fonction pour remettre a jour les donnees manquantes
    simple_df_modified  = fstpy.add_parsed_etiket_columns(simple_df_modified)

    # Verification que la valeur du label n'a pas ete ecrasee et que la run a ete mise a jour
    cols      = ['nomvar','etiket', 'run', 'implementation', 'label', 'ensemble_member', 'etiket_format']
    simple_df          = simple_df.loc         [:, cols]
    simple_df_modified = simple_df_modified.loc[:, cols]

    assert simple_df_modified.equals(simple_df)

def test_17(simple_df):
    """Check that reduce_parsed_etiket_columns recreates the etiket from the values from its sub-columns"""
    
    simple_df_new       = fstpy.add_columns(simple_df, 'etiket')

    # On drop la colonne implementation et modifie les infos de run et label
    simple_df_new.drop(labels='implementation', axis=1, inplace=True)
    simple_df_new.label = '_V900_'
    simple_df_new.run   = 'R2'

    # On veut s'assurer que la fonction reparse l'etiket pour ajouter la colonne implementation
    # sans ecraser les autres valeurs modifiees (run et label), et recree l'etiket
    simple_df_new = fstpy.reduce_parsed_etiket_columns(simple_df_new)

    # Verifions que la bonne etiket a ete cree a partir des colonnes label et run modifies
    etiket = simple_df_new.etiket.unique()
    assert(etiket == 'R2_V900_N')

    # Verifions que les dataframes sont identiques a l'exception de la colonne etiket
    cols_df       = simple_df.columns
    cols          = list(set(cols_df) - set(['etiket']))
    simple_df     = simple_df.loc[:, cols]
    simple_df_new = simple_df_new.loc[:, cols]

    assert((simple_df_new == simple_df).all().all())   

def test_18(simple_df):
    """Check that the function remove_list_of_columns does the good work
    """
    assert(len(simple_df.columns) == 22)
    simple_df_new  = fstpy.add_unit_and_description_columns(simple_df)
    assert(len(simple_df_new.columns) == 24)
    # S'assurer que ca ne plante pas meme si certaines colonnes sont absentes (ip2kind et level)
    simple_df_new  = fstpy.remove_list_of_columns(simple_df_new, ['unit', 'description', 'ip2kind', 'level'])

    assert(len(simple_df_new.columns) == 22)

def test_19(simple_df):
    """Check that the function reduce_forecast_hour_column updates npas when necessary
    """
    simple_df_new  = fstpy.add_forecast_hour_column(simple_df)
    npas           = simple_df.npas.unique()
    assert(npas == 72)

    # Multiplions le forecast_hour (6) par 6 => 1 day 12 h (36)
    simple_df_new.forecast_hour = simple_df_new.forecast_hour * 6
    simple_df_new  = fstpy.reduce_forecast_hour_column(simple_df_new)
    npas_new       = simple_df_new.npas.unique()

    # Npas ajuste?  (forecast_hour/deet = npas) (36 h * 3600 s)/300 = 432
    assert(npas_new == 432)

def test_20(simple_df):
    """Check the reduce_forecast_hour_column and update_ip2_from_forecast_hour when working with seconds
    """
    simple_df_new  = fstpy.add_forecast_hour_column(simple_df)
    simple_df_new.reset_index(drop=True, inplace=True)

    # Multiplions le forecast_hour (6) par 6.34 => 1 day 14 h 2 min 24 sec (38.04)
    simple_df_new.forecast_hour = simple_df_new.forecast_hour * 6.34
    simple_df_new  = fstpy.reduce_forecast_hour_column(simple_df_new)
    simple_df_new  = fstpy.update_ip2_from_forecast_hour(simple_df_new)

    # Nouvelle valeur encodee de ip2, a decoder pour comparer avec forecast_hour
    ip2_new = simple_df_new.iloc[0].ip2
    ip2_dec = rmn.convertIp(rmn.CONVIP_DECODE, int(ip2_new), 10)

    forecast_hour = simple_df_new.iloc[0].forecast_hour/pd.Timedelta(hours=1)
    assert(round(ip2_dec[0],2) == forecast_hour)

    # Ajustement du npas mais pas du deet, ceci ne nous permet pas d'avoir la precision pour 
    # representer les secondes (forecast_hour = 38.04 h)
    # deet (300) * npas (456) / 3600 = 38 h
    npas_new       = simple_df_new.iloc[4].npas 

    assert(npas_new == 456)

def test_21(simple_df):
    """Test function update_ip2_from_ip2dec avec ip2 non-encode et encode """

    simple_df_new         = fstpy.add_ip_info_columns(simple_df)
    simple_df_new.ip2_dec = 18.0  
    simple_df_new.reset_index(drop=True, inplace=True)

    # Test avec ip2 non encode
    simple_df_new         = fstpy.update_ip2_from_ip2dec(simple_df_new)
    new_ip2               = simple_df_new.iloc[0].ip2
    assert(new_ip2 == 18)

    # Test avec un ip2 encode
    ip2_enc               = rmn.convertIp(rmn.CONVIP_ENCODE, int(new_ip2), 10)
    simple_df_new.ip2     = ip2_enc
    simple_df_new.ip2_dec = 12.0 
    simple_df_new         = fstpy.update_ip2_from_ip2dec(simple_df_new)
    new_ip2               = simple_df_new.iloc[0].ip2

    assert(new_ip2        == 176280768)

def test_22(simple_df):
    """Test function update_ip1_from_level niveaux hybrides """
    simple_df_new         = fstpy.add_ip_info_columns(simple_df)

    simple_df_new.reset_index(drop=True, inplace=True)
    simple_df_new.at[0,'level'] = 0.934233
    simple_df_new               = fstpy.update_ip1_from_level(simple_df_new)
    new_ip1 = simple_df_new.iloc[0].ip1

    assert(new_ip1 == 95306073)

def test_23():
    """Test function update_ip1_from_level niveaux en millibars, non encodes """

    source    = TEST_PATH + '/testsFiles/2013022712_012_regpres'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[(simple_df.nomvar == 'ES')]
    simple_df = fstpy.add_ip_info_columns(simple_df)

    simple_df.reset_index(drop=True, inplace=True)
    old_ip1   =  simple_df.at[4,'ip1']

    simple_df.at[4,'level'] = 975.0

    simple_df = fstpy.update_ip1_from_level(simple_df)
    new_ip1   = simple_df.iloc[4].ip1

    assert(old_ip1 == 925)
    assert(new_ip1 == 975)

def test_24():
    """Test function update_ip1_from_level niveaux en millibars, encodes """

    source    = TEST_PATH + '/testsFiles/TTUUVV_24h.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[(simple_df.nomvar == 'TT')]
    simple_df = fstpy.add_ip_info_columns(simple_df)

    simple_df.reset_index(drop=True, inplace=True)
    old_ip1   =  simple_df.at[1,'ip1']

    simple_df.at[1,'level'] = 975.0

    simple_df = fstpy.update_ip1_from_level(simple_df)
    new_ip1   = simple_df.iloc[1].ip1

    assert(old_ip1 == 41694464)
    assert(new_ip1 == 41869464)

def test_25(simple_df):
    """Test function update_ip3_from_ip3dec and reduce_interval_columns with a height interval """

    source    = TEST_PATH + 'MinMaxLevelIndex/testsFiles/test_ICGA_file2cmp_20201202.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[(simple_df.nomvar == 'IND')]
    simple_df = fstpy.add_ip_info_columns(simple_df)
    simple_df.reset_index(drop=True, inplace=True)
    simple_df_new = fstpy.add_ip_info_columns(simple_df)

    # Modification du ip3_dec 
    simple_df_new.ip3_dec = 600.0
    simple_df_new.reset_index(drop=True, inplace=True)

    # MAJ du ip3 
    simple_df_new         = fstpy.update_ip3_from_ip3dec(simple_df_new)
    new_ip3               = simple_df_new.iloc[0].ip3

    # valeur de 600 mb encode
    assert(new_ip3 == 41494464 )

    # Reduction de la colonne interval... la valeur originale de 500 mb devrait etre remise 
    # dans le ip3 car elle est dans l'intervalle
    # Modifions aussi le ip1... a  1000 mb
    simple_df_new.ip1           = 39945888 
    simple_df_new               = fstpy.reduce_interval_columns(simple_df_new)
    new_ip1                     = simple_df_new.iloc[0].ip1
    new_ip3                     = simple_df_new.iloc[0].ip3

    # valeur de 850 mb pour ip1 et 500 mb pour ip3, tels que dans l'intervalle
    assert(new_ip1 == 41744464)
    assert(new_ip3 == 41394464 )

    # Suppression des colonnes inutiles pour fins de verification
    cols = ['level', 'ip1_kind', 'ip1_pkind', 'ip2_dec', 'ip2_kind', 'ip2_pkind', 'ip3_dec','ip3_kind', 'ip3_pkind', 'interval']
    cols_to_remove = [x for x in cols if x in simple_df.columns]
    simple_df.drop(labels=cols_to_remove, axis=1, inplace=True)
    simple_df_new.drop(labels=cols_to_remove, axis=1, inplace=True)

    assert((simple_df_new == simple_df).all().all()) 

def test_26(simple_df):
    """Test function reduce_interval_columns with an interval of time """

    source    = TEST_PATH + 'TimeIntervalDifference/testsFiles/PR_Interval_6-9_15-18_fileSrc_encoded.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()

    simple_df = fstpy.add_ip_info_columns(simple_df)
    simple_df = simple_df.loc[(simple_df.nomvar == 'PR') & (simple_df.ip3_dec == 6.0)]

    # Modification de l'interval de "ip2:6.0H@9.0H" a "ip2:12.0H@22.0H"
    simple_df.interval = "ip2:12.0H@22.0H"

    # Reduction de la colonne interval... les valeurs modifiees de l'interval devraient etre mises 
    # dans les ip2 et ip3 
    simple_df                   = fstpy.reduce_interval_columns(simple_df)
    new_ip2                     = simple_df.iloc[0].ip2
    new_ip3                     = simple_df.iloc[0].ip3

    # Nouvelles valeurs encodees ip2 = 22 H et ip3 = 12 H
    assert(new_ip2 == 176380768)
    assert(new_ip3 == 176280768)

def test_27(simple_df):
    """Test function reduce_decoded_date_column """

    simple_df = fstpy.add_decoded_date_column(simple_df,'dateo')
    simple_df = fstpy.add_decoded_date_column(simple_df,'datev')
    simple_df.reset_index(drop=True, inplace=True)

    # Modifions date of origin
    simple_df.date_of_observation = np.datetime64('2020-07-22T00:02:00')

    simple_df = fstpy.reduce_decoded_date_column(simple_df)  

    # Verifions que dateo = 2020072200020000   et 
    # datev = 2020072206020000 (dateo + (deet*npas/3600))
    assert(simple_df.dateo.unique()[0] == 443160830)
    assert(simple_df.datev.unique()[0] == 443166230)

def test_28(simple_df):
    """Test function reduce_columns """

    source    = TEST_PATH + 'MinMaxLevelIndex/testsFiles/test_ICGA_file2cmp_20201202.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[(simple_df.nomvar == 'IND')]
    # Pour avoir un dataframe contenant un deet et npas != 0
    simple_df.deet = 300
    simple_df.npas = 288
    simple_df.ip2  = rmn.convertIp(rmn.CONVIP_ENCODE,24, 10)

    # Ajout de toutes les colonnes
    simple_df = fstpy.add_columns(simple_df, ['etiket', 'flags', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info'])
    
    # Partie etiquette
    simple_df.label = '_TEST_'
    simple_df.run   = 'G1'

    # Partie forecast_hour
    simple_df.forecast_hour = pd.Timedelta(hours=6)

    # Partie date
    simple_df.date_of_observation = np.datetime64('2023-07-14T00:02:00')

    # Partie flags
    simple_df.interpolated = True
    simple_df.filtered     = True

    # Partie ip et interval
    simple_df.interval = "ip1:1000.0mb@200.0mb"
    simple_df.level    = 750
    simple_df.ip3_dec  = 350
    
    # Dataframe ressemble a ceci, avec informations incoherentes: 
    # label = "_TEST_" , run = G1,  etiket = __MINMAXX
    # forecast_hour = 0 days 06:00:00 , deet = 300, npas = 288, ip2 = 176400768
    # date_of_observation(origin)  =  2023-07-14 00:02:00 date_of_validity = 2009-11-22 13:34:15
    # dateo = 359041013 et datev = 359041013
    # les flags bounded, interpolated et filtered a TRUE, typvar = PB
    # interval = ip1:1000.0mb@200.0mb, level = 750, ip3_dec 350, ip1 = 41744464, ip3 = 41394464 

    # Reduction de toutes les colonnes
    simple_df = fstpy.reduce_columns(simple_df)

    assert(simple_df.etiket[0] == "G1_TEST_X")
    assert(simple_df.npas[0]   == 72)         # Correspond a forecast_hour * 3600 / deet
    assert(simple_df.dateo[0]  == 466640030)  # Correspond a 2023071400020000
    assert(simple_df.datev[0]  == 466645430)  # Correspond a 2023071406020000
    assert(simple_df.typvar[0] == "PM")       # M for multiple modifications

    assert(simple_df.ip1[0]    == 39945888)   # Correspond a 1000 mb encode
    assert(simple_df.ip3[0]    == 41094464)   # Correspond a 200 mb encode
    assert(simple_df.ip2[0]    == 177809344)  # Correspond a 6 H encode 

    assert(len(simple_df.columns) == 24)

def test_29(simple_df):
    """Test function reduce_columns avec fichier contenant P0 et PT """

    source    = TEST_PATH + 'testsFiles/2016010600_009_regeta_GZ.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[simple_df.nomvar.isin(['P0', 'PT']) | ((simple_df.nomvar == "GZ") & (simple_df.ip1 == 28257976)) ]

    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)

    # Ajout de toutes les colonnes
    simple_df = fstpy.add_columns(simple_df, ['etiket', 'flags', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info'])

    # Modification de forecast_hour et date_of_origin
    simple_df.date_of_observation = np.datetime64('2023-07-14T00:02:00')
    simple_df.forecast_hour = pd.Timedelta(hours=12)

    # Reduction de toutes les colonnes incluant P0 et PT
    simple_df = fstpy.reduce_columns(simple_df)

    assert(simple_df.npas.unique()[0]   == 144)        # Correspond a forecast_hour * 3600 / deet
    assert(simple_df.dateo.unique()[0]  == 466640030)  # Correspond a 2023071400020000
    assert(simple_df.datev.unique()[0]  == 466650830)  # Correspond a 2023071412020000
    assert(simple_df.ip2.unique()[0]    == 12)         # Correspond a 12 H encode 

    assert(len(simple_df.columns) == 24)

def test_30(simple_df):
    """Test function reduce_columns, deet = 0 """

    source    = TEST_PATH + 'MinMaxLevelIndex/testsFiles/test_ICGA_file2cmp_20201202.std'
    simple_df = fstpy.StandardFileReader(source).to_pandas()
    simple_df = simple_df.loc[(simple_df.nomvar == 'IND')]
    simple_df.npas = 96

    # Ajout de toutes les colonnes
    simple_df = fstpy.add_columns(simple_df, ['dateo', 'datev', 'forecast_hour', 'ip_info'])

    # Partie forecast_hour
    simple_df.forecast_hour = pd.Timedelta(hours=6)

    # Dataframe ressemble a ceci, avec informations incoherentes: 
    # forecast_hour = 0 days 6:00:00 , deet = 0, npas = 96
    # level = 850, ip3_dec 500, ip1 = 41744464, ip3 = 41394464 

    # Reduction de toutes les colonnes
    simple_df = fstpy.reduce_columns(simple_df)

    assert(simple_df.ip1[0]    == 41744464)   # Correspond a 850 mb encode
    assert(simple_df.ip3[0]    == 41394464)   # Correspond a 200 mb encode
    assert(simple_df.ip2[0]    == 177809344)  # Correspond a 6 H encode 
    assert(simple_df.npas[0]   == 96)         # Npas non modifie car deet == 0

    assert(len(simple_df.columns) == 22)
