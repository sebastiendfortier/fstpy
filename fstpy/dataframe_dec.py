# -*- coding: utf-8 -*-
import pandas as pd


def create_grid_column(nomvar_col:pd.Series,ip1_col:pd.Series,ip2_col:pd.Series,ig1_col:pd.Series,ig2_col:pd.Series):
    from .std_dec import create_grid_identifier
    grid = nomvar_col.copy(deep=True)
    for i in nomvar_col.index:
        grid[i] = create_grid_identifier(nomvar_col[i],ip1_col[i],ip2_col[i],ig1_col[i],ig2_col[i])        
    return grid

def create_decoded_etiket_columns(etiket_col:pd.Series):
    from .std_dec import parse_etiket
    label = etiket_col.copy(deep=True)
    run = etiket_col.copy(deep=True)
    implementation = etiket_col.copy(deep=True)
    ensemble_member = etiket_col.copy(deep=True)
    for i in etiket_col.index:
        label[i], run[i], implementation[i], ensemble_member[i] = parse_etiket(etiket_col[i])
    return label, run, implementation, ensemble_member

def get_unit_and_description_columns(nomvar_col:pd.Series):
    from .std_dec import get_unit_and_description
    unit = nomvar_col.copy(deep=True)
    description = nomvar_col.copy(deep=True)
    for i in nomvar_col.index:
        unit[i], description[i] = get_unit_and_description(nomvar_col[i])
    return unit, description


def create_decoded_dateo_column(dateo_col:pd.Series):
    from .std_dec import convert_rmndate_to_datetime
    date_of_observation = dateo_col.copy(deep=True)
    for i in dateo_col.index:
        date_of_observation[i] = convert_rmndate_to_datetime(int(dateo_col[i]))
    return date_of_observation

def create_decoded_datev_column(datev_col:pd.Series):
    from .std_dec import convert_rmndate_to_datetime
    date_of_validity = datev_col.copy(deep=True)
    for i in datev_col.index:
        date_of_validity[i] = convert_rmndate_to_datetime(int(datev_col[i]))
    return date_of_validity
    
def create_decoded_deet_npas_column(deet_col:pd.Series,npas_col:pd.Series):
    import datetime
    forecast_hour = deet_col.copy(deep=True)
    for i in deet_col.index:
        forecast_hour[i] = datetime.timedelta(seconds=(npas_col[i] * deet_col[i]))
    return forecast_hour

def create_decoded_ips_columns(nomvar_col:pd.Series,ip1_col:pd.Series,ip2_col:pd.Series,ip3_col:pd.Series):
    from .std_dec import decode_ips
    level = ip1_col.copy(deep=True)
    ip1_kind = ip1_col.copy(deep=True)
    ip1_pkind = nomvar_col.copy(deep=True)
    ip2_dec = ip1_col.copy(deep=True)
    ip2_kind = ip1_col.copy(deep=True)
    ip2_pkind = nomvar_col.copy(deep=True)
    ip3_dec = ip1_col.copy(deep=True)
    ip3_kind = ip1_col.copy(deep=True)
    ip3_pkind = nomvar_col.copy(deep=True)
    for i in nomvar_col.index:
        level[i],ip1_kind[i],ip1_pkind[i],ip2_dec[i],ip2_kind[i],ip2_pkind[i],ip3_dec[i],ip3_kind[i],ip3_pkind[i] = decode_ips(nomvar_col[i],ip1_col[i],ip2_col[i],ip3_col[i])
    return level,ip1_kind,ip1_pkind,ip2_dec,ip2_kind,ip2_pkind,ip3_dec,ip3_kind,ip3_pkind

def create_decoded_datyp_column(datyp_col:pd.Series):
    from .constants import DATYP_DICT
    data_type_str = datyp_col.copy(deep=True)
    for i in datyp_col.index:
        data_type_str[i] = DATYP_DICT[datyp_col[i]]
    return data_type_str

def create_surface_column(ip1_kind_col:pd.Series,level_col:pd.Series):    
    from .std_dec import is_surface
    surface = ip1_kind_col.copy(deep=True)
    for i in ip1_kind_col.index:
        surface[i] = is_surface(ip1_kind_col[i],level_col[i])

def create_follow_topography_column(ip1_kind_col:pd.Series):    
    from .std_dec import level_type_follows_topography
    follow_topography = ip1_kind_col.copy(deep=True)
    for i in ip1_kind_col.index:
        follow_topography[i] = level_type_follows_topography(ip1_kind_col[i])
    return follow_topography
