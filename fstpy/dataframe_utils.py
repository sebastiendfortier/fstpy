# -*- coding: utf-8 -*-
import logging
import math

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from fstpy import DATYP_DICT
from .std_reader import compute

from .dataframe import add_columns, add_ip_info_columns, reorder_columns
from .std_dec import convert_rmndate_to_datetime


class SelectError(Exception):
    pass


def select_meta(df: pd.DataFrame) -> pd.DataFrame:
    meta_df = df.loc[df.nomvar.isin(
        ["!!", "P0", "PT", ">>", "^^", "^>", "HY", "!!SF"])]
    return meta_df


def select_with_meta(df: pd.DataFrame, nomvar: list) -> pd.DataFrame:
    """Select fields with accompaning meta data  

    :param df: dataframe to select from  
    :type df: pd.DataFrame  
    :param nomvar: list of nomvars to select   
    :type nomvar: list  
    :raises SelectError: if dataframe is empty, if nothing to select or if variable not found in dataframe  
    :return: dataframe with selection results  
    :rtype: pd.DataFrame  
    """
    if df.empty:
        raise SelectError(f'dataframe is empty - nothing to select into')

    results = []

    if len(nomvar) == 0:
        raise SelectError(f'nomvar is empty - nothing to select')

    for var in nomvar:
        res_df = df.loc[df.nomvar == var]
        if res_df.empty:
            raise SelectError(f'missing {var} in dataframe')
        results.append(res_df)

    meta_df = select_meta(df)

    if not meta_df.empty:
        results.append(meta_df)

    selection_result_df = pd.concat(results, ignore_index=True)

    selection_result_df = metadata_cleanup(selection_result_df)

    return selection_result_df


def metadata_cleanup(df: pd.DataFrame, strict_toctoc=True) -> pd.DataFrame:
    """Cleans the metadata from a dataframe according to rules.   

    :param df: dataframe to clean  
    :type df: pd.DataFrame  
    :return: dataframe with only cleaned meta_data  
    :rtype: pd.DataFrame  
    """

    if df.empty:
        return df

    df = add_columns(df,['ip_info'])
    no_meta_df = df.loc[~df.nomvar.isin(["!!", "P0", "PT", ">>", "^^", "^>", "HY", "!!SF"])]

    # get deformation fields
    grid_deformation_fields_df = get_grid_deformation_fileds(df, no_meta_df)

    sigma_ips = get_sigma_ips(no_meta_df)

    hybrid_ips = get_hybrid_ips(no_meta_df)

    # get P0's
    p0_fields_df = get_p0_fields(df, no_meta_df, hybrid_ips, sigma_ips)

    # get PT's
    pt_fields_df = get_pt_fields(df, no_meta_df, sigma_ips)

    # get HY
    hy_field_df = get_hy_field(df, hybrid_ips)

    pressure_ips = get_pressure_ips(no_meta_df)

    # get !!'s strict
    toctoc_fields_df = get_toctoc_fields(df, no_meta_df, hybrid_ips, sigma_ips, pressure_ips, strict_toctoc)

    df = pd.concat([no_meta_df, grid_deformation_fields_df, p0_fields_df,
                    pt_fields_df, hy_field_df, toctoc_fields_df], ignore_index=True)

    return df


class VoirError(Exception):
    pass


def voir(df: pd.DataFrame, style=False):
    """Displays the metadata of the supplied records in the rpn voir format"""
    if df.empty:
        raise VoirError('No records to process')

    to_print_df = df.copy()
    to_print_df['datyp'] = to_print_df['datyp'].map(DATYP_DICT)
    to_print_df['datev'] = to_print_df['datev'].apply(
        convert_rmndate_to_datetime)
    to_print_df['dateo'] = to_print_df['dateo'].apply(
        convert_rmndate_to_datetime)
    to_print_df = add_ip_info_columns(to_print_df)

    res_df = to_print_df.sort_values(
        by=['nomvar', 'level'], ascending=[True, False])

    if style:
        res_df = res_df.drop(columns=['dateo', 'grid', 'run', 'implementation', 'ensemble_member', 'shape', 'key', 'd', 'path', 'file_modification_time', 'ip1_kind', 'ip2_dec', 'ip2_kind', 'ip2_pkind',
                                      'ip3_dec', 'ip3_kind', 'ip3_pkind', 'date_of_observation', 'date_of_validity', 'forecast_hour', 'd', 'surface', 'follow_topography', 'ascending', 'interval'], errors='ignore')
        res_df = reorder_columns(res_df, ordered=['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'datev', 'level',
                                                  ' ', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'])
    else:
        res_df = res_df.drop(columns=['datev', 'grid', 'run', 'implementation', 'ensemble_member', 'shape', 'key', 'd', 'path', 'file_modification_time', 'ip1_kind', 'ip2_dec', 'ip2_kind', 'ip2_pkind',
                                      'ip3_dec', 'ip3_kind', 'ip3_pkind', 'date_of_observation', 'date_of_validity', 'forecast_hour', 'd', 'surface', 'follow_topography', 'ascending', 'interval'], errors='ignore')

    logging.info('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    logging.info('\n%s' % res_df.reset_index(drop=True).to_string(header=True))


class FstStatError(Exception):
    pass


def fststat(df: pd.DataFrame) -> pd.DataFrame:
    """Produces summary statistics for a dataframe

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: output dataframe with added 'mean','std','min_pos','min','max_pos','max' columns
    :rtype: pd.DataFrame
    """

    logging.info('fststat')
    pd.options.display.float_format = '{:0.6E}'.format
    if df.empty:
        raise FstStatError('fststat - no records to process')
    df = compute_stats(df)
    df = add_ip_info_columns(df)

    df.sort_values(by=['nomvar', 'level'], ascending=[
                   True, False], inplace=True)

    logging.info('\n%s' % df[['nomvar', 'typvar', 'level', 'ip1', 'ip2', 'ip3', 'dateo', 'etiket', 'mean',
                              'std', 'min_pos', 'min', 'max_pos', 'max']].to_string(formatters={'level': '{:,.6f}'.format}))

    return df


def compute_stats(df: pd.DataFrame) -> pd.DataFrame:
    df['min'] = None
    df['max'] = None
    df['mean'] = None
    df['std'] = None
    shape_list = [(0, 0) for _ in range(len(df.index))]
    df['min_pos'] = shape_list
    df['max_pos'] = shape_list

    df = compute(df)
    for i in df.index:
        df.at[i, 'mean'] = np.mean(df.at[i, 'd'])
        df.at[i, 'std'] = np.std(df.at[i, 'd'])
        df.at[i, 'min'] = np.min(df.at[i, 'd'])
        df.at[i, 'max'] = np.max(df.at[i, 'd'])
        min_pos = np.unravel_index(
            np.argmin(df.at[i, 'd']), (df.at[i, 'ni'], df.at[i, 'nj']))
        df.at[i, 'min_pos'] = (min_pos[0] + 1, min_pos[1]+1)
        max_pos = np.unravel_index(
            np.argmax(df.at[i, 'd']), (df.at[i, 'ni'], df.at[i, 'nj']))
        df.at[i, 'max_pos'] = (max_pos[0] + 1, max_pos[1]+1)

    return df


def get_kinds_and_ip1(df: pd.DataFrame) -> dict:
    ip1s = df.ip1.unique()
    kinds = {}
    for ip1 in ip1s:
        if math.isnan(ip1):
            continue
        (_, kind) = rmn.convertIp(rmn.CONVIP_DECODE, int(ip1))
        if kind not in kinds.keys():
            kinds[kind] = []
        kinds[kind].append(ip1)

    return kinds


def get_ips(df: pd.DataFrame, sigma=False, hybrid=False, pressure=False) -> list:
    kinds = get_kinds_and_ip1(df)

    ip1_list = []
    if sigma:
        if 1 in kinds.keys():
            ip1_list.extend(kinds[1])
    if hybrid:
        if 5 in kinds.keys():
            ip1_list.extend(kinds[5])
    if pressure:
        if 2 in kinds.keys():
            ip1_list.extend(kinds[2])
    return ip1_list


def get_model_ips(df: pd.DataFrame) -> list:
    return get_ips(df, sigma=True, hybrid=True)


def get_sigma_ips(df: pd.DataFrame) -> list:
    return get_ips(df, sigma=True)


def get_pressure_ips(df: pd.DataFrame) -> list:
    return get_ips(df, pressure=True)


def get_hybrid_ips(df: pd.DataFrame) -> list:
    return get_ips(df, hybrid=True)


def get_toctoc_fields(df: pd.DataFrame, no_meta_df:pd.DataFrame, hybrid_ips: list, sigma_ips: list, pressure_ips: list, strict=True):

    toctoc_df = df.loc[df.nomvar=='!!']
    
    df_list = []

    hybrid_fields_df = pd.DataFrame(dtype=object)
    # hybrid
    if len(hybrid_ips):
        hybrid_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(hybrid_ips)]

    hybrid_grids = []
    if not hybrid_fields_df.empty:
        hybrid_grids = hybrid_fields_df.grid.unique()

    # sigma
    sigma_fields_df = pd.DataFrame(dtype=object)
    if len(sigma_ips):
        sigma_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(sigma_ips)]

    sigma_grids = []
    if not sigma_fields_df.empty:
        sigma_grids = sigma_fields_df.grid.unique()

    # pressure
    pressure_fields_df = pd.DataFrame(dtype=object)
    if len(pressure_ips):
        pressure_fields_df = no_meta_df.loc[no_meta_df.ip1.isin(pressure_ips)]

    pressure_grids = []
    if not pressure_fields_df.empty:
        pressure_grids = pressure_fields_df.grid.unique()

    for grid in hybrid_grids:
        # grids_no_meta_df = no_meta_df.loc[no_meta_df.grid == grid]
        # vctypes = list(grids_no_meta_df.vctype.unique())
        hyb_toctoc_df = toctoc_df.loc[(toctoc_df.grid == grid) & (
            toctoc_df.ig1.isin([1003, 5001, 5002, 5003, 5004, 5005, 5100, 5999, 21001, 21002]))]
        # vctypes = list(hyb_toctoc_df.ig1.unique())
        # vctypes = numeric_vctype_to_string(vctypes)
        if not hyb_toctoc_df.empty:
            df_list.append(hyb_toctoc_df)

    # vcode 1001 -> Sigma levels
    # vcode 1002 -> Eta levels
    for grid in sigma_grids:
        # grids_no_meta_df = no_meta_df.loc[no_meta_df.grid == grid]
        sigma_toctoc_df = toctoc_df.loc[(toctoc_df.grid == grid) & (toctoc_df.ig1.isin([1001, 1002]))]
        # vctypes = list(sigma_toctoc_df.ig1.unique())
        # vctypes = numeric_vctype_to_string(vctypes)
        if not sigma_toctoc_df.empty:
            df_list.append(sigma_toctoc_df)

    # vcode 2001 -> Pressure levels
    for grid in pressure_grids:
        presure_toctoc_df = toctoc_df.loc[(df.grid == grid) & (df.ig1 == 2001)]
        if not presure_toctoc_df.empty:
            df_list.append(presure_toctoc_df)

    toctoc_fields_df = pd.DataFrame(dtype=object)

    if len(df_list):
        toctoc_fields_df = pd.concat(df_list, ignore_index=True)

    toctoc_fields_df = toctoc_fields_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk', 'ip1',
                                                                'ip2', 'ip3', 'deet', 'npas', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], ignore_index=True)

    return toctoc_fields_df

def numeric_vctype_to_string(vctypes):
    vctype_list = []
    for vctype in vctypes:
        if vctype == 5002:
            vctype_list.append('HYBRID_STAGGERED')
        elif vctype == 5001:
            vctype_list.append('HYBRID')
        elif vctype == 5005:
            vctype_list.append('HYBRID_5005')
        elif vctype == 2001:
            vctype_list.append('PRESSURE')
        elif vctype == 1002:
            vctype_list.append('ETA')
        elif vctype == 1001:
            vctype_list.append('SIGMA')
        else:
            vctype_list.append('UNKNOWN')
    return vctype_list        


def get_hy_field(df: pd.DataFrame, hybrid_ips: list):

    hy_field_df = pd.DataFrame(dtype=object)
    if len(hybrid_ips):
        hy_field_df = df.loc[df.nomvar == "HY"]

    hy_field_df = hy_field_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk', 'ip1', 'ip2',
                                                      'ip3', 'deet', 'npas', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], ignore_index=True)

    return hy_field_df


def get_grid_deformation_fileds(df: pd.DataFrame, no_meta_df: pd.DataFrame):
    grid_deformation_fields_df = pd.DataFrame(dtype=object)

    groups = no_meta_df.groupby(['grid', 'dateo', 'deet', 'npas'])

    df_list = []
    hasDate = False
    for _, group in groups:
        grid = group.grid.unique()[0]
        dateo = group.dateo.unique()[0]
        if len(list(group.ni.unique())) > 1:
            logging.error(f'grid with fields of different sizes for ni {group.ni.unique()}')
        if len(list(group.nj.unique())) > 1:
            logging.error(f'grid with fields of different sizes for nj {group.nj.unique()}')    
        ni = group.ni.unique()[0]
        nj = group.nj.unique()[0]
        lat_df = df.loc[(df.nomvar == "^^") & (df.grid == grid) & (df.dateo == dateo) & (df.nj == nj)]
        if lat_df.empty:
            lat_df = df.loc[(df.nomvar == "^^") & (df.grid == grid) & (df.nj == nj)]

        lon_df = df.loc[(df.nomvar == ">>") & (df.grid == grid) & (df.dateo == dateo) & (df.ni == ni)]
        if lon_df.empty:
            lon_df = df.loc[(df.nomvar == ">>") & (df.grid == grid) & (df.ni == ni)]

        tictac_df = df.loc[(df.nomvar == "^>") & (df.grid == grid) & (df.dateo == dateo)]
        if tictac_df.empty:
            tictac_df = df.loc[(df.nomvar == "^>") & (df.grid == grid)]

        if not lat_df.empty:
            hasDate = ((lat_df.deet.unique()[0] != 0))
        if not lon_df.empty:
            hasDate = ((lon_df.deet.unique()[0] != 0))
        if not tictac_df.empty:
            hasDate = ((tictac_df.deet.unique()[0] != 0))

        if hasDate:
            deet = group.deet.unique()[0]
            npas = group.npas.unique()[0]
            df_list.append(df.loc[(df.nomvar == ">>") & (
                df.grid == grid) & (df.deet == deet) & (df.npas == npas)])
            df_list.append(df.loc[(df.nomvar == "^^") & (
                df.grid == grid) & (df.deet == deet) & (df.npas == npas)])
            df_list.append(df.loc[(df.nomvar == "^>") & (
                df.grid == grid) & (df.deet == deet) & (df.npas == npas)])
        else:
            df_list.append(lat_df)
            df_list.append(lon_df)
            df_list.append(tictac_df)

    if len(df_list):
        grid_deformation_fields_df = pd.concat(df_list, ignore_index=True)
    if hasDate:
        grid_deformation_fields_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3',
                                                           'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datyp', 'deet', 'npas', 'dateo', 'datev'], inplace=True, ignore_index=True)
    else:
        grid_deformation_fields_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk',
                                                           'ip1', 'ip2', 'ip3', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datyp'], inplace=True, ignore_index=True)
    return grid_deformation_fields_df


def get_p0_fields(df: pd.DataFrame, no_meta_df: pd.DataFrame, hybrid_ips: list, sigma_ips: list):

    p0_df = df.loc[df.nomvar=='P0']

    p0_fields_df = pd.DataFrame(dtype=object)

    hybrid_grids = set()
    for ip1 in hybrid_ips:
        hybrid_grids.add(no_meta_df.loc[no_meta_df.ip1 == ip1].iloc[0]['grid'])

   
    df_list = []
    for grid in hybrid_grids:
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]
        df_list.append(p0_df.loc[(p0_df.grid == grid) & (p0_df.ni == ni) & (p0_df.nj == nj)])

    if len(df_list):
        p0_fields_df = pd.concat(df_list, ignore_index=True)


    sigma_grids = set()
    for ip1 in sigma_ips:
        sigma_grids.add(no_meta_df.loc[no_meta_df.ip1 == ip1].iloc[0]['grid'])

    df_list = []
    for grid in sigma_grids:
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]
        df_list.append(p0_df.loc[(p0_df.grid == grid) & (p0_df.ni == ni) & (p0_df.nj == nj)])

    if len(df_list):
        p0_fields_df = pd.concat(df_list, ignore_index=True)

    p0_fields_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet',
                                         'npas', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], inplace=True, ignore_index=True)

    return p0_fields_df


def get_pt_fields(df: pd.DataFrame, no_meta_df: pd.DataFrame, sigma_ips: list):
    pt_df = df.loc[df.nomvar=='PT']
    
    pt_fields_df = pd.DataFrame(dtype=object)
    
    sigma_grids = set()
    for ip1 in sigma_ips:
        sigma_grids.add(no_meta_df.loc[no_meta_df.ip1 == ip1].iloc[0]['grid'])

    df_list = []
    for grid in list(sigma_grids):
        ni = no_meta_df.loc[no_meta_df.grid == grid].ni.unique()[0]
        nj = no_meta_df.loc[no_meta_df.grid == grid].nj.unique()[0]
        df_list.append(pt_df.loc[(pt_df.grid == grid) & (pt_df.ni == ni) & (pt_df.nj == nj)])

    if len(df_list):
        pt_fields_df = pd.concat(df_list, ignore_index=True)

    pt_fields_df.drop_duplicates(subset=['grtyp', 'nomvar', 'typvar', 'ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet',
                                         'npas', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], inplace=True, ignore_index=True)

    return pt_fields_df
