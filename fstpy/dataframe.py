# -*- coding: utf-8 -*-
import copy
import logging

import dask.array as da
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from .std_dec import (convert_rmndate_to_datetime, get_grid_identifier,
                      get_parsed_etiket, get_unit_and_description)
from .std_io import remove_column


def add_grid_column(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the grid column to the dataframe. The grid column is a simple identifier composed of ip1+ip2 or ig1+ig2 depending on the type of record (>>,^^,^>) vs regular field. 

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: modified dataframe with the 'grid' column added
    :rtype: pd.DataFrame
    """
    vcreate_grid_identifier = np.vectorize(get_grid_identifier, otypes=['str'])
    df['grid'] = vcreate_grid_identifier(df['nomvar'].values, df['ip1'].values, df['ip2'].values, df['ig1'].values, df['ig2'].values)
    return df
    
def get_path_and_key_from_array(darr:'da.core.Array'):
    if not isinstance(darr,da.core.Array):
        return None, None
    graph = darr.__dask_graph__()
    graph_list = list(graph.to_dict())
    path_and_key = graph_list[0][0]
    if ':' in path_and_key:
        path_and_key = path_and_key.split(':')
        return path_and_key[0], path_and_key[1]
    else:
        return None, None
        
def add_path_and_key_columns(df: pd.DataFrame):
    """adds the path and key columns to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: path and key for each row
    :rtype: pd.DataFrame
    """
    df['path'] = None
    df['key'] = None
    vparse_task_list = np.vectorize(get_path_and_key_from_array, otypes=['object','object'])
    df['path'], df['key'] = vparse_task_list(df['d'].values)
    return df

    
# get modifier information from the second character of typvar
def parse_typvar(typvar: str):
    multiple_modifications = False
    zapped = False
    filtered = False
    interpolated = False
    unit_converted = False
    bounded = False
    missing_data = False
    ensemble_extra_info = False
    if len(typvar) == 2:
        typvar2 = typvar[1]
        if (typvar2 == 'M'):
            # Il n'y a pas de façon de savoir quelle modif a ete faite
            multiple_modifications = True
        elif (typvar2 == 'Z'):
            zapped = True
        elif (typvar2 == 'F'):
            filtered = True
        elif (typvar2 == 'I'):
            interpolated = True
        elif (typvar2 == 'U'):
            unit_converted = True
        elif (typvar2 == 'B'):
            bounded = True
        elif (typvar2 == '?'):
            missing_data = True
        elif (typvar2 == '!'):
            ensemble_extra_info = True
    return multiple_modifications, zapped, filtered, interpolated, unit_converted, bounded, missing_data, ensemble_extra_info

  
def add_flag_values(df: pd.DataFrame):
    """adds the correct flag values derived from parsing the typvar

    :param df: dataframe
    :type df: pd.DataFrame
    :return: flag values set according to second character of typvar if present
    :rtype: pd.DataFrame
    """
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vparse_typvar = np.vectorize(parse_typvar, otypes=['bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool', 'bool'])
    df['multiple_modifications'], df['zapped'], df['filtered'], df['interpolated'], df['unit_converted'], df['bounded'], df['missing_data'], df['ensemble_extra_info'] = vparse_typvar(df['typvar'].values)
    return df



def drop_duplicates(df: pd.DataFrame):
    """Removes duplicate rows from dataframe

    :param df: original dataframe
    :type df: pd.DataFrame
    :return: dataframe without duplicate rows
    :rtype: pd.DataFrame
    """
    init_row_count = len(df.index)
    columns = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo',
               'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits',
               'grtyp', 'ig1', 'ig3', 'ig4', 'datev']

    df.drop_duplicates(subset=columns, keep='first',inplace=True)

    row_count = len(df.index)
    if init_row_count != row_count:
        logging.warning('Found duplicate rows in dataframe!')
    
    return df    



def add_shape_column(df):
    df['shape'] = pd.Series(zip(df.ni.to_numpy(),df.nj.to_numpy()),dtype='object').to_numpy()
    return df


def add_parsed_etiket_columns(df: pd.DataFrame) -> pd.DataFrame:
    """adds label,run,implementation and ensemble_member columns from the parsed 
       etikets to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns 
             added
    :rtype: pd.DataFrame
    """
    # df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = get_parsed_etiket(df.at[i,'etiket'])
    vparse_etiket = np.vectorize(get_parsed_etiket, otypes=['str', 'str', 'str', 'str'])
    df['label'], df['run'], df['implementation'], df['ensemble_member'] = vparse_etiket(df['etiket'].values)
    return df


def add_unit_and_description_columns(df: pd.DataFrame):
    """adds unit and description from the nomvars to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit and description columns added
    :rtype: pd.DataFrame
    """
    df.reset_index(drop=True,inplace=True)
    vget_unit_and_description = np.vectorize(get_unit_and_description, otypes=['str', 'str'])
    if 'unit' in df.columns:
        to_modify_df = copy.deepcopy(df.loc[df.unit.isna()])
        remove_column(to_modify_df,'unit')
        remove_column(to_modify_df,'description')
        to_modify_df['unit'], to_modify_df['description'] = vget_unit_and_description(to_modify_df['nomvar'].values)
        others_df = copy.deepcopy(df.loc[~df.unit.isna()])
        df = pd.concat([to_modify_df,others_df]).sort_index()
    else:    
        df['unit'], df['description'] = vget_unit_and_description(df['nomvar'].values)
    return df


def add_decoded_date_column(df: pd.DataFrame, attr: str = 'dateo'):
    """adds the decoded dateo or datev column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :param attr: selected date to decode, defaults to 'dateo'
    :type attr: str, optional
    :return: either date_of_observation or date_of_validity column added to the 
             dataframe
    :rtype: pd.DataFrame
    """
    vconvert_rmndate_to_datetime = np.vectorize(convert_rmndate_to_datetime)  # ,otypes=['datetime64']
    if attr == 'dateo':
        df['date_of_observation'] = vconvert_rmndate_to_datetime(df['dateo'].values)
    else:
        df['date_of_validity'] = vconvert_rmndate_to_datetime(df['datev'].values)
    return df    



def add_forecast_hour_column(df: pd.DataFrame):
    """adds the forecast_hour column derived from the deet and npas columns

    :param df: dataframe
    :type df: pd.DataFrame
    :return: forecast_hour column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_forecast_hour

    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vcreate_forecast_hour = np.vectorize(get_forecast_hour)  # ,otypes=['timedelta64[ns]']
    df['forecast_hour'] = vcreate_forecast_hour(df['deet'].values, df['npas'].values)
    return df
    


def add_data_type_str_column(df: pd.DataFrame) -> pd.DataFrame:
    """adds the data type decoded to string value column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: data_type_str column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_data_type_str
    vcreate_data_type_str = np.vectorize(get_data_type_str, otypes=['str'])
    df['data_type_str'] = vcreate_data_type_str(df['datyp'].values)
    return df
    


def add_ip_info_columns(df: pd.DataFrame):
    """adds all relevant level info from the ip1 column values

    :param df: dataframe
    :type df: pd.DataFrame
    :return: level, ip1_kind, ip1_pkind,surface and follow_topography columns 
             added to the dataframe.
    :rtype: pd.DataFrame
    """
    from .std_dec import get_ip_info
    new_df = copy.deepcopy(df)
    vcreate_ip_info = np.vectorize(get_ip_info, otypes=[
                                   'float32', 'int32', 'str', 'float32', 'int32', 'str', 'float32', 'int32', 'str', 'bool', 'bool', 'bool', 'object'])
    new_df['level'], new_df['ip1_kind'], new_df['ip1_pkind'], new_df['ip2_dec'], new_df['ip2_kind'], new_df['ip2_pkind'], new_df['ip3_dec'], new_df['ip3_kind'], new_df['ip3_pkind'], new_df['surface'], new_df['follow_topography'], new_df['ascending'], new_df['interval'] = vcreate_ip_info(new_df['ip1'].values, new_df['ip2'].values, new_df['ip3'].values)
    return new_df



def add_columns(df: pd.DataFrame, columns: 'str|list[str]' = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info']):
    """If valid columns are provided, they will be added. 
       These include ['flags','etiket','unit','dateo','datev','forecast_hour',
       'datyp','ip_info']

    :param df: dataframe to modify (meta data needs to be present in dataframe)
    :type df: pd.DataFrame
    :param decode: if decode is True, add the specified columns
    :type decode: bool
    :param columns: [description], defaults to 
                    ['flags','etiket','unit','dateo','datev','forecast_hour',
                    'datyp','ip_info']
    :type columns: list[str], optional
    """
    cols = ['flags', 'etiket', 'unit', 'dateo', 'datev', 'forecast_hour', 'datyp', 'ip_info']
    if isinstance(columns,str):
        columns = [columns]
    
    for col in columns:
        if col not in cols:
            logging.warning(f'{col} not found in {cols}')

    if 'etiket' in columns:
        df = add_parsed_etiket_columns(df)

    if 'unit' in columns:
        df = add_unit_and_description_columns(df)

    if 'dateo' in columns:
        df = add_decoded_date_column(df, 'dateo')

    if 'datev' in columns:
        df = add_decoded_date_column(df, 'datev')

    if 'forecast_hour' in columns:
        df = add_forecast_hour_column(df)

    if 'datyp' in columns:
        df = add_data_type_str_column(df)

    if ('ip_info' in columns):
        df = add_ip_info_columns(df)
        df = set_vertical_coordinate_type(df)

    if 'flags' in columns:
        df = add_flag_values(df)

    return df    

    


def reorder_columns(df):
    ordered = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
               'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    if df.empty:
        return 
    all_columns = set(df.columns)
    # all_columns = set(df.columns.to_list())

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns))

    df = df[ordered]



def set_vertical_coordinate_type(df):
    from fstpy import VCTYPES
    if 'level' not in df.columns:
        df = add_ip_info_columns(df)
    newdfs = []
    df.loc[:,'vctype'] = 'UNKNOWN'
    grid_groups = df.groupby(df.grid)

    for _, grid in grid_groups:
        toctoc, p0, e1, pt, hy, sf, vcode = get_meta_fields_exists(grid)
        this_vcode = vcode[0]
        ip1_kind_groups = grid.groupby(grid.ip1_kind)
        for _, ip1_kind_group in ip1_kind_groups:
            # these ip1_kinds are not defined
            without_meta = ip1_kind_group.loc[(~ip1_kind_group.ip1_kind.isin(
                [-1, 3, 6])) & (~ip1_kind_group.nomvar.isin(["!!", "HY", "P0", "PT", ">>", "^^", "PN"]))]
            if not without_meta.empty:
                ip1_kind = without_meta.iloc[0]['ip1_kind']
                # print(vcode)
                if len(vcode) > 1:
                    for vc in vcode:
                        d, _ = divmod(vc, 1000)
                        if ip1_kind == d:
                            this_vcode = vc
                            continue

                ip1_kind_group['vctype'] = 'UNKNOWN'
                #vctype_dict = {'ip1_kind':ip1_kind,'toctoc':toctoc,'P0':p0,'E1':e1,'PT':pt,'HY':hy,'SF':sf,'vcode':vcode}
                # print(VCTYPES)
                # print(VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(5,False,True,False,False,False,False,-1)))
                # print('\n(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                # vctyte_df = VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                vctyte_df = VCTYPES.loc[(VCTYPES.ip1_kind == ip1_kind) & (VCTYPES.toctoc == toctoc) & (VCTYPES.P0 == p0) & (
                    VCTYPES.E1 == e1) & (VCTYPES.PT == pt) & (VCTYPES.HY == hy) & (VCTYPES.SF == sf) & (VCTYPES.vcode == this_vcode)]
                # print(vctyte_df)
                if not vctyte_df.empty:
                    if len(vctyte_df.index) > 1:
                        logging.warning('set_vertical_coordinate_type - more than one match!!!')
                    ip1_kind_group['vctype'] = vctyte_df.iloc[0]['vctype']
            newdfs.append(ip1_kind_group)

    df = pd.concat(newdfs, ignore_index=True)

    df.loc[df.nomvar.isin([">>", "^^", "!!", "P0", "PT","HY", "PN", "!!SF"]), "vctype"] = "UNKNOWN"
    
    return df



def get_meta_fields_exists(grid):
    toctoc = grid.loc[grid.nomvar == "!!"]
    vcode = []
    if not toctoc.empty:
        for row in toctoc.itertuples():
            vcode.append(row.ig1)
        toctoc = True
    else:
        vcode.append(-1)
        toctoc = False
    p0 = meta_exists(grid, "P0")
    e1 = meta_exists(grid, "E1")
    pt = meta_exists(grid, "PT")
    hy = meta_exists(grid, "HY")
    sf = meta_exists(grid, "!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode


def meta_exists(grid, nomvar) -> bool:
    df = grid.loc[grid.nomvar == nomvar]
    return not df.empty

def create_empty_dataframe(num_rows):
    record = {
        'nomvar': ' ',
        'typvar': 'P',
        'etiket': ' ',
        'ni': 1,
        'nj': 1,
        'nk': 1,
        'dateo': 0,
        'ip1': 0,
        'ip2': 0,
        'ip3': 0,
        'deet': 0,
        'npas': 0,
        'datyp': 133,
        'nbits': 16,
        'grtyp': 'G',
        'ig1': 0,
        'ig2': 0,
        'ig3': 0,
        'ig4': 0,
        'datev': 0,
        'd':None
        }
    df =  pd.DataFrame([record for _ in range(num_rows)])
    return df

