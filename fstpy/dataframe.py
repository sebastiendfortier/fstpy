# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import logging
from .std_dec import (convert_rmndate_to_datetime,
                      get_parsed_etiket, get_unit_and_description)



# get modifier information from the second character of typvar
def parse_typvar(typvar:str):
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
    return multiple_modifications,zapped,filtered,interpolated,unit_converted,bounded,missing_data,ensemble_extra_info

def add_flag_values(df:pd.DataFrame) -> pd.DataFrame:
    """adds the correct flag values derived from parsing the typvar

    :param df:dataframe
    :type df: pd.DataFrame
    :return: flag values set according to second character of typvar if present
    :rtype: pd.DataFrame
    """
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vparse_typvar = np.vectorize(parse_typvar,otypes=['bool','bool','bool','bool','bool','bool','bool','bool'])
    df.loc[:,'multiple_modifications'],df.loc[:,'zapped'],df.loc[:,'filtered'],df.loc[:,'interpolated'],df.loc[:,'unit_converted'],df.loc[:,'bounded'],df.loc[:,'missing_data'],df.loc[:,'ensemble_extra_info'] = vparse_typvar(df['typvar'].values)
    return df


def drop_duplicates(df:pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows from dataframe
        
    :param df: original dataframe
    :type df: pd.DataFrame
    :return: dataframe without duplicate rows
    :rtype: pd.DataFrame
    """
    init_row_count = len(df.index)
    columns = ['nomvar','typvar','etiket','ni','nj','nk','dateo',
        'ip1','ip2','ip3','deet','npas','datyp','nbits',
        'grtyp','ig1','ig3','ig4','datev','key']

    df = df.drop_duplicates(subset=columns,keep='first')

    row_count = len(df.index)
    if init_row_count != row_count:
        logging.warning('Found duplicate rows in dataframe!')
        
    return df

def get_shape(ni,nj):
    return (ni,nj)

def add_shape_column(df):
    if 'shape' in df.columns:
        return df
    vmake_shape = np.vectorize(get_shape,otypes=['object'])
    df.loc[:,'shape'] = vmake_shape(df['ni'].values,df['nj'].values)
    return df


def add_data_column(df):
    if 'd' in df.columns:
        return df
    df.loc[:,'d']=None
    return df


def add_parsed_etiket_columns(df:pd.DataFrame) ->pd.DataFrame:
    """adds label,run,implementation and ensemble_member columns from the parsed 
       etikets to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns 
             added
    :rtype: pd.DataFrame
    """
    # df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = get_parsed_etiket(df.at[i,'etiket'])
    vparse_etiket = np.vectorize(get_parsed_etiket,otypes=['str','str','str','str'])
    df.loc[:,'label'],df.loc[:,'run'],df.loc[:,'implementation'],df.loc[:,'ensemble_member'] = vparse_etiket(df['etiket'].values)
    return df

def add_unit_and_description_columns(df:pd.DataFrame) ->pd.DataFrame:
    """adds unit and description from the nomvars to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit and description columns added
    :rtype: pd.DataFrame
    """

    vget_unit_and_description = np.vectorize(get_unit_and_description,otypes=['str','str'])
    if 'unit' in df.columns:
        sub_df = df.loc[df['unit'].isna()].copy()
        if not sub_df.empty:
            # sub_df.loc[:,'unit'],sub_df.loc[:,'description'] = vget_unit_and_description(df['nomvar'].values)
            assign_unit_and_description(vget_unit_and_description, sub_df)
            df.loc[df['unit'].isna()] = sub_df
    else:
        df.loc[:,'unit'] = None
        # df.loc[:,'unit'],df.loc[:,'description'] = vget_unit_and_description(df['nomvar'].values)
        assign_unit_and_description(vget_unit_and_description, df)
    return df

def assign_unit_and_description(vget_unit_and_description, df):
    df.loc[:,'unit'],df.loc[:,'description'] = vget_unit_and_description(df['nomvar'].values)

def add_decoded_date_column(df:pd.DataFrame,attr:str='dateo') ->pd.DataFrame:
    """adds the decoded dateo or datev column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :param attr: selected date to decode, defaults to 'dateo'
    :type attr: str, optional
    :return: either date_of_observation or date_of_validity column added to the 
             dataframe
    :rtype: pd.DataFrame
    """
    vconvert_rmndate_to_datetime = np.vectorize(convert_rmndate_to_datetime)#,otypes=['datetime64']
    if attr == 'dateo':
        df.loc[:,'date_of_observation'] = vconvert_rmndate_to_datetime(df['dateo'].values)
    else:
        df.loc[:,'date_of_validity'] = vconvert_rmndate_to_datetime(df['datev'].values)
    return df


def add_forecast_hour_column(df:pd.DataFrame) -> pd.DataFrame:
    """adds the forecast_hour column derived from the deet and npas columns

    :param df:dataframe
    :type df: pd.DataFrame
    :return: forecast_hour column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_forecast_hour
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vcreate_forecast_hour = np.vectorize(get_forecast_hour)#,otypes=['timedelta64[ns]']
    df.loc[:,'forecast_hour'] = vcreate_forecast_hour(df['deet'].values, df['npas'].values)
    return df



def add_data_type_str_column(df:pd.DataFrame) -> pd.DataFrame:
    """adds the data type decoded to string value column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: data_type_str column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_data_type_str
    vcreate_data_type_str = np.vectorize(get_data_type_str,otypes=['str'])
    df.loc[:,'data_type_str'] = vcreate_data_type_str(df['datyp'].values)
    return df


def add_ip_info_columns(df:pd.DataFrame) -> pd.DataFrame:
    """adds all relevant level info from the ip1 column values

    :param df: dataframe
    :type df: pd.DataFrame
    :return: level, ip1_kind, ip1_pkind,surface and follow_topography columns 
             added to the dataframe.
    :rtype: pd.DataFrame
    """
    from .std_dec import get_ip_info
    vcreate_ip_info = np.vectorize(get_ip_info,otypes=['float32','int32','str','float32','int32','str','float32','int32','str','bool','bool','bool','object'])
    df.loc[:,'level'],df.loc[:,'ip1_kind'],df.loc[:,'ip1_pkind'],df.loc[:,'ip2_dec'],df.loc[:,'ip2_kind'],df.loc[:,'ip2_pkind'],df.loc[:,'ip3_dec'],df.loc[:,'ip3_kind'],df.loc[:,'ip3_pkind'],df.loc[:,'surface'],df.loc[:,'follow_topography'],df.loc[:,'ascending'],df.loc[:,'interval'] = vcreate_ip_info(df['ip1'].values,df['ip2'].values,df['ip3'].values)
    return df

def add_columns(df:pd.DataFrame, columns:'list[str]'=['flags','etiket','unit','dateo','datev','forecast_hour','datyp','ip_info']) -> pd.DataFrame:
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
    :return: dataframe with coluns added
    :rtype: pd.DataFrame
    """

    cols = ['flags','etiket','unit','dateo','datev','forecast_hour','datyp','ip_info']
    for col in columns:
        if col not in cols:
            logging.warning(f'{col} not found in {cols}\n')

    
    if 'etiket' in columns:
        df = add_parsed_etiket_columns(df)

    if 'unit' in columns:
        df = add_unit_and_description_columns(df)

    if 'dateo' in columns:
        df = add_decoded_date_column(df,'dateo')

    if 'datev' in columns:
        df = add_decoded_date_column(df,'datev')

    if 'forecast_hour' in columns:
        df = add_forecast_hour_column(df)

    if 'datyp' in columns:
        df = add_data_type_str_column(df)

    if ('ip_info' in  columns):
        df = add_ip_info_columns(df)

    if 'flags' in columns:
        df = add_flag_values(df)

    if 'ip_info' in columns:
        df = set_vertical_coordinate_type(df)

    return df


def reorder_columns(df) -> pd.DataFrame:
    ordered = ['nomvar','typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas','datyp', 'nbits' , 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    if df.empty:
        return df
    all_columns = set(df.columns)
    # all_columns = set(df.columns.to_list())

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns))

    df = df[ordered]
    return df


def set_vertical_coordinate_type(df) -> 'pd.DataFrame|pd.Series[Dtype@pconcat]':
    from fstpy import VCTYPES
    if 'level' not in df.columns:
        df = add_ip_info_columns(df)
    newdfs=[]
    df.loc[:,'vctype'] = 'UNKNOWN'
    grid_groups = df.groupby(df.grid)

    for _, grid in grid_groups:
        toctoc, p0, e1, pt, hy, sf, vcode = get_meta_fields_exists(grid)
        ip1_kind_groups = grid.groupby(grid.ip1_kind)
        for _, ip1_kind_group in ip1_kind_groups:
            #these ip1_kinds are not defined
            without_meta = ip1_kind_group.loc[(~ip1_kind_group.ip1_kind.isin([-1,3,6])) & (~ip1_kind_group.nomvar.isin(["!!","HY","P0","PT",">>","^^","PN"]))]
            if not without_meta.empty:
                ip1_kind = without_meta.iloc[0]['ip1_kind']
                # print(vcode)
                if len(vcode) > 1:
                    for vc in vcode:
                        d,_=divmod(vc,1000)
                        if ip1_kind == d:
                            this_vcode = vc
                            continue
                else:
                    this_vcode = vcode[0]
                ip1_kind_group.loc[:,'vctype'] = 'UNKNOWN'
                #vctype_dict = {'ip1_kind':ip1_kind,'toctoc':toctoc,'P0':p0,'E1':e1,'PT':pt,'HY':hy,'SF':sf,'vcode':vcode}
                # print(VCTYPES)
                # print(VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(5,False,True,False,False,False,False,-1)))
                # print('\n(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                # vctyte_df = VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                vctyte_df = VCTYPES.loc[(VCTYPES.ip1_kind==ip1_kind) & (VCTYPES.toctoc==toctoc) & (VCTYPES.P0==p0) & (VCTYPES.E1==e1) & (VCTYPES.PT==pt) & (VCTYPES.HY==hy) & (VCTYPES.SF==sf) & (VCTYPES.vcode==this_vcode)]
                # print(vctyte_df)
                if not vctyte_df.empty:
                    if len(vctyte_df.index)>1:
                        logging.warning('set_vertical_coordinate_type - more than one match!!!')
                    ip1_kind_group.loc[:,'vctype'] = vctyte_df.iloc[0]['vctype']
            newdfs.append(ip1_kind_group)

    df = pd.concat(newdfs,ignore_index=True)
    
    df.loc[df.nomvar.isin([">>","^^","!!","P0","PT","HY","PN","!!SF"]), "vctype"] = "UNKNOWN"
    return df

def get_meta_fields_exists(grid):
    toctoc = grid.loc[grid.nomvar=="!!"]
    vcode = []
    if not toctoc.empty:
        for i in toctoc.index:
            vcode.append(toctoc.at[i,'ig1'])
        toctoc = True
    else:
        vcode.append(-1)
        toctoc = False
    p0 = meta_exists(grid,"P0")
    e1 = meta_exists(grid,"E1")
    pt = meta_exists(grid,"PT")
    hy = meta_exists(grid,"HY")
    sf = meta_exists(grid,"!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode

def meta_exists(grid, nomvar) -> bool:
    df = grid.loc[grid.nomvar==nomvar]
    return not df.empty
