# -*- coding: utf-8 -*-
import sys
from math import isnan

import dask
import dask.array as da
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

import fstpy
from fstpy import DATYP_DICT, KIND_DICT

from .dataframe import remove_from_df, reorder_columns, sort_dataframe
from .exceptions import SelectError, StandardFileError
from .logger_config import logger
from .std_dec import convert_rmndate_to_datetime, decode_ip
from .std_reader import StandardFileReader, load_data
from .utils import validate_df_not_empty


def fstcomp(file1:str, file2:str, columns=['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], verbose=False) -> bool:
    """Utility used to compare the contents of two RPN standard files (record by record).

    :param file1: path to file 1
    :type file1: str
    :param file2: path to file 2
    :type file2: str
    :param columns: columns to be considered, defaults to ['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    :type columns: list, optional
    :param verbose: if activated prints more information, defaults to False
    :type verbose: bool, optional
    :raises StandardFileError: type of error that will be raised
    :return: True if files are sufficiently similar else False
    :rtype: bool
    """
    logger.info('fstcomp -A %s -B %s'%(file1,file2))
    import os
    if not os.path.exists(file1):
        logger.error('fstcomp - %s does not exist' % file1)
        raise StandardFileError('fstcomp - %s does not exist' % file1)
    if not os.path.exists(file2):
        logger.error('fstcomp - %s does not exist' % file2)
        raise StandardFileError('fstcomp - %s does not exist' % file2)    
    # open and read files
    df1 = StandardFileReader(file1,load_data=True).to_pandas()
    # print('df1',df1)
    df2 = StandardFileReader(file2,load_data=True).to_pandas()
    #print('df2',df2)
    return fstcomp_df(df1, df2, columns, print_unmatched=True if verbose else False)

def voir(df:pd.DataFrame,style=False):
    """Displays the metadata of the supplied records in the rpn voir format"""
    validate_df_not_empty(df,'voir',StandardFileError)

    df['datyp'] = df['datyp'].map(DATYP_DICT)
    df['datev'] = df['datev'].apply(convert_rmndate_to_datetime)
    df['dateo'] = df['dateo'].apply(convert_rmndate_to_datetime)
    res = df['ip1'].apply(decode_ip)
    df['level'] = None
    df[' '] = None
    for i in df.index:
        df.at[i,'level']=res.loc[i][0] 
        df.at[i,' '] = res.loc[i][2]

    res_df = df.sort_values(by=['nomvar','level'],ascending=True)

    if style:
        res_df = res_df.drop(columns=['dateo','grid','run','implementation','ensemble_member','shape','key','d','path','file_modification_time','ip1_kind','ip2_dec','ip2_kind','ip2_pkind','ip3_dec','ip3_kind','ip3_pkind','date_of_observation','date_of_validity','forecast_hour','d'],errors='ignore')
        res_df = reorder_columns(res_df,ordered=['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'datev', 'level',' ','ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'])        
    else:
        res_df = res_df.drop(columns=['datev','grid','run','implementation','ensemble_member','shape','key','d','path','file_modification_time','ip1_kind','ip2_dec','ip2_kind','ip2_pkind','ip3_dec','ip3_kind','ip3_pkind','date_of_observation','date_of_validity','forecast_hour','d'],errors='ignore')

    #logger.debug('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    sys.stdout.writelines(res_df.reset_index(drop=True).to_string(header=True))




def zap(df:pd.DataFrame, validate_keys=True,**kwargs:dict ) -> pd.DataFrame:
    """Modifies column values of a dataframe according to specific criteria

    :param df: input dataframe
    :type df: pd.DataFrame
    :param validate_keys: checks if the supplied keys are valid, defaults to True
    :type validate_keys: bool, optional
    :return: modified dataframe
    :rtype: pd.DataFrame
    """
    
    validate_df_not_empty(df,'zap',StandardFileError)            
    if validate_zap_keys:
        validate_zap_keys(**kwargs)

    logger.info('zap - ' + str(kwargs)[0:100] + '...')

    #res_df = create_load_data_info(df)
    res_df = df
    #res_df.loc[:,'dirty'] = True
    #res_df['key'] = np.nan
    for k,v in kwargs.items():
        if (k == 'level') and ('ip1_kind' in  kwargs.keys()):
            res_df = zap_level(res_df,v,kwargs['ip1_kind'])
            continue
        elif (k == 'level') and ('ip1_kind' not in  kwargs.keys()):
            logger.warning("zap - can't zap level without ip1_kind")
            continue
        if k == 'ip1':
            res_df = zap_ip1(res_df,v)
            continue
        if k == 'npas':
            res_df = zap_npas(res_df, v)
            continue
        if k == 'forecast_hour':
            res_df = zap_forecast_hour(res_df, v)
            continue
        if k == 'ip1_kind':
            pass
        if k == 'ip1_pkind':
            pass
        res_df.loc[:,k] = v
    # if mark:
    #     res_df.loc[:,'typvar'] = res_df['typvar'].str.cat([ 'Z' for x in res_df.index])
    res_df = sort_dataframe(res_df) 
    return res_df

def fststat(df:pd.DataFrame) -> pd.DataFrame:
    """Produces summary statistics for a dataframe

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: output dataframe with added 'mean','std','min_pos','min','max_pos','max' columns
    :rtype: pd.DataFrame
    """
    
    logger.info('fststat')
    pd.options.display.float_format = '{:0.6E}'.format
    validate_df_not_empty(df,'fststat',StandardFileError)
    df = load_data(df)
    df = compute_stats(df)
    res = df['ip1'].apply(decode_ip)
    df['level'] = None
    df[' '] = None
    for i in df.index:
        df.at[i,'level']=res.loc[i][0] 
        df.at[i,' '] = res.loc[i][2]

    df.sort_values(by=['nomvar','level'],ascending=True,inplace=True)

    # if 'level' in df.columns:
    print(df[['nomvar','typvar','level',' ','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string(formatters={'level':'{:,.6f}'.format}))
    # else:    
    #     print(res_df[['nomvar','typvar','ip1','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string())
    del df[' ']
    return df    

def select(df:pd.DataFrame, query_str:str, exclude:bool=False, no_fail=False, engine=None) -> pd.DataFrame:
    """Wrapper for pandas dataframe query function. Adds some checks to work with standard file dataframes

    :param df: input dataframe
    :type df: pd.DataFrame
    :param query_str: a query string as described in the pandas documentation
    :type query_str: str
    :param exclude: reverses a simple query, defaults to False
    :type exclude: bool, optional
    :param no_fail: if True, will raise an exception on failure instead of returning an empty dataframe, defaults to False
    :type no_fail: bool, optional
    :param engine: query engine name, see pandas documentation, defaults to None
    :type engine: [type], optional
    :raises SelectError: type of exception raised on failure if no_fail is active
    :return: result of query as a dataframe
    :rtype: pd.DataFrame
    """
    # print a summay ry of query
    #logger.info('select %s' % query_str[0:100])
    # warn if selecting by 'forecast_hour'
    if 'forecast_hour' in query_str:
        logger.warning('select - selecting forecast_hour might not return expected results - it is a claculated value (forecast_hour = deet * npas / 3600.)')
        logger.info('select - avalable forecast hours are %s' % list(df.forecast_hour.unique()))
    if isinstance(engine,str):
        view = df.query(query_str,engine=engine)
        tmp_df = view.copy(deep=True)
    else:
        view = df.query(query_str)
        tmp_df = view.copy(deep=True)
    if tmp_df.empty:
        if no_fail:
            return pd.DataFrame(dtype=object)
        else:
            logger.warning('select - no matching records for query %s' % query_str[0:200])
            raise SelectError('select - failed!')
    if exclude:
        columns = df.columns.values.tolist()
        columns.remove('d')
        #columns.remove('fstinl_params')
        tmp_df = pd.concat([df, tmp_df]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df) 
    return tmp_df


def select_zap(df:pd.DataFrame, query:str, **kwargs:dict) -> pd.DataFrame:
    """Selects a subset of records from a dataframe and applies zap to those records instead of the whole dataframe

    :param df: input dataframe
    :type df: pd.DataFrame
    :param query: query string, see pandas documentation
    :type query: str
    :return: the modified dataframe
    :rtype: pd.DataFrame
    """
    selection_df = select(df,query)
    df = remove_from_df(df,selection_df)
    zapped_df = zap(selection_df,**kwargs)
    res_df = pd.concat([df,zapped_df])
    res_df = sort_dataframe(res_df)
    return res_df

##################################################################################################
##################################################################################################
##################################################################################################
def remove_meta_data_fields(df: pd.DataFrame) -> pd.DataFrame:
    for meta in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]:
        df = df[df.nomvar != meta]
    return df


    #df = df.reindex(columns = df.columns.tolist() + ['min','max','mean','std','min_pos','max_pos'])   

# min_delayed = [dask.delayed(np.nanmin)(df.at[i, 'd']) for i in df.index]
# max_delayed = [dask.delayed(np.nanmax)(df.at[i, 'd']) for i in df.index]
# mean_delayed = [dask.delayed(np.nanmean)(df.at[i, 'd']) for i in df.index]
# std_delayed = [dask.delayed(np.nanstd)(df.at[i, 'd']) for i in df.index]
# df['min'] = dask.compute(min_delayed)[0]
# df['max'] = dask.compute(max_delayed)[0]
# df['mean'] = dask.compute(mean_delayed)[0]
# df['std'] = dask.compute(std_delayed)[0]
def compute_stats_dask(df:pd.DataFrame) -> pd.DataFrame:

    df['min'] = None
    df['max'] = None
    df['mean'] = None
    df['std'] = None
    df['min_pos'] = None
    df['max_pos'] = None
    #add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
    #add_empty_columns(df, ['min_pos','max_pos'],None,dtype_str='O')
    min_delayed=[]
    max_delayed=[]
    mean_delayed=[]
    std_delayed=[]
    argmin_delayed=[]
    #min_pos_delayed=[]
    argmax_delayed=[]
    #max_pos_delayed=[]
    for i in df.index:   
        if isinstance(df.at[i, 'd'],np.ndarray):
            df.at[i, 'd'] = da.from_array(df.at[i, 'd'])
        min_delayed.append(dask.delayed(np.nanmin)(df.at[i, 'd']))
        max_delayed.append(dask.delayed(np.nanmax)(df.at[i, 'd']))
        mean_delayed.append(dask.delayed(np.nanmean)(df.at[i, 'd']))
        std_delayed.append(dask.delayed(np.nanstd)(df.at[i, 'd']))
        argmin_delayed.append(dask.delayed(np.nanargmin)(df.at[i, 'd']))
        #min_pos_delayed.append(dask.delayed(np.unravel_index)(argmin_delayed, (df.at[i,'ni'],df.at[i,'nj'])))
        argmax_delayed.append(dask.delayed(np.nanargmax)(df.at[i, 'd']))
        #max_pos_delayed.append(dask.delayed(np.unravel_index)(argmax_delayed, (df.at[i,'ni'],df.at[i,'nj'])))
        # max_pos_delayed = dask.delayed(np.unravel_index)(dask.delayed(np.nanargmax)(df.at[i, 'd']), (df.at[i,'ni'],df.at[i,'nj']))


    df['min'] = dask.compute(min_delayed)[0]
    df['max'] = dask.compute(max_delayed)[0]
    df['mean'] = dask.compute(mean_delayed)[0]
    df['std'] = dask.compute(std_delayed)[0]
    df['min_pos'] = dask.compute(argmin_delayed)[0]
    #argmax = dask.compute(argmax_delayed)[0]
    df['max_pos'] = dask.compute(argmax_delayed)[0]
    # max_pos = dask.compute(max_pos_delayed)[0]
    df['min_pos1'] = None
    df['max_pos1'] = None
    for i in df.index:
        df.at[i,'min_pos1'] = np.unravel_index(df.at[i,'min_pos'], (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'max_pos1'] = np.unravel_index(df.at[i,'max_pos'], (df.at[i,'ni'],df.at[i,'nj']))

    df['min_pos'] = df['min_pos1']
    df['max_pos'] = df['max_pos1']
    #df = fstpy.dataframe.sort_dataframe(df)    
    return df

def compute_stats(df:pd.DataFrame) -> pd.DataFrame:
    df['min'] = None
    df['max'] = None
    df['mean'] = None
    df['std'] = None
    df['min_pos'] = (0,0)
    df['max_pos'] = (0,0)
    #add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
    #add_empty_columns(df, ['min_pos','max_pos'],None,dtype_str='O')
    for i in df.index:
        df.at[i,'mean'] = np.nanmean(df.at[i,'d'])
        df.at[i,'std'] = np.nanstd(df.at[i,'d'])
        df.at[i,'min'] = np.nanmin(df.at[i,'d'])
        df.at[i,'max'] = np.nanmax(df.at[i,'d'])
        min_pos = np.unravel_index(np.nanargmin(df.at[i,'d']), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'min_pos'] = (min_pos[0] + 1, min_pos[1]+1)
        max_pos = np.unravel_index(np.nanargmax(df.at[i,'d']), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'max_pos'] = (max_pos[0] + 1, max_pos[1]+1)
    #df = fstpy.dataframe.sort_dataframe(df)    
    return df

# def create_load_data_info(df:pd.DataFrame) -> pd.DataFrame:
#     for i in df.index:
#         if df.at[i,'d'] is None == False:
#             return df
#         if df.at[i,'key'] != None:
#             fstinl_params={
#             'etiket':df.at[i,'etiket'],
#             'datev':df.at[i,'datev'],
#             'ip1':df.at[i,'ip1'],
#             'ip2':df.at[i,'ip2'],
#             'ip3':df.at[i,'ip3'],
#             'typvar':df.at[i,'typvar'],
#             'nomvar':df.at[i,'nomvar']}
#             df.at[i,'fstinl_params'] = fstinl_params
#     return df

def validate_zap_keys(**kwargs):
    available_keys = {'grid', 'forecast_hour', 'nomvar', 'typvar', 'etiket', 'dateo', 'datev', 'ip1', 'ip2', 'ip3', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'level', 'ip1_kind', 'ip1_pkind','unit'}
    keys_to_modify = set(kwargs.keys())
    acceptable_keys = keys_to_modify.intersection(available_keys)
    if len(acceptable_keys) != len(keys_to_modify):
        logger.warning("zap - can't find modifiable key in available keys. asked for %s in %s"%(keys_to_modify,available_keys))
        raise StandardFileError("zap - can't find modifiable key in available keys")

def zap_ip1(df:pd.DataFrame, ip1_value:int) -> pd.DataFrame:
    
    logger.warning('zap - changed ip1, triggers updating level and ip1_kind')
    df.loc[:,'ip1'] = ip1_value
    level, ip1_kind, ip1_pkind = decode_ip(ip1_value)
    df.loc[:,'level'] = level
    df.loc[:,'ip1_kind'] = ip1_kind
    df.loc[:,'ip1_pkind'] = ip1_pkind
    return df

def zap_level(df:pd.DataFrame, level_value:float, ip1_kind_value:int) -> pd.DataFrame:
    logger.warning('zap - changed level, triggers updating ip1')
    df['level'] = level_value
    df['ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, level_value, ip1_kind_value)
    return df

def zap_ip1_kind(df:pd.DataFrame, ip1_kind_value:int) -> pd.DataFrame:
    logger.warning('zap - changed ip1_kind, triggers updating ip1 and ip1_pkind')
    df['ip1_kind'] = ip1_kind_value
    df['ip1_pkind'] = KIND_DICT[int(ip1_kind_value)]
    for i in df.index:
        df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], ip1_kind_value)
    return df

def zap_pip1_kind(df:pd.DataFrame, ip1_pkind_value:str) -> pd.DataFrame:
    logger.warning('zap - changed ip1_pkind, triggers updating ip1 and ip1_kind')
    df['ip1_pkind'] = ip1_pkind_value
    #invert ip1_kind dict
    PKIND_DICT = {v: k for k, v in KIND_DICT.items()}
    df['ip1_kind'] = PKIND_DICT[ip1_pkind_value]
    for i in df.index:
        df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], df.at[i,'ip1_kind'])
    return df

def zap_npas(df:pd.DataFrame, npas_value:int) -> pd.DataFrame:
    logger.warning('zap - changed npas, triggers updating forecast_hour')
    df['npas'] = npas_value
    for i in df.index:
        df.at[i,'forecast_hour'] =  df.at[i,'deet'] * df.at[i,'npas'] / 3600.
        df.at[i,'ip2'] = np.floor(df.at[i,'forecast_hour']).astype(int)
    return df


def zap_forecast_hour(df:pd.DataFrame, forecast_hour_value:int) -> pd.DataFrame:
    logger.warning('zap - changed forecast_hour, triggers updating npas')
    df['forecast_hour'] = forecast_hour_value
    df['ip2'] = np.floor(df['forecast_hour']).astype(int)
    for i in df.index:
        df.at[i,'npas'] = df.at[i,'forecast_hour'] * 3600. / df.at[i,'deet']
        df.at[i,'npas'] = df.at[i,'npas'].astype(int)
    return df


def add_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
    diff['abs_diff'] = diff['d_x'].copy(deep=True)
    diff['e_rel_max'] = diff['d_x'].copy(deep=True)
    diff['e_rel_moy'] = diff['d_x'].copy(deep=True)
    diff['var_a'] = diff['d_x'].copy(deep=True)
    diff['var_b'] = diff['d_x'].copy(deep=True)
    diff['c_cor'] = diff['d_x'].copy(deep=True)
    diff['moy_a'] = diff['d_x'].copy(deep=True)
    diff['moy_b'] = diff['d_x'].copy(deep=True)
    diff['bias'] = diff['d_x'].copy(deep=True)
    diff['e_max'] = diff['d_x'].copy(deep=True)
    diff['e_moy'] = diff['d_x'].copy(deep=True)
    diff.drop(columns=['d_x', 'd_y'], inplace=True,errors='ignore')
    return diff


def del_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
    diff['etiket'] = diff['etiket_x']
    #diff['ip1_kind'] = diff['ip1_kind_x']
    #diff['ip2'] = diff['ip2_x']
    #diff['ip3'] = diff['ip3_x']
    diff.drop(columns=['abs_diff'], inplace=True,errors='ignore')
    return diff

def fstcomp_df(df1: pd.DataFrame, df2: pd.DataFrame, exclude_meta=True, columns=['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], print_unmatched=False) -> bool:
    df1.sort_values(by=columns,inplace=True)
    df2.sort_values(by=columns,inplace=True)
    success = False
    pd.options.display.float_format = '{:0.6E}'.format
    # check if both df have records
    if df1.empty or df2.empty:
        logger.error('you must supply files witch contain records')
        if df1.empty:
            logger.error('file 1 is empty')
        if df2.empty:
            logger.error('file 2 is empty')
        raise fstpy.StandardFileError('fstcomp - one of the files is empty')
    # remove meta data {!!,>>,^^,P0,PT,HY,!!SF} from records to compare
    if exclude_meta:
        df1 = remove_meta_data_fields(df1)
        df2 = remove_meta_data_fields(df2)
    # voir(df1)
    # voir(df2)    
    for i in df1.index:
        if df1.at[i,'d'].all() != df2.at[i,'d'].all():
            print(df1.at[i,'d'][:10],df2.at[i,'d'][:10])
    #logger.debug('A',df1['d'][:100].to_string())
    #logger.debug('A',df1.loc[i])#[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']])
    #logger.debug('----------')
    #logger.debug('B',df2['d'][:100].to_string())
    #logger.debug('B',df2.loc[i])#[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']])    
    # check if they are exactly the same
    if df1.equals(df2):
        # logger.debug('files are indetical - excluding meta data fields')
        # logger.debug('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
        # logger.debug('----------')
        # logger.debug('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
        return True
    #create common fields
    common = pd.merge(df1, df2, how='inner', on=columns)
    #Rows in df1 Which Are Not Available in df2
    common_with_1 = common.merge(df1, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
    #Rows in df2 Which Are Not Available in df1
    common_with_2 = common.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
    missing = pd.concat([common_with_1, common_with_2])
    missing = remove_meta_data_fields(missing)
    if len(common.index) != 0:
        if len(common_with_1.index) != 0:
            if print_unmatched:
                logger.info('df in file 1 that were not found in file 2 - excluded from comparison')
                logger.info(common_with_1.to_string())
        if len(common_with_2.index) != 0:
            if print_unmatched:
                logger.info('df in file 2 that were not found in file 1 - excluded from comparison')
                logger.info(common_with_2.to_string())
    else:
        logger.error('fstcomp - no common df to compare')
        logger.error('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].to_string())
        logger.error('----------')
        logger.error('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].to_string())
        raise StandardFileError('fstcomp - no common df to compare')
    diff = common.copy()
    voir(diff)
    diff = add_fstcomp_columns(diff)
    
    success = compute_fstcomp_stats(common, diff)
    diff = del_fstcomp_columns(diff)
    if len(diff.index):
        logger.info(diff[['nomvar', 'etiket', 'ip1', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'biais', 'e_max', 'e_moy','diff_percent']].to_string(formatters={'level': '{:,.6f}'.format,'diff_percent': '{:,.1f}%'.format}))
        #logger.debug(diff[['nomvar', 'etiket', 'ip1_pkind', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'bias', 'e_max', 'e_moy']].to_string())
    if len(missing.index):
        logger.info('missing df')
        logger.info(missing[['nomvar', 'etiket', 'ip1', 'ip2', 'ip3']].to_string(header=False, formatters={'level': '{:,.6f}'.format}))
        #logger.debug(missing[['nomvar', 'etiket', 'ip1_pkind', 'ip2', 'ip3']].to_string(header=False))
    return success

def compute_fstcomp_stats(common: pd.DataFrame, diff: pd.DataFrame) -> bool:
    
    success = True

    for i in common.index:
        a = common.at[i, 'd_x'].flatten()
        b = common.at[i, 'd_y'].flatten()
        diff.at[i, 'abs_diff'] = np.abs(a-b)

        derr = np.where(a == 0, np.abs(1-a/b), np.abs(1-b/a))
        derr_sum=np.nansum(derr)
        if isnan(derr_sum):
            diff.at[i, 'e_rel_max'] = 0.
            diff.at[i, 'e_rel_moy'] = 0.
        else:    
            diff.at[i, 'e_rel_max'] = 0. if isnan(np.nanmax(derr)) else np.nanmax(derr)
            diff.at[i, 'e_rel_moy'] = 0. if isnan(np.nanmean(derr)) else np.nanmean(derr)
        sum_a2 = np.nansum(a**2)
        sum_b2 = np.nansum(b**2)
        diff.at[i, 'var_a'] = np.nanmean(sum_a2)
        diff.at[i, 'var_b'] = np.nanmean(sum_b2)
        diff.at[i, 'moy_a'] = np.nanmean(a)
        diff.at[i, 'moy_b'] = np.nanmean(b)
        
        c_cor = np.nansum(a*b)
        if sum_a2*sum_b2 != 0:
            c_cor = c_cor/np.sqrt(sum_a2*sum_b2)
        elif (sum_a2==0) and (sum_b2==0):
            c_cor = 1.0
        elif sum_a2 == 0:
            c_cor = np.sqrt(diff.at[i, 'var_b'])
        else:
            c_cor = np.sqrt(diff.at[i, 'var_a'])
        diff.at[i, 'c_cor'] = c_cor 
        diff.at[i, 'biais']=diff.at[i, 'moy_b']-diff.at[i, 'moy_a']
        diff.at[i, 'e_max'] = np.nanmax(diff.at[i, 'abs_diff'])
        diff.at[i, 'e_moy'] = np.nanmean(diff.at[i, 'abs_diff'])
        
        nbdiff = np.count_nonzero(a!=b)
        diff.at[i, 'diff_percent'] = nbdiff / a.size * 100.0
        if not ((-1.000001 <= diff.at[i, 'c_cor'] <= 1.000001) and (-0.1 <= diff.at[i, 'e_rel_max'] <= 0.1) and (-0.1 <= diff.at[i, 'e_rel_moy'] <= 0.1)):
            diff.at[i, 'nomvar'] = '<' + diff.at[i, 'nomvar'] + '>'
            success = False
    return success            
