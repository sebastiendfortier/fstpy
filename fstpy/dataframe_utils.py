# -*- coding: utf-8 -*-
import math
import sys

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from fstpy import DATYP_DICT

from .dataframe import add_ip_info_columns, reorder_columns
from .exceptions import SelectError, StandardFileError
from .std_dec import convert_rmndate_to_datetime
from .std_reader import load_data


def select_with_meta(df:pd.DataFrame,nomvar:list) -> pd.DataFrame:
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
        res_df = df.loc[df.nomvar==var]
        if res_df.empty:
            raise SelectError(f'missing {var} in dataframe')
        results.append(res_df)

    meta_df = select_meta(df)

    if not meta_df.empty:
        results.append(meta_df)

    selection_result_df = pd.concat(results,ignore_index=True)

    selection_result_df = metadata_cleanup(selection_result_df)

    return selection_result_df

def metadata_cleanup(df:pd.DataFrame,strict_toctoc=True) -> pd.DataFrame:
    """Cleans the metadata from a dataframe according to rules.   
  
    :param df: dataframe to clean  
    :type df: pd.DataFrame  
    :return: dataframe with only cleaned meta_data  
    :rtype: pd.DataFrame  
    """

    if df.empty:
        return df

    # no_meta_df = df.query('nomvar not in  ["!!","P0","PT",">>","^^","^>","HY","!!SF"]')
    no_meta_df = df.loc[~df.nomvar.isin(["!!","P0","PT",">>","^^","^>","HY","!!SF"])]
    # print(no_meta_df.grid)
    # get deformation fields
    grid_deformation_fields_df = get_grid_deformation_fileds(df,no_meta_df)

    sigma_ips = get_sigma_ips(no_meta_df)

    hybrid_ips = get_hybrid_ips(no_meta_df)

    # get P0's
    p0_fields_df = get_p0_fields(df,no_meta_df,hybrid_ips,sigma_ips)

    #get PT's
    pt_fields_df = get_pt_fields(df,no_meta_df,sigma_ips)

    #get HY
    hy_field_df = get_hy_field(df,hybrid_ips)

    pressure_ips = get_pressure_ips(no_meta_df)
    # print('no_meta_df\n',no_meta_df[['nomvar','typvar','etiket','ip1', 'ip2', 'ip3', 'datev']].to_string())
    #get !!'s strict
    toctoc_fields_df = get_toctoc_fields(df,hybrid_ips,sigma_ips,pressure_ips,strict_toctoc)
    # print('cleaned toctoc\n',toctoc_fields_df)
    df = pd.concat([no_meta_df,grid_deformation_fields_df,p0_fields_df,pt_fields_df,hy_field_df,toctoc_fields_df],ignore_index=True)

    return df

# class FstCompError(Exception):
#     pass

# def fstcomp(file1:str, file2:str, exclude_meta=False, cmp_number_of_fields=True,columns=['nomvar', 'etiket','ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], verbose=False,e_max=0.0001,e_c_cor=0.00001) -> bool:
#     """Utility used to compare the contents of two RPN standard files (record by record).

#     :param file1: path to file 1
#     :type file1: str
#     :param file2: path to file 2
#     :type file2: str
#     :param columns: columns to be considered, defaults to ['nomvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
#     :type columns: list, optional
#     :param verbose: if activated prints more information, defaults to False
#     :type verbose: bool, optional
#     :raises StandardFileError: type of error that will be raised
#     :return: True if files are sufficiently similar else False
#     :rtype: bool
#     """
#     sys.stdout.write('fstcomp -A %s -B %s\n'%(file1,file2))


#     if not os.path.exists(file1):
#         sys.stderr.write('fstcomp - %s does not exist\n' % file1)
#         raise StandardFileError('fstcomp - %s does not exist' % file1)
#     if not os.path.exists(file2):
#         sys.stderr.write('fstcomp - %s does not exist\n' % file2)
#         raise StandardFileError('fstcomp - %s does not exist' % file2)
#     # open and read files
#     df1 = StandardFileReader(file1).to_pandas()

#     df2 = StandardFileReader(file2).to_pandas()
#     # print('df2',df2)

#     return fstcomp_df(df1, df2, exclude_meta, cmp_number_of_fields, columns, print_unmatched=verbose,e_max=e_max,e_c_cor=e_c_cor)

def voir(df:pd.DataFrame,style=False):
    """Displays the metadata of the supplied records in the rpn voir format"""
    if df.empty:
        raise StandardFileError('voir - no records to process')

    to_print_df = df.copy()
    to_print_df['datyp'] = to_print_df['datyp'].map(DATYP_DICT)
    to_print_df['datev'] = to_print_df['datev'].apply(convert_rmndate_to_datetime)
    to_print_df['dateo'] = to_print_df['dateo'].apply(convert_rmndate_to_datetime)
    to_print_df = add_ip_info_columns(to_print_df)
    # res = df['ip1'].apply(decode_ip)
    # df['level'] = None
    # df[' '] = None
    # for i in df.index:
    #     df.at[i,'level']=res.loc[i][0]
    #     df.at[i,' '] = res.loc[i][2]

    res_df = to_print_df.sort_values(by=['nomvar','level'],ascending=[True,False])

    if style:
        res_df = res_df.drop(columns=['dateo','grid','run','implementation','ensemble_member','shape','key','d','path','file_modification_time','ip1_kind','ip2_dec','ip2_kind','ip2_pkind','ip3_dec','ip3_kind','ip3_pkind','date_of_observation','date_of_validity','forecast_hour','d','surface','follow_topography','ascending','interval'],errors='ignore')
        res_df = reorder_columns(res_df,ordered=['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'datev', 'level',' ','ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'])
    else:
        res_df = res_df.drop(columns=['datev','grid','run','implementation','ensemble_member','shape','key','d','path','file_modification_time','ip1_kind','ip2_dec','ip2_kind','ip2_pkind','ip3_dec','ip3_kind','ip3_pkind','date_of_observation','date_of_validity','forecast_hour','d','surface','follow_topography','ascending','interval'],errors='ignore')

    #logger.debug('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    sys.stdout.write('%s\n'%res_df.reset_index(drop=True).to_string(header=True))




# def zap(df:pd.DataFrame, validate_keys=True,**kwargs:dict ) -> pd.DataFrame:
#     """Modifies column values of a dataframe according to specific criteria

#     :param df: input dataframe
#     :type df: pd.DataFrame
#     :param validate_keys: checks if the supplied keys are valid, defaults to True
#     :type validate_keys: bool, optional
#     :return: modified dataframe
#     :rtype: pd.DataFrame
#     """

#     if df.empty:
#         raise StandardFileError('zap - no records to process')
#     if validate_zap_keys:
#         validate_zap_keys(**kwargs)

#     sys.stdout.write('zap - %s...\n'% str(kwargs)[0:100])

#     #res_df = create_load_data_info(df)
#     res_df = df
#     #res_df.loc[:,'dirty'] = True
#     #res_df['key'] = np.nan
#     for k,v in kwargs.items():
#         if (k == 'level') and ('ip1_kind' in  kwargs.keys()):
#             res_df = zap_level(res_df,v,kwargs['ip1_kind'])
#             continue
#         elif (k == 'level') and ('ip1_kind' not in  kwargs.keys()):
#             sys.stdout.write("zap - can't zap level without ip1_kind\n")
#             continue
#         if k == 'ip1':
#             res_df = zap_ip1(res_df,v)
#             continue
#         if k == 'npas':
#             res_df = zap_npas(res_df, v)
#             continue
#         if k == 'forecast_hour':
#             res_df = zap_forecast_hour(res_df, v)
#             continue
#         if k == 'ip1_kind':
#             pass
#         if k == 'ip1_pkind':
#             pass
#         res_df.loc[:,k] = v
#     # if mark:
#     #     res_df.loc[:,'typvar'] = res_df['typvar'].str.cat([ 'Z' for x in res_df.index])
#     res_df = sort_dataframe(res_df)
#     return res_df

def fststat(df:pd.DataFrame) -> pd.DataFrame:
    """Produces summary statistics for a dataframe

    :param df: input dataframe
    :type df: pd.DataFrame
    :return: output dataframe with added 'mean','std','min_pos','min','max_pos','max' columns
    :rtype: pd.DataFrame
    """

    sys.stdout.write('fststat\n')
    pd.options.display.float_format = '{:0.6E}'.format
    if df.empty:
        raise StandardFileError('fststat - no records to process')
    df = load_data(df)
    df = compute_stats(df)
    df = add_ip_info_columns(df)
    # res = df['ip1'].apply(decode_ip)
    # df['level'] = None
    # df[' '] = None
    # for i in df.index:
    #     df.at[i,'level']=res.loc[i][0]
    #     df.at[i,' '] = res.loc[i][2]

    df.sort_values(by=['nomvar','level'],ascending=[True,False],inplace=True)

    # if 'level' in df.columns:
    print(df[['nomvar','typvar','level',' ','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string(formatters={'level':'{:,.6f}'.format}))
    # else:
    #     print(res_df[['nomvar','typvar','ip1','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string())
    del df[' ']
    return df

# def select(df:pd.DataFrame, query_str:str, exclude:bool=False, no_fail=False, engine=None) -> pd.DataFrame:
#     """Wrapper for pandas dataframe query function. Adds some checks to work with standard file dataframes

#     :param df: input dataframe
#     :type df: pd.DataFrame
#     :param query_str: a query string as described in the pandas documentation
#     :type query_str: str
#     :param exclude: reverses a simple query, defaults to False
#     :type exclude: bool, optional
#     :param no_fail: if True, will raise an exception on failure instead of returning an empty dataframe, defaults to False
#     :type no_fail: bool, optional
#     :param engine: query engine name, see pandas documentation, defaults to None
#     :type engine: [type], optional
#     :raises SelectError: type of exception raised on failure if no_fail is active
#     :return: result of query as a dataframe
#     :rtype: pd.DataFrame
#     """
#     # print a summay ry of query
#     #sys.stdout.write('select %s' % query_str[0:100])
#     # warn if selecting by 'forecast_hour'
#     if 'forecast_hour' in query_str:
#         sys.stdout.write('select - selecting forecast_hour might not return expected results - it is a claculated value (forecast_hour = deet * npas / 3600.)\n')
#         sys.stdout.write('select - avalable forecast hours are %s\n' % list(df.forecast_hour.unique()))
#     if isinstance(engine,str):
#         view = df.query(query_str,engine=engine)
#         tmp_df = view.copy(deep=True)
#     else:
#         view = df.query(query_str)
#         tmp_df = view.copy(deep=True)
#     if tmp_df.empty:
#         if no_fail:
#             return pd.DataFrame(dtype=object)
#         else:
#             sys.stdout.write('select - no matching records for query %s\n' % query_str[0:200])
#             raise SelectError('select - failed!')
#     if exclude:
#         columns = df.columns.values.tolist()
#         columns.remove('d')
#         #columns.remove('fstinl_params')
#         tmp_df = pd.concat([df, tmp_df],ignore_index=True).drop_duplicates(subset=columns,keep=False)
#     tmp_df = sort_dataframe(tmp_df)
#     return tmp_df


# def select_zap(df:pd.DataFrame, query:str, **kwargs:dict) -> pd.DataFrame:
#     """Selects a subset of records from a dataframe and applies zap to those records instead of the whole dataframe

#     :param df: input dataframe
#     :type df: pd.DataFrame
#     :param query: query string, see pandas documentation
#     :type query: str
#     :return: the modified dataframe
#     :rtype: pd.DataFrame
#     """
#     selection_df = select(df,query)
#     df = remove_from_df(df,selection_df)
#     zapped_df = zap(selection_df,**kwargs)
#     res_df = pd.concat([df,zapped_df],ignore_index=True)
#     res_df = sort_dataframe(res_df)
#     return res_df

##################################################################################################
##################################################################################################
##################################################################################################
# def remove_meta_data_fields(df: pd.DataFrame) -> pd.DataFrame:
#     df.drop(df[df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"])].index, inplace=True, errors='ignore')
#     return df



    #df = df.reindex(columns = df.columns.tolist() + ['min','max','mean','std','min_pos','max_pos'])

# min_delayed = [dask.delayed(np.nanmin)(df.at[i, 'd']) for i in df.index]
# max_delayed = [dask.delayed(np.nanmax)(df.at[i, 'd']) for i in df.index]
# mean_delayed = [dask.delayed(np.nanmean)(df.at[i, 'd']) for i in df.index]
# std_delayed = [dask.delayed(np.nanstd)(df.at[i, 'd']) for i in df.index]
# df['min'] = dask.compute(min_delayed)[0]
# df['max'] = dask.compute(max_delayed)[0]
# df['mean'] = dask.compute(mean_delayed)[0]
# df['std'] = dask.compute(std_delayed)[0]
# def compute_stats_dask(df:pd.DataFrame) -> pd.DataFrame:

#     df.loc[:,'min'] = None
#     df.loc[:,'max'] = None
#     df.loc[:,'mean'] = None
#     df.loc[:,'std'] = None
#     df.loc[:,'min_pos'] = None
#     df.loc[:,'max_pos'] = None
#     #add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
#     #add_empty_columns(df, ['min_pos','max_pos'],None,dtype_str='O')
#     min_delayed=[]
#     max_delayed=[]
#     mean_delayed=[]
#     std_delayed=[]
#     argmin_delayed=[]
#     #min_pos_delayed=[]
#     argmax_delayed=[]
#     #max_pos_delayed=[]
#     for i in df.index:
#         if isinstance(df.at[i, 'd'],np.ndarray):
#             df.at[i, 'd'] = da.from_array(df.at[i, 'd'])
#         min_delayed.append(dask.delayed(np.nanmin)(df.at[i, 'd']))
#         max_delayed.append(dask.delayed(np.nanmax)(df.at[i, 'd']))
#         mean_delayed.append(dask.delayed(np.nanmean)(df.at[i, 'd']))
#         std_delayed.append(dask.delayed(np.nanstd)(df.at[i, 'd']))
#         argmin_delayed.append(dask.delayed(np.nanargmin)(df.at[i, 'd']))
#         #min_pos_delayed.append(dask.delayed(np.unravel_index)(argmin_delayed, (df.at[i,'ni'],df.at[i,'nj'])))
#         argmax_delayed.append(dask.delayed(np.nanargmax)(df.at[i, 'd']))
#         #max_pos_delayed.append(dask.delayed(np.unravel_index)(argmax_delayed, (df.at[i,'ni'],df.at[i,'nj'])))
#         # max_pos_delayed = dask.delayed(np.unravel_index)(dask.delayed(np.nanargmax)(df.at[i, 'd']), (df.at[i,'ni'],df.at[i,'nj']))


#     df.loc[:,'min'] = dask.compute(min_delayed)[0]
#     df.loc[:,'max'] = dask.compute(max_delayed)[0]
#     df.loc[:,'mean'] = dask.compute(mean_delayed)[0]
#     df.loc[:,'std'] = dask.compute(std_delayed)[0]
#     df.loc[:,'min_pos'] = dask.compute(argmin_delayed)[0]
#     #argmax = dask.compute(argmax_delayed)[0]
#     df.loc[:,'max_pos'] = dask.compute(argmax_delayed)[0]
#     # max_pos = dask.compute(max_pos_delayed)[0]
#     df.loc[:,'min_pos1'] = None
#     df.loc[:,'max_pos1'] = None
#     for i in df.index:
#         df.at[i,'min_pos1'] = np.unravel_index(df.at[i,'min_pos'], (df.at[i,'ni'],df.at[i,'nj']))
#         df.at[i,'max_pos1'] = np.unravel_index(df.at[i,'max_pos'], (df.at[i,'ni'],df.at[i,'nj']))

#     df.loc[:,'min_pos'] = df['min_pos1']
#     df.loc[:,'max_pos'] = df['max_pos1']
#     #df = fstpy.dataframe.sort_dataframe(df)
#     return df

def compute_stats(df:pd.DataFrame) -> pd.DataFrame:
    df.loc[:,'min'] = None
    df.loc[:,'max'] = None
    df.loc[:,'mean'] = None
    df.loc[:,'std'] = None
    df.loc[:,'min_pos'] = (0,0)
    df.loc[:,'max_pos'] = (0,0)
    #add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
    #add_empty_columns(df, ['min_pos','max_pos'],None,dtype_str='O')
    for i in df.index:
        df.at[i,'mean'] = np.mean(df.at[i,'d'])
        df.at[i,'std'] = np.std(df.at[i,'d'])
        df.at[i,'min'] = np.min(df.at[i,'d'])
        df.at[i,'max'] = np.max(df.at[i,'d'])
        min_pos = np.unravel_index(np.argmin(df.at[i,'d']), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'min_pos'] = (min_pos[0] + 1, min_pos[1]+1)
        max_pos = np.unravel_index(np.argmax(df.at[i,'d']), (df.at[i,'ni'],df.at[i,'nj']))
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

# def validate_zap_keys(**kwargs):
#     available_keys = {'grid', 'forecast_hour', 'nomvar', 'typvar', 'etiket', 'dateo', 'datev', 'ip1', 'ip2', 'ip3', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'level', 'ip1_kind', 'ip1_pkind','unit'}
#     keys_to_modify = set(kwargs.keys())
#     acceptable_keys = keys_to_modify.intersection(available_keys)
#     if len(acceptable_keys) != len(keys_to_modify):
#         sys.stdout.write("zap - can't find modifiable key in available keys. asked for %s in %s"%(keys_to_modify,available_keys))
#         raise StandardFileError("zap - can't find modifiable key in available keys")

# def zap_ip1(df:pd.DataFrame, ip1_value:int) -> pd.DataFrame:

#     sys.stdout.write('zap - changed ip1, triggers updating level and ip1_kind\n')
#     df.loc[:,'ip1'] = ip1_value
#     df = add_ip_info_columns(df)
#     # level, ip1_kind, ip1_pkind = decode_ip(ip1_value)
#     # df.loc[:,'level'] = level
#     # df.loc[:,'ip1_kind'] = ip1_kind
#     # df.loc[:,'ip1_pkind'] = ip1_pkind
#     return df

# def zap_level(df:pd.DataFrame, level_value:float, ip1_kind_value:int) -> pd.DataFrame:
#     sys.stdout.write('zap - changed level, triggers updating ip1\n')
#     df.loc[:,'level'] = level_value
#     df.loc[:,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, level_value, ip1_kind_value)
#     return df

# def zap_ip1_kind(df:pd.DataFrame, ip1_kind_value:int) -> pd.DataFrame:
#     sys.stdout.write('zap - changed ip1_kind, triggers updating ip1 and ip1_pkind\n')
#     df.loc[:,'ip1_kind'] = ip1_kind_value
#     df.loc[:,'ip1_pkind'] = KIND_DICT[int(ip1_kind_value)]
#     for i in df.index:
#         df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], ip1_kind_value)
#     return df

# def zap_pip1_kind(df:pd.DataFrame, ip1_pkind_value:str) -> pd.DataFrame:
#     sys.stdout.write('zap - changed ip1_pkind, triggers updating ip1 and ip1_kind\n')
#     df['ip1_pkind'] = ip1_pkind_value
#     #invert ip1_kind dict
#     PKIND_DICT = {v: k for k, v in KIND_DICT.items()}
#     df['ip1_kind'] = PKIND_DICT[ip1_pkind_value]
#     for i in df.index:
#         df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], df.at[i,'ip1_kind'])
#     return df

# def zap_npas(df:pd.DataFrame, npas_value:int) -> pd.DataFrame:
#     sys.stdout.write('zap - changed npas, triggers updating forecast_hour\n')
#     df['npas'] = npas_value
#     for i in df.index:
#         df.at[i,'forecast_hour'] =  df.at[i,'deet'] * df.at[i,'npas'] / 3600.
#         df.at[i,'ip2'] = np.floor(df.at[i,'forecast_hour']).astype(int)
#     return df


# def zap_forecast_hour(df:pd.DataFrame, forecast_hour_value:int) -> pd.DataFrame:
#     sys.stdout.write('zap - changed forecast_hour, triggers updating npas\n')
#     df.loc[:,'forecast_hour'] = forecast_hour_value
#     df.loc[:,'ip2'] = np.floor(df['forecast_hour']).astype(int)
#     for i in df.index:
#         df.at[i,'npas'] = df.at[i,'forecast_hour'] * 3600. / df.at[i,'deet']
#         df.at[i,'npas'] = df.at[i,'npas'].astype(int)
#     return df


# def add_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
#     diff['abs_diff'] = None# = diff['d_x'].copy(deep=True)
#     diff['e_rel_max'] = None# = diff['d_x'].copy(deep=True)
#     diff['e_rel_moy'] = None# = diff['d_x'].copy(deep=True)
#     diff['var_a'] = None# = diff['d_x'].copy(deep=True)
#     diff['var_b'] = None# = diff['d_x'].copy(deep=True)
#     diff['c_cor'] = None# = diff['d_x'].copy(deep=True)
#     diff['moy_a'] = None# = diff['d_x'].copy(deep=True)
#     diff['moy_b'] = None# = diff['d_x'].copy(deep=True)
#     diff['bias'] = None #= diff['d_x'].copy(deep=True)
#     diff['e_max'] = None# = diff['d_x'].copy(deep=True)
#     diff['e_moy'] = None# = diff['d_x'].copy(deep=True)
#     # diff.drop(columns=['d_x', 'd_y'], inplace=True,errors='ignore')
#     return diff


# def del_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
#     #diff['etiket'] = diff['etiket_x']
#     #diff['ip1_kind'] = diff['ip1_kind_x']
#     #diff['ip2'] = diff['ip2_x']
#     #diff['ip3'] = diff['ip3_x']
#     diff.drop(columns=['abs_diff'], inplace=True,errors='ignore')
#     return diff

# def fstcomp_df(df1: pd.DataFrame, df2: pd.DataFrame, exclude_meta=False, cmp_number_of_fields=True, columns=['nomvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3','deet','npas','grtyp','ig1','ig2','ig3','ig4'], print_unmatched=False,e_max=0.0001,e_c_cor=0.00001) -> bool:
#     path1 = df1.path.unique()[0]
#     path2 = df2.path.unique()[0]

#     columns_to_keep = columns.copy()
#     columns_to_keep.extend(['d','key'])
#     df1 = df1[columns_to_keep]
#     df2 = df2[columns_to_keep]
#     #print(df1.columns)
#     #print(df2.columns)
#     # df1 = df1.sort_values(by=columns)
#     # df2 = df2.sort_values(by=columns)
#     df1 = df1.sort_values(by=columns)
#     df2 = df2.sort_values(by=columns)

#     # print('allclose= ',allclose,'exclude_meta= ',exclude_meta)
#     success = True
#     pd.options.display.float_format = '{:0.6E}'.format
#     # check if both df have records
#     if df1.empty or df2.empty:
#         sys.stderr.write('you must supply files witch contain records\n')
#         if df1.empty:
#             sys.stderr.write('file 1 is empty\n')
#         if df2.empty:
#             sys.stderr.write('file 2 is empty\n')
#         raise fstpy.StandardFileError('fstcomp - one of the files is empty\n')
#     # remove meta data {!!,>>,^^,P0,PT,HY,!!SF} from records to compare

#     if exclude_meta:
#         df1 = remove_meta_data_fields(df1)
#         df2 = remove_meta_data_fields(df2)


#     if cmp_number_of_fields:
#         if len(df1.index) != len(df2.index):
#             sys.stderr.write(f'file 1 ({len(df1.index)}) and file 2 ({len(df2.index)}) dont have the same number of records\n')
#             # sys.stderr.write(f'file 1 {df1.nomvar.tolist()}\n')
#             # sys.stderr.write(f'file 2 {df2.nomvar.tolist()}\n')
#             unique1, counts1 = np.unique(df1.nomvar.to_numpy(), return_counts=True)
#             unique2, counts2 = np.unique(df2.nomvar.to_numpy(), return_counts=True)
#             vars1 = dict(zip(unique1, counts1))
#             vars2 = dict(zip(unique2, counts2))
#             sys.stderr.write(f'file 1 {vars1}\n')
#             sys.stderr.write(f'file 2 {vars2}\n')
#             return False
#     # print(df1.to_string())
#     # print(df2.to_string())
#     # voir(df1)
#     # voir(df2)
#     # for i in df1.index:
#     #     if df1.at[i,'d'].all() != df2.at[i,'d'].all():
#     #         print(df1.at[i,'d'][:10],df2.at[i,'d'][:10])
#     #logger.debug('A',df1['d'][:100].to_string())
#     #logger.debug('A',df1.loc[i])#[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']])
#     #logger.debug('----------')
#     #logger.debug('B',df2['d'][:100].to_string())
#     #logger.debug('B',df2.loc[i])#[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']])
#     # check if they are exactly the same
#     # if df1.equals(df2):
#     #     if exclude_meta:
#     #         print('files are identical - excluding meta data fields')
#     #     else:
#     #         print('files are identical')
#     #     # logger.debug('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
#     #     # logger.debug('----------')
#     #     # logger.debug('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
#     #     return True
#     #create common fields
#     if print_unmatched:
#         print(df1[['nomvar', 'etiket','ni', 'nj', 'nk', 'dateo']])
#         print(df2[['nomvar', 'etiket','ni', 'nj', 'nk', 'dateo']])
#         print(df1[['ip1', 'ip2', 'ip3', 'deet', 'npas']])
#         print(df2[['ip1', 'ip2', 'ip3', 'deet', 'npas']])
#         print(df1[['grtyp', 'ig1', 'ig2', 'ig3', 'ig4']])
#         print(df2[['grtyp', 'ig1', 'ig2', 'ig3', 'ig4']])

#     common = pd.merge(df1, df2, how='inner', on=columns)

#     #Rows in df1 Which Are Not Available in df2
#     common_with_1 = common.merge(df1, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
#     #Rows in df2 Which Are Not Available in df1
#     common_with_2 = common.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
#     missing = pd.concat([common_with_1, common_with_2],ignore_index=True)

#     if exclude_meta:
#         missing = remove_meta_data_fields(missing)

#     if len(common.index) != 0:
#         if len(common_with_1.index) != 0:
#             if print_unmatched:
#                 sys.stdout.write('df in file 1 that were not found in file 2 - excluded from comparison\n')
#                 sys.stdout.write('%s\n'%common_with_1.to_string())
#         if len(common_with_2.index) != 0:
#             if print_unmatched:
#                 sys.stdout.write('df in file 2 that were not found in file 1 - excluded from comparison\n')
#                 sys.stdout.write('%s\n'%common_with_2.to_string())
#     else:
#         sys.stderr.write('fstcomp - no common df to compare\n')
#         if not df1.empty:
#             sys.stderr.write('A %s\n'%df1[['nomvar', 'etiket','ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].reset_index(drop=True).to_string())
#         sys.stderr.write('----------\n')
#         if not df2.empty:
#             sys.stderr.write('B %s\n'%df2[['nomvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].reset_index(drop=True).to_string())
#         raise FstCompError('fstcomp - no common df to compare')


#     # force free some memory
#     del df1
#     del df2
#     # print(path1,path2)
#     diff = common
#     # print(diff.columns)
#     # diff = common.copy()
#     #voir(diff)
#     diff = add_fstcomp_columns(diff)

#     diff,success = compute_fstcomp_stats(diff,path1,path2,e_max,e_c_cor)

#     diff = del_fstcomp_columns(diff)

#     # diff.sort_values(by=['nomvar','etiket','ip1'],inplace=True)
#     if len(diff.index):
#         sys.stdout.write('%s\n'%diff[['nomvar', 'etiket', 'ip1', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'bias', 'e_max', 'e_moy']].to_string(formatters={'level': '{:,.6f}'.format,'diff_percent': '{:,.1f}%'.format}))
#         #logger.debug(diff[['nomvar', 'etiket', 'ip1_pkind', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'bias', 'e_max', 'e_moy']].to_string())
#     if len(missing.index):
#         sys.stdout.write('missing df\n')
#         sys.stdout.write('%s\n'%missing[['nomvar', 'etiket', 'ip1', 'ip2', 'ip3']].to_string(header=False, formatters={'level': '{:,.6f}'.format}))
#         return False
#         #logger.debug(missing[['nomvar', 'etiket', 'ip1_pkind', 'ip2', 'ip3']].to_string(header=False))
#     return success

# def calc_derr(a,b):
#     if (a == 0.) and (b != 0.):
#         return abs(a-b)/abs(b)
#     elif (b == 0.) and (a != 0.):
#         return abs(a-b)/abs(a)
#     else:
#         return 0.

# def calc_derr(ar,br):
#     # np.where(ar == 0,abs(1-(ar/br)),abs(1-(br/ar)))
# #     print(ar,br,sep='\t')
#     if ar == 0:
#        return np.abs(1-(ar/br))
#     return np.abs(1-(br/ar))

# ligne 441

# def compute_fstcomp_stats(diff: pd.DataFrame,path1:str,path2:str,e_max=0.0001,e_c_cor=0.00001) -> bool:
#     success = True

#     df_list = np.array_split(diff, math.ceil(len(diff.index)/100))
#     for df in df_list:
#         iun1 = rmn.fstopenall(path1)
#         for i in df.index:
#             df.at[i,'d_x'] = rmn.fstluk(int(df.at[i,'key_x']))['d']
#         rmn.fstcloseall(iun1)
#         iun2 = rmn.fstopenall(path2)
#         for i in df.index:
#             df.at[i,'d_y'] = rmn.fstluk(int(df.at[i,'key_y']))['d']
#         rmn.fstcloseall(iun2)

#         print('diff dx\n',df[['nomvar','d_x']])
#         print('diff dy\n',df[['nomvar','d_y']])
#         to_drop = []
#         for i in df.index:
#             # a = df.at[i, 'd_x'].astype('float64')
#             # b = df.at[i, 'd_y'].astype('float64')
#             a = np.array(df.at[i, 'd_x'].ravel(),dtype=np.float32,order='F')
#             b = np.array(df.at[i, 'd_y'].ravel(),dtype=np.float32,order='F')
#             if np.allclose(a,b):
#                 to_drop.append(i)
#                 continue
#             # e_rel_max = 0.
#             # e_rel_moy = 0.
#             # var_a = 0.
#             # var_b = 0.
#             # c_cor = 0.
#             # moy_a = 0.
#             # moy_b = 0.
#             # bias = 0.
#             # e_max = 0.
#             # e_moy = 0.

#             n = a.size
#             # fstcompstats(a,b,n,errrelmax,errrelmoy,vara,varb,ccor,moya,moyb,bias,errmax,errmoy)
#             df.at[i, 'e_rel_max'], df.at[i, 'e_rel_moy'], df.at[i, 'var_a'], df.at[i, 'var_b'], df.at[i, 'c_cor'], df.at[i, 'moy_a'], df.at[i, 'moy_b'], df.at[i, 'bias'], df.at[i, 'e_max'], df.at[i, 'e_moy'] = fstcompstats(a=a, b=b, n=n)#, errrelmax=e_rel_max, errrelmoy=e_rel_moy, vara=var_a, varb=var_b, ccor=c_cor, moya=moy_a, moyb=moy_b, bias=bias, errmax=e_max, errmoy=e_moy)
#             # print(e_rel_max, e_rel_moy, var_a, var_b, c_cor, moy_a, moy_b, bias, e_max, e_moy)
#             # print(r)
#             # a = df.at[i, 'd_x'].flatten().astype('float64')
#             # b = df.at[i, 'd_y'].flatten().astype('float64')

#             # df.at[i, 'abs_diff'] = np.abs(a-b)

#             # # verror_rel = np.vectorize(calc_derr)

#             # # derr = verror_rel(a1,b1)
#             # # np.abs(1-(a/b)), np.where((b == 0) & (a != 0), np.abs(1-(b/a)), 0))
#             # derr = np.full_like(a,0)
#             # derr = np.where(a!=0,np.abs(1-(b/a)),np.where(b!=0,np.abs(1-(a/b)),derr))


#             # # derr = np.where(derr==1,0,derr)

#             # df.at[i, 'e_rel_max'] = np.max(derr)
#             # df.at[i, 'e_rel_moy'] = np.mean(derr)

#             # sum_a2 = np.sum(a**2)
#             # sum_b2 = np.sum(b**2)
#             # # print(np.mean(sum_a2) == np.var(a))
#             # # print(np.mean(sum_b2) == np.var(b))
#             # df.at[i, 'var_a'] = np.var(a)
#             # df.at[i, 'var_b'] = np.var(b)
#             # df.at[i, 'moy_a'] = np.mean(a)
#             # df.at[i, 'moy_b'] = np.mean(b)

#             # c_cor = np.sum(a*b)
#             # if sum_a2*sum_b2 != 0:
#             #     c_cor = c_cor/np.sqrt(sum_a2*sum_b2)
#             # elif (sum_a2==0) and (sum_b2==0):
#             #     c_cor = 1.0
#             # elif sum_a2 == 0:
#             #     c_cor = np.sqrt(df.at[i, 'var_b'])
#             # else:
#             #     c_cor = np.sqrt(df.at[i, 'var_a'])
#             # df.at[i, 'c_cor'] = c_cor
#             # # df.at[i, 'c_cor'] = np.correlate(a,b)[0]
#             # df.at[i, 'bias']=(df.at[i, 'moy_b']-df.at[i, 'moy_a'])
#             # df.at[i, 'e_max'] = np.max(df.at[i, 'abs_diff'])
#             # df.at[i, 'e_moy'] = np.mean(df.at[i, 'abs_diff'])

#             # print(df.at[i, 'e_rel_max'], df.at[i, 'e_rel_moy'], df.at[i, 'var_a'], df.at[i, 'var_b'], df.at[i, 'c_cor'], df.at[i, 'moy_a'], df.at[i, 'moy_b'], df.at[i, 'bias'], df.at[i, 'e_max'], df.at[i, 'e_moy'])
#             # # nbdiff = np.count_nonzero(a!=b)
#             # # df.at[i, 'diff_percent'] = nbdiff / a.size * 100.0
#             # # print(df.at[i, 'c_cor'],1.0,1.0+1e-07,1-1e-07,math.isclose(df.at[i, 'c_cor'],1.0,rel_tol=1e-07))

#             # if ((mod(datypa,128) != 1) and (mod(datypa,128) != 6)):
#             #     PACK_ERR2 = 0
#             # else:
#             #     PACK_ERR2 = PACK_ERR

#             # ERR_UNIT = (MAX_A - MIN_A) / (2.0 ** NBITS)

#             # if ((df.at[i, 'e_rel_max'] > e_max) and (PACK_ERR!=0)):
#             #     # found err true
#             #     pass

#             # if (PACK_ERR > 0) and (df.at[i, 'e_max'] > (PACK_ERR*ERR_UNIT*1.001)):
#             #     # found err true
#             #      pass



#             if (not (-e_c_cor <= abs(df.at[i, 'c_cor']-1.0) <= e_c_cor)) or (not (-e_max <= df.at[i, 'e_max'] <= e_max)):
#                 df.at[i, 'nomvar'] = '<' + df.at[i, 'nomvar'] + '>'
#                 # print('maximum absolute difference:%s'%np.max(np.abs(a-b)))
#                 # print(np.abs(a-b))
#                 success = False
#             # print('dataframe\n',df)
#         df['d_x']=None
#         df['d_y']=None
#         df.drop(to_drop,inplace=True)
#     diff = pd.concat(df_list,ignore_index=True)
#     return diff,success

class SelectError(Exception):
    pass

def select_meta(df:pd.DataFrame) -> pd.DataFrame:
    meta_df = df.loc[df.nomvar.isin(["!!","P0","PT",">>","^^","^>","HY","!!SF"])]
    return meta_df



    #    kind can take the following values
    #    0, p est en hauteur (m) rel. au niveau de la mer (-20, 000 -> 100, 000)
    #    1, p est en sigma                                (0.0 -> 1.0)
    #    2, p est en pression (mb)                        (0 -> 1100)
    #    3, p est un code arbitraire                      (-4.8e8 -> 1.0e10)
    #    4, p est en hauteur (M) rel. au niveau du sol    (-20, 000 -> 100, 000)
    #    5, p est en coordonnee hybride                   (0.0 -> 1.0)
    #    6, p est en coordonnee theta                     (1 -> 200, 000)
    #    10, p represente le temps en heure               (0.0 -> 1.0e10)
    #    15, reserve (entiers)
    #    17, p represente l'indice x de la matrice de conversion (1.0 -> 1.0e10)
    #        (partage avec kind=1 a cause du range exclusif
    #    21, p est en metres-pression                     (0 -> 1, 000, 000)
    #                                                     fact=1e4
    #        (partage avec kind=5 a cause du range exclusif)


def get_kinds_and_ip1(df:pd.DataFrame) -> dict:
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

def get_ips(df:pd.DataFrame,sigma=False,hybrid=False,pressure=False) -> list:
    kinds = get_kinds_and_ip1(df)
    # print(kinds)
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

def get_model_ips(df:pd.DataFrame) -> list:
    return get_ips(df,sigma=True,hybrid=True)
    # kinds = get_kinds_and_ip1(df)

    # ip1_list = []
    # if 1 in kinds.keys():
    #     ip1_list.append(kinds[1])
    # if 5 in kinds.keys():
    #     ip1_list.append(kinds[5])
    # return ip1_list

def get_sigma_ips(df:pd.DataFrame) -> list:
    return get_ips(df,sigma=True)

def get_pressure_ips(df:pd.DataFrame) -> list:
    return get_ips(df,pressure=True)

def get_hybrid_ips(df:pd.DataFrame) -> list:
    return get_ips(df,hybrid=True)
    # kinds = get_kinds_and_ip1(df)
    # ip1_list = []
    # if 5 in kinds.keys():
    #     ip1_list.append(kinds[5])
    # return ip1_list

# def get_model_ips(df:pd.DataFrame) -> list:
#     ip1s = df.ip1.unique()
#     ip1_list = []
#     for ip1 in ip1s:
#         (_, kind) = rmn.convertIp(rmn.CONVIP_DECODE, int(ip1))
#         if (kind == 1) or (kind == 5):
#             ip1_list.append(ip1)
#     return ip1_list

# def get_sigma_ips(df:pd.DataFrame) -> list:
#     ip1s = df.ip1.unique()
#     ip1_list = []
#     for ip1 in ip1s:
#         (_, kind) = rmn.convertIp(rmn.CONVIP_DECODE, int(ip1))
#         if (kind == 1):
#             ip1_list.append(ip1)
#     return ip1_list

# def get_hybrid_ips(df:pd.DataFrame) -> list:
#     ip1s = df.ip1.unique()
#     ip1_list = []
#     for ip1 in ip1s:
#         (_, kind) = rmn.convertIp(rmn.CONVIP_DECODE, int(ip1))
#         if (kind == 5):
#             ip1_list.append(ip1)
#     return ip1_list


# toctoc without hybrid
# 2,True,True,False,False,False,False,2001,PRESSURE
# 1,True,True,False,False,False,False,1002,ETA
# 1,True,True,False,False,False,False,1001,SIGMA



    # vcode 2001 -> Pressure levels
    # vcode 4001 -> Heights with respect to surface level, may be in ocean

def get_toctoc_fields(df:pd.DataFrame,hybrid_ips:list,sigma_ips:list,pressure_ips:list,strict=True):
    # print('hybrid_ips:',hybrid_ips,'sigma_ips:',sigma_ips,'pressure_ips:',pressure_ips)
    df_list = []

    hybrid_fields_df = pd.DataFrame(dtype=object)
    # hybrid
    if len(hybrid_ips):
        # hybrid_fields_df = df.query(f'ip1 in {hybrid_ips}')
        hybrid_fields_df = df.loc[df.ip1.isin(hybrid_ips)]

    hybrid_grids = []
    if not hybrid_fields_df.empty:
        hybrid_grids = hybrid_fields_df.grid.unique()

    # sigma
    sigma_fields_df = pd.DataFrame(dtype=object)
    if len(sigma_ips):
        # sigma_fields_df = df.query(f'ip1 in {sigma_ips}')
        sigma_fields_df = df.loc[df.ip1.isin(sigma_ips)]

    sigma_grids = []
    if not sigma_fields_df.empty:
        sigma_grids = sigma_fields_df.grid.unique()

    # pressure
    pressure_fields_df = pd.DataFrame(dtype=object)
    if len(pressure_ips):
        # pressure_fields_df = df.query(f'ip1 in {pressure_ips}')
        pressure_fields_df = df.loc[df.ip1.isin(pressure_ips)]

    pressure_grids = []
    if not pressure_fields_df.empty:
        pressure_grids = pressure_fields_df.grid.unique()

    # vcode 1003 -> Hybrid normalized levels
    # vcode 5001 -> Hybrid levels, unstaggered
    # vcode 5002 -> Hybrid staggered levels, with extra thermo level at top
    # vcode 5003 -> Like 5002 but tlift (not used much)
    # vcode 5004 -> Hybrid staggered levels, no extra thermo level at top
    # vcode 5005 -> Hybrid staggered levels, no extra thermo level at top, diag levels encoded as m AGL
    # vcode 5100 -> Hybrid staggered SLEVE like coordinate
    # vcode 5999 -> Hybrid unstaggered coordinate of unknown/other origin (like ECMWF)
    # vcode 21001 -> Hybrid heights levels on Charney-Philips grid, may be non SLEVE (reference field: ME) or SLEVE (reference fields: ME, MELS)
    # vcode 21002 -> Hybrid heights levels on Lorenz grid, may be non SLEVE (reference field: ME) or SLEVE (reference fields: ME, MELS)
    for grid in hybrid_grids:
        # toctoc_df = df.query(f'(nomvar=="!!") and (grid=="{grid}") and (ig1 in [1003,5001,5002,5003,5004,5005,5100,5999,21001,21002])')
        toctoc_df = df.loc[(df.nomvar=="!!") & (df.grid==grid) & (df.ig1.isin([1003,5001,5002,5003,5004,5005,5100,5999,21001,21002]))]
        if not toctoc_df.empty:
            df_list.append(toctoc_df)

    # vcode 1001 -> Sigma levels
    # vcode 1002 -> Eta levels
    for grid in sigma_grids:
        # toctoc_df = df.query(f'(nomvar=="!!") and (grid=="{grid}") and (ig1 in [1001,1002])')
        toctoc_df = df.loc[(df.nomvar=="!!") & (df.grid==grid) & (df.ig1.isin([1001,1002]))]
        if not toctoc_df.empty:
            df_list.append(toctoc_df)

    # vcode 2001 -> Pressure levels
    for grid in pressure_grids:
        # toctoc_df = df.query(f'(nomvar=="!!") and (grid=="{grid}") and (ig1==2001)')
        toctoc_df = df.loc[(df.nomvar=="!!") & (df.grid==grid) & (df.ig1==2001)]
        if not toctoc_df.empty:
            df_list.append(toctoc_df)


    # print('df_list\n',df_list)
    toctoc_fields_df = pd.DataFrame(dtype=object)
    if len(df_list):
        toctoc_fields_df = pd.concat(df_list,ignore_index=True)

    toctoc_fields_df = toctoc_fields_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], ignore_index=True)

    return toctoc_fields_df

def get_hy_field(df:pd.DataFrame,hybrid_ips:list):

    hy_field_df = pd.DataFrame(dtype=object)
    if len(hybrid_ips):
        # hy_field_df = df.query(f'nomvar=="HY"')
        hy_field_df = df.loc[df.nomvar=="HY"]

    hy_field_df = hy_field_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'], ignore_index=True)

    return hy_field_df
    # if len(hybrid_ips):
    #     df.query(f'ip=="HY"')

    #     #par defaut
    #     else if ( mpds->second->getPdsName() == "!!")

    #         # only keep !! if we have at least one pds with a vcode equal to the ig1 value of the !!
    #         int toctocVcode
    #         LEXICALCAST(mpds->second->getOtherInformation().ig1,toctocVcode)
    #         keep = hasPdsWithVcode(toctocVcode)

    #     # active par variable globale
    #     else if ( mpds->second->getPdsName() == "!!")

    #         # only keep !! if we have at least one pds with a vcode equal to the ig1 value of the !!
    #         int toctocVcode
    #         LEXICALCAST(mpds->second->getOtherInformation().ig1,toctocVcode)
    #         keep = hasPdsWithVcode(toctocVcode)
def get_grid_deformation_fileds(df:pd.DataFrame,no_meta_df:pd.DataFrame):
    grid_deformation_fields_df = pd.DataFrame(dtype=object)

    groups = no_meta_df.groupby(['grid','dateo','deet','npas'])

    df_list = []
    hasDate = False
    for _,group in groups:
        grid = group.grid.unique()[0]
        dateo = group.dateo.unique()[0]
        lat_df = df.loc[(df.nomvar=="^^") & (df.grid==grid) & (df.dateo==dateo)]
        if lat_df.empty:
            lat_df = df.loc[(df.nomvar=="^^") & (df.grid==grid)]

        lon_df = df.loc[(df.nomvar==">>") & (df.grid==grid) & (df.dateo==dateo)]
        if lon_df.empty:
            lon_df = df.loc[(df.nomvar==">>") & (df.grid==grid)]

        tictac_df = df.loc[(df.nomvar=="^>") & (df.grid==grid) & (df.dateo==dateo)]
        if tictac_df.empty:
            tictac_df = df.loc[(df.nomvar=="^>") & (df.grid==grid)]

        if not lat_df.empty:
            hasDate = ((lat_df.deet.unique()[0]!=0))
        if not lon_df.empty:
            hasDate = ((lon_df.deet.unique()[0]!=0))
        if not tictac_df.empty:
            hasDate = ((tictac_df.deet.unique()[0]!=0))

        if hasDate:
            deet = group.deet.unique()[0]
            npas = group.npas.unique()[0]
            df_list.append(df.loc[(df.nomvar==">>") & (df.grid==grid) & (df.deet==deet) & (df.npas==npas)])
            df_list.append(df.loc[(df.nomvar=="^^") & (df.grid==grid) & (df.deet==deet) & (df.npas==npas)])
            df_list.append(df.loc[(df.nomvar=="^>") & (df.grid==grid) & (df.deet==deet) & (df.npas==npas)])
        else:
            df_list.append(lat_df)
            df_list.append(lon_df)
            df_list.append(tictac_df)

    if len(df_list):
        grid_deformation_fields_df = pd.concat(df_list,ignore_index=True)
    if hasDate:
        grid_deformation_fields_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datyp','deet','npas','dateo','datev'],inplace=True, ignore_index=True)
    else:
        grid_deformation_fields_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datyp'],inplace=True, ignore_index=True)
    return grid_deformation_fields_df

def get_p0_fields(df:pd.DataFrame,no_meta_df:pd.DataFrame,hybrid_ips:list,sigma_ips:list):
    p0_fields_df = pd.DataFrame(dtype=object)
    # print('hybrid_ips',hybrid_ips)
    # print('sigma_ips',sigma_ips)
    hybrid_grids = set()
    for ip1 in hybrid_ips:
        # hybrid_grids.add(no_meta_df.query(f'ip1=={ip1}').iloc[0]['grid'])
        hybrid_grids.add(no_meta_df.loc[no_meta_df.ip1==ip1].iloc[0]['grid'])

    df_list = []
    for grid in hybrid_grids:
        # print(grid)
        # df_list.append(df.query(f'(nomvar=="P0") and (grid=="{grid}")'))
        df_list.append(df.loc[(df.nomvar=="P0") & (df.grid==grid)])

    if len(df_list):
        p0_fields_df = pd.concat(df_list,ignore_index=True)

    sigma_grids = set()
    for ip1 in sigma_ips:
        # sigma_grids.add(no_meta_df.query(f'ip1=={ip1}').iloc[0]['grid'])
        sigma_grids.add(no_meta_df.loc[no_meta_df.ip1==ip1].iloc[0]['grid'])

    df_list = []
    for grid in sigma_grids:
        # print(grid)
        # df_list.append(df.query(f'(nomvar=="P0") and (grid=="{grid}")'))
        df_list.append(df.loc[(df.nomvar=="P0") & (df.grid==grid)])

    if len(df_list):
        p0_fields_df = pd.concat(df_list,ignore_index=True)

    p0_fields_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'],inplace=True, ignore_index=True)
    # print('p0_fields_df',p0_fields_df)
    return p0_fields_df

def get_pt_fields(df:pd.DataFrame,no_meta_df:pd.DataFrame,sigma_ips:list):
    pt_fields_df = pd.DataFrame(dtype=object)

    sigma_grids = set()
    for ip1 in sigma_ips:
        # sigma_grids.add(no_meta_df.query(f'ip1=={ip1}').iloc[0]['grid'])
        sigma_grids.add(no_meta_df.loc[no_meta_df.ip1==ip1].iloc[0]['grid'])

    # print(sigma_grids)
    df_list = []
    for grid in list(sigma_grids):
        # print('checking for pt\n',df.query(f'(nomvar=="PT") and (grid=="{grid}")'))
        # df_list.append(df.query(f'(nomvar=="PT") and (grid=="{grid}")'))
        df_list.append(df.loc[(df.nomvar=="PT") & (df.grid==grid)])

    if len(df_list):
        pt_fields_df = pd.concat(df_list,ignore_index=True)

    pt_fields_df.drop_duplicates(subset=['grtyp','nomvar','typvar','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'],inplace=True, ignore_index=True)

    return pt_fields_df
