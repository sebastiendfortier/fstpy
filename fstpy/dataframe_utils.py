# -*- coding: utf-8 -*-
import fstpy
import pandas as pd
from .logger_config import logger
from .dataframe import remove_meta_data_fields
from .exceptions import StandardFileError, SelectError
import numpy as np
import rpnpy.librmn.all as rmn

def fstcomp(file1:str, file2:str, columns=['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], verbose=False) -> bool:
    from .std_reader import load_data,StandardFileReader
    logger.info('fstcomp -A %s -B %s'%(file1,file2))
    import os
    if not os.path.exists(file1):
        logger.error('fstcomp - %s does not exist' % file1)
        raise StandardFileError('fstcomp - %s does not exist' % file1)
    if not os.path.exists(file2):
        logger.error('fstcomp - %s does not exist' % file2)
        raise StandardFileError('fstcomp - %s does not exist' % file2)    
    # open and read files
    df1 = StandardFileReader(file1).to_pandas()
    df1 = load_data(df1)
    df2 = StandardFileReader(file2).to_pandas()
    df2 = load_data(df2)
    return fstcomp_df(df1, df2, columns, print_unmatched=True if verbose else False)

def voir(df:pd.DataFrame):
    from .utils import validate_df_not_empty
    import sys
    """Displays the metadata of the supplied records in the rpn voir format"""
    validate_df_not_empty(df,'voir',StandardFileError)
    #logger.debug('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    sys.stdout.writelines(df.sort_values(by=['nomvar']).reset_index(drop=True).to_string(header=True))




def zap(df:pd.DataFrame, mark:bool=True, validate_keys=True,**kwargs:dict ) -> pd.DataFrame:
    from .utils import validate_df_not_empty
    from .dataframe import sort_dataframe
    """ Modifies records from the input file or other supplied records according to specific criteria
        kwargs: can contain these key, value pairs to select specific records from the input file or input records
                nomvar=str
                typvar=str
                etiket=str
                dateo=int
                datev=int
                ip1=int
                ip2=int
                ip3=int
                deet=int
                npas=int
                datyp=int
                nbits=int
                grtyp=str
                ig1=int
                ig2=int
                ig3=int
                ig4=int """
    validate_df_not_empty(df,'zap',StandardFileError)            
    if validate_zap_keys:
        validate_zap_keys(**kwargs)

    logger.info('zap - ' + str(kwargs)[0:100] + '...')

    res_df = create_load_data_info(df)
    res_df.loc[:,'dirty'] = True
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
    if mark:
        res_df.loc[:,'typvar'] = res_df['typvar'].str.cat([ 'Z' for x in res_df.index])
    res_df = sort_dataframe(res_df) 
    return res_df

def fststat(df:pd.DataFrame):
    from .utils import validate_df_not_empty
    from .std_reader import load_data
    """ reads the data from the supplied records and calculates then displays the statistics for that record """
    logger.info('fststat')
    pd.options.display.float_format = '{:0.6E}'.format
    validate_df_not_empty(df,'fststat',StandardFileError)
    df = load_data(df)
    df = compute_stats(df)
    print(df[['nomvar','typvar','level','ip1_pkind','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string(formatters={'level':'{:,.6f}'.format}))

def select(df:pd.DataFrame, query_str:str, exclude:bool=False, no_meta_data=False, loose_match=False, no_fail=False, engine=None) -> pd.DataFrame:
    from .dataframe import sort_dataframe
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
        columns.remove('fstinl_params')
        tmp_df = pd.concat([df, tmp_df]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df) 
    return tmp_df


def select_zap(df:pd.DataFrame, query:str, **kwargs:dict) -> pd.DataFrame:
    from .dataframe import remove_from_df,sort_dataframe
    selection_df = select(df,query)
    df = remove_from_df(df,selection_df)
    zapped_df = zap(selection_df,mark=False,**kwargs)
    res_df = pd.concat([df,zapped_df])
    res_df = sort_dataframe(res_df)
    return res_df

##################################################################################################
##################################################################################################
##################################################################################################
def add_empty_columns(df, columns, init, dtype_str):
    for col in columns:
        df.insert(len(df.columns),col,init)
        df = df.astype({col:dtype_str})
    return df         
    #df = df.reindex(columns = df.columns.tolist() + ['min','max','mean','std','min_pos','max_pos'])   

# min_delayed = [dask.delayed(np.min)(df.at[i, 'd']) for i in df.index]
# max_delayed = [dask.delayed(np.max)(df.at[i, 'd']) for i in df.index]
# mean_delayed = [dask.delayed(np.mean)(df.at[i, 'd']) for i in df.index]
# std_delayed = [dask.delayed(np.std)(df.at[i, 'd']) for i in df.index]
# df['min'] = dask.compute(min_delayed)[0]
# df['max'] = dask.compute(max_delayed)[0]
# df['mean'] = dask.compute(mean_delayed)[0]
# df['std'] = dask.compute(std_delayed)[0]

def compute_stats(df:pd.DataFrame) -> pd.DataFrame:
    add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
    initializer = (np.nan,np.nan)
    add_empty_columns(df, ['min_pos','max_pos'],None)
    for i in df.index:
        df.at[i,'mean'] = df.at[i,'d'].mean()
        df.at[i,'std'] = df.at[i,'d'].std()
        df.at[i,'min'] = df.at[i,'d'].min()
        df.at[i,'max'] = df.at[i,'d'].max()
        df.at[i,'min_pos'] = np.unravel_index(df.at[i,'d'].argmin(), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'min_pos'] = (df.at[i,'min_pos'][0] + 1, df.at[i,'min_pos'][1]+1)
        df.at[i,'max_pos'] = np.unravel_index(df.at[i,'d'].argmax(), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'max_pos'] = (df.at[i,'max_pos'][0] + 1, df.at[i,'max_pos'][1]+1)
    #df = fstpy.dataframe.sort_dataframe(df)    
    return df

def create_load_data_info(df:pd.DataFrame) -> pd.DataFrame:
    for i in df.index:
        if df.at[i,'d'] is None == False:
            return df
        if df.at[i,'key'] != None:
            fstinl_params={
            'etiket':df.at[i,'etiket'],
            'datev':df.at[i,'datev'],
            'ip1':df.at[i,'ip1'],
            'ip2':df.at[i,'ip2'],
            'ip3':df.at[i,'ip3'],
            'typvar':df.at[i,'typvar'],
            'nomvar':df.at[i,'nomvar']}
            df.at[i,'fstinl_params'] = fstinl_params
    return df

def validate_zap_keys(**kwargs):
    available_keys = {'grid', 'forecast_hour', 'nomvar', 'typvar', 'etiket', 'dateo', 'datev', 'ip1', 'ip2', 'ip3', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'level', 'ip1_kind', 'ip1_pkind','unit'}
    keys_to_modify = set(kwargs.keys())
    acceptable_keys = keys_to_modify.intersection(available_keys)
    if len(acceptable_keys) != len(keys_to_modify):
        logger.warning("zap - can't find modifiable key in available keys. asked for %s in %s"%(keys_to_modify,available_keys))
        raise StandardFileError("zap - can't find modifiable key in available keys")

def zap_ip1(df:pd.DataFrame, ip1_value:int) -> pd.DataFrame:
    from .std_dec import decode_ip1
    from .constants import KIND_DICT
    logger.warning('zap - changed ip1, triggers updating level and ip1_kind')
    df.loc[:,'ip1'] = ip1_value
    level, ip1_kind, ip1_pkind = decode_ip1(ip1_value)
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
    from .constants import KIND_DICT
    logger.warning('zap - changed ip1_kind, triggers updating ip1 and ip1_pkind')
    df['ip1_kind'] = ip1_kind_value
    df['ip1_pkind'] = KIND_DICT[int(ip1_kind_value)]
    for i in df.index:
        df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], ip1_kind_value)
    return df

def zap_pip1_kind(df:pd.DataFrame, ip1_pkind_value:str) -> pd.DataFrame:
    from .constants import KIND_DICT
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
    diff.drop(columns=['d_x', 'd_y'], inplace=True)
    return diff


def del_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
    diff['etiket'] = diff['etiket_x']
    #diff['ip1_kind'] = diff['ip1_kind_x']
    #diff['ip2'] = diff['ip2_x']
    #diff['ip3'] = diff['ip3_x']
    diff.drop(columns=['abs_diff'], inplace=True)
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
    # logger.debug('A',df1['d'][:100].to_string())
    # logger.debug('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
    # logger.debug('----------')
    # logger.debug('B',df2['d'][:100].to_string())
    # logger.debug('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())    
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
    from math import isnan
    success = True

    for i in common.index:
        a = common.at[i, 'd_x'].flatten()
        b = common.at[i, 'd_y'].flatten()
        diff.at[i, 'abs_diff'] = np.abs(a-b)

        derr = np.where(a == 0, np.abs(1-a/b), np.abs(1-b/a))
        derr_sum=np.sum(derr)
        if isnan(derr_sum):
            diff.at[i, 'e_rel_max'] = 0.
            diff.at[i, 'e_rel_moy'] = 0.
        else:    
            diff.at[i, 'e_rel_max'] = 0. if isnan(np.nanmax(derr)) else np.nanmax(derr)
            diff.at[i, 'e_rel_moy'] = 0. if isnan(np.nanmean(derr)) else np.nanmean(derr)
        sum_a2 = np.sum(a**2)
        sum_b2 = np.sum(b**2)
        diff.at[i, 'var_a'] = np.mean(sum_a2)
        diff.at[i, 'var_b'] = np.mean(sum_b2)
        diff.at[i, 'moy_a'] = np.mean(a)
        diff.at[i, 'moy_b'] = np.mean(b)
        
        c_cor = np.sum(a*b)
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
        diff.at[i, 'e_max'] = np.max(diff.at[i, 'abs_diff'])
        diff.at[i, 'e_moy'] = np.mean(diff.at[i, 'abs_diff'])
        
        nbdiff = np.count_nonzero(a!=b)
        diff.at[i, 'diff_percent'] = nbdiff / a.size * 100.0
        if not ((-1.000001 <= diff.at[i, 'c_cor'] <= 1.000001) and (-0.1 <= diff.at[i, 'e_rel_max'] <= 0.1) and (-0.1 <= diff.at[i, 'e_rel_moy'] <= 0.1)):
            diff.at[i, 'nomvar'] = '<' + diff.at[i, 'nomvar'] + '>'
            success = False
    return success            