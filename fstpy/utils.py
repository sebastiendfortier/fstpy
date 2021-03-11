# -*- coding: utf-8 -*-
import inspect
from functools import wraps

import pandas as pd
import rpnpy.librmn.all as rmn

from .logger_config import logger


def initializer(func):
    """
    Automatically assigns the parameters.

    >>> class process:
    ...     @initializer
    ...     def __init__(self, cmd, reachable=False, user='root'):
    ...         pass
    >>> p = process('halt', True)
    >>> p.cmd, p.reachable, p.user
    ('halt', True, 'root')
    """
    names, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, annotations = inspect.getfullargspec(func)
    #names, varargs, keywords, defaults = inspect.getfullargspec(func)

    @wraps(func)
    def wrapper(self, *args, **kargs):
        for name, arg in list(zip(names[1:], args)) + list(kargs.items()):
            setattr(self, name, arg)

        for name, default in zip(reversed(names), reversed(defaults)):
            if not hasattr(self, name):
                setattr(self, name, default)

        func(self, *args, **kargs)

    return wrapper

def delete_file(my_file):
    import os
    if os.path.exists(my_file):
        os.unlink(my_file)
        
def get_grid_groups(groups:list) -> list:
    return make_groups(groups, 'grid', ['forecast_hour','nomvar','level'])

def get_forecast_hour_groups(groups:list) -> list:
    return make_groups(groups, 'forecast_hour', ['forecast_hour','nomvar','level'])

def get_nomvar_groups(groups:list) -> list:
    return make_groups(groups, 'nomvar', ['forecast_hour','nomvar','level'])

def get_level_groups(groups:list) -> list:
    return make_groups(groups, 'level', ['forecast_hour','nomvar'])

def make_groups(groups:list, group_by_attribute, sort_by_attributes:list) -> list:
    groupdfs = []
    for group in groups:
        sub_groups = group.groupby(getattr(group,group_by_attribute))
        for _, sub_group in sub_groups:
            groupdfs.append(sub_group.sort_values(by=sort_by_attributes)) 
    return  groupdfs

def get_groups(df:pd.DataFrame, group_by_forecast_hour:bool=False,group_by_level=True) -> list:
    # create grid group
    grid_groups = get_grid_groups([df])
    if (group_by_forecast_hour == False) and (group_by_level == False):
        return grid_groups

    forecast_hour_groups= get_forecast_hour_groups(grid_groups)
    if (group_by_forecast_hour == True) and (group_by_level == False):
        return forecast_hour_groups
            
    if (group_by_forecast_hour == False) and (group_by_level == True):
        return get_level_groups(grid_groups)
    
    if (group_by_forecast_hour == True) and (group_by_level == True):
        return get_level_groups(forecast_hour_groups)


def flatten_data_series(df) -> pd.DataFrame:
    import sys
    if len(df.nomvar.unique()) > 1:
        sys.stderr.write('more than one variable, stacking the arrays would not yield a 3d array for one variable - no modifications made\n')
        return df
    for i in df.index:
        df.at[i,'d'] = df.at[i,'d'].flatten()
    return df    

def create_1row_df_from_model(df:pd.DataFrame) -> pd.DataFrame:
    import sys
    if len(df.nomvar.unique()) > 1:
        sys.stderr.write('more than one variable, returning a dataframe based on first row\n')
        return df
    res_d = df.iloc[0].to_dict()
    res_df = pd.DataFrame([res_d])
    #print(res_df)
    #res_df['fstinl_params'] = None
    res_df['file_modification_time'] = None
    res_df['key'] = None
    return res_df

def validate_nomvar(nomvar, caller_class, error_class):
    if len(nomvar) > 4:
        raise error_class(caller_class + ' - max 4 char for nomvar')

def validate_df_not_empty(df, caller_class, error_class):
    if df.empty:
        logger.error(caller_class + ' - no records to process')
        raise error_class(caller_class + ' - no records to process')    


def get_file_list(pattern):
    import glob
    files = glob.glob(pattern)
    return files

def ip1_from_level_and_kind(level:float,kind:str):
    d = {
        'm':0,
        'sg':1,
        'mb':2,
        'M':4,
        'hy':5,
        'th':6,
        'H':10,
        'mp':21
    }
    pk =  rmn.listToFLOATIP((level, level, d[kind.strip()]))
    (ip1, _, _) = rmn.convertPKtoIP(pk,pk,pk)
    return ip1

def column_descriptions():
    import sys
    sys.stdout.write('nomvar: variable name')
    sys.stdout.write('typvar: type of field ([F]orecast, [A]nalysis, [C]limatology)')
    sys.stdout.write('etiket: concatenation of label, run, implementation and ensemble_member')
    sys.stdout.write('ni: first dimension of the data field - relates to shape')
    sys.stdout.write('nj: second dimension of the data field - relates to shape')
    sys.stdout.write('nk: third dimension of the data field - relates to shape')
    sys.stdout.write('dateo: date of observation time stamp')
    sys.stdout.write('ip1: encoded vertical level')
    sys.stdout.write('ip2: encoded forecast hour, but can be used in other ways by encoding an ip value')
    sys.stdout.write('ip3: user defined identifier')
    sys.stdout.write('deet: length of a time step in seconds - usually invariable - relates to model ouput times')
    sys.stdout.write('npas: time step number')
    sys.stdout.write('datyp: data type of the elements (int,float,str,etc)')
    sys.stdout.write('nbits: number of bits kept for the elements of the field (16,32,etc)')
    sys.stdout.write('ig1: first grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig2: second grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig3: third grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig4: fourth grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('grtyp: type of geographical projection identifier (Z, X, Y, etc)')
    sys.stdout.write('datev: date of validity (dateo + deet * npas) Will be set to -1 if dateo invalid')
    sys.stdout.write('d: data associated to record, empty until data is loaded - either a numpy array or a daks array for one level of data')
    sys.stdout.write('key: key/handle of the record - used by rpnpy to locate records in a file')
    sys.stdout.write('shape: (ni, nj, nk) dimensions of the data field - an attribute of the numpy/dask array (array.shape)')
