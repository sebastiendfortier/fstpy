# -*- coding: utf-8 -*-
import inspect
from functools import wraps
import pandas as pd
import rpnpy.librmn.all as rmn
import logging


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

def delete_file(my_file:str):
    """delete a file by path

    :param my_file: path to file to delete
    :type my_file: str
    """
    import os
    if os.path.exists(my_file):
        os.unlink(my_file)
        


def create_1row_df_from_model(df:pd.DataFrame) -> pd.DataFrame:
    """Creates a one row dataframe based on a model dataframe's first row. 
    This is useful when you need to create a 1 dimension result holder. 
    Since the new row will hold a result, the key and file_modification_time are set to None
    so that if a call to load_data is made, the data portion won't be overwritten

    :param df: dataframe used as a model
    :type df: pd.DataFrame
    :return: a 1 row dataframe
    :rtype: pd.DataFrame
    """
    if df.empty:
        logging.warning('cant create, model dataframe empty')
    res_df = df.iloc[0].to_dict()
    res_df = pd.DataFrame([res_df])

    res_df['file_modification_time'] = None
    res_df['key'] = None
    return res_df

def get_file_list(pattern:str) -> str:
    """Gets the list of files for provided path expression with wildcards

    :param pattern: a directory with regex pattern to find files in
    :type pattern: str
    :return: a list of files
    :rtype: str
    """
    import glob
    files = glob.glob(pattern)
    return files

def ip_from_value_and_kind(value:float,kind:str) -> int:
    """Create an encoded ip value out of a value (float) and a printable kind.
    Valid kinds are m,sg,M,hy,th,H and mp

    :param level: a level value as a float
    :type level: float
    :param kind: a textual representation of kind, m,sg,M,hy,th,H or mp
    :type kind: str
    :return: encoded ip value
    :rtype: int
    """
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

    pk =  rmn.listToFLOATIP((value, value, d[kind.strip()]))
    
    if kind.strip() == 'H':
        (_, ip, _) =  rmn.convertPKtoIP(rmn.listToFLOATIP((value, value, d["m"])), pk, pk)
    else:
        (ip, _, _) = rmn.convertPKtoIP(pk,pk,pk)
    return ip


def column_descriptions():
    """Prints the base attributes descriptions
    """
    logging.info('nomvar: variable name')
    logging.info('typvar: type of field ([F]orecast, [A]nalysis, [C]limatology)')
    logging.info('etiket: concatenation of label, run, implementation and ensemble_member')
    logging.info('ni: first dimension of the data field - relates to shape')
    logging.info('nj: second dimension of the data field - relates to shape')
    logging.info('nk: third dimension of the data field - relates to shape')
    logging.info('dateo: date of observation time stamp')
    logging.info('ip1: encoded vertical level')
    logging.info('ip2: encoded forecast hour, but can be used in other ways by encoding an ip value')
    logging.info('ip3: user defined identifier')
    logging.info('deet: length of a time step in seconds - usually invariable - relates to model ouput times')
    logging.info('npas: time step number')
    logging.info('datyp: data type of the elements (int,float,str,etc)')
    logging.info('nbits: number of bits kept for the elements of the field (16,32,etc)')
    logging.info('ig1: first grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    logging.info('ig2: second grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    logging.info('ig3: third grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    logging.info('ig4: fourth grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    logging.info('grtyp: type of geographical projection identifier (Z, X, Y, etc)')
    logging.info('datev: date of validity (dateo + deet * npas) Will be set to -1 if dateo invalid')
    logging.info('d: data associated to record, empty until data is loaded - either a numpy array or a daks array for one level of data')
    logging.info('key: key/handle of the record - used by rpnpy to locate records in a file')
    logging.info('shape: (ni, nj, nk) dimensions of the data field - an attribute of the numpy/dask array (array.shape)')
