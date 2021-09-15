# -*- coding: utf-8 -*-
import itertools
import multiprocessing as mp
import os

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn


# import xarray as xr

from .dataframe import add_columns, add_data_column, add_shape_column, drop_duplicates
from .std_io import (close_fst, compare_modification_times, open_fst, 
                     parallel_get_dataframe_from_file, get_dataframe_from_file)
from .utils import initializer

class StandardFileReaderError(Exception):
    pass

class StandardFileReader:
    """Class to handle fst files
        Opens, reads the contents of an fst files or files into a pandas Dataframe and closes  
        No data is loaded unless specified, only the metadata is read. Extra metadata is added to the dataframe if specified.  
  
        :param filenames: path to file or list of paths to files  
        :type filenames: str|list[str], does not accept wildcards (numpy has many tools for this)
        :param decode_metadata: adds extra columns, defaults to False  
                'unit':str, unit name   
                'unit_converted':bool  
                'description':str, field description   
                'date_of_observation':datetime, of the date of observation   
                'date_of_validity':datetime, of the date of validity   
                'level':float32, decoded ip1 level   
                'ip1_kind':int32, decoded ip1 kind   
                'ip1_pkind':str, string repr of ip1_kind int   
                'data_type_str':str, string repr of data type   
                'label':str, label derived from etiket   
                'run':str, run derived from etiket   
                'implementation':str, implementation derived from etiket   
                'ensemble_member':str, ensemble member derived from etiket   
                'surface':bool, True if the level is a surface level   
                'follow_topography':bool, indicates if this type of level follows topography   
                'ascending':bool, indicates if this type of level is in ascending order   
                'vctype':str, vertical level type   
                'forecast_hour':timedelta, forecast hour obtained from deet * npas / 3600   
                'ip2_dec':value of decoded ip2    
                'ip2_kind':kind of decoded ip2    
                'ip2_pkind':printable kind of decoded ip2   
                'ip3_dec':value of decoded ip3   
                'ip3_kind':kind of decoded ip3   
                'ip3_pkind':printable kind of decoded ip3   
        :type decode_metadata: bool, optional  
        :param load_data: if True, the data will be read, not just the metadata (fstluk vs fstprm), default False  
        :type load_data: bool, optional  
        :param query: parameter to pass to dataframe.query method, to select specific records  
        :type query: str, optional  
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"]
    @initializer
    def __init__(self, filenames, decode_metadata=False,load_data=False,query=None):
        """init instance"""
        if isinstance(self.filenames,str):
            self.filenames = os.path.abspath(str(self.filenames))
        elif isinstance(self.filenames,list):
            self.filenames = [os.path.abspath(str(f)) for f in filenames]
        else:
            raise StandardFileReaderError('Filenames must be str or list\n')



    def to_pandas(self) -> pd.DataFrame:
        """creates the dataframe from the provided file metadata

        :return: df
        :rtype: pd.Dataframe
        """

        if isinstance(self.filenames, list):
            # convert to list of tuple (path,query,load_data)
            self.filenames = list(zip(self.filenames,itertools.repeat(self.query),itertools.repeat(self.load_data)))
            
            df = parallel_get_dataframe_from_file(self.filenames, get_dataframe_from_file, n_cores=min(mp.cpu_count(),len(self.filenames)))

        else:
            df = get_dataframe_from_file(self.filenames, self.query, self.load_data)

        df = add_data_column(df)

        df = add_shape_column(df)
    
        if self.decode_metadata:
            df = add_columns(df)

        df = drop_duplicates(df)

        return df

def load_data(df:pd.DataFrame,clean:bool=False) -> pd.DataFrame:
    """Gets the associated data for every record in a dataframe

    :param df: dataframe to add arrays to
    :type df: pd.DataFrame
    :param clean: mark loaded data for removal by unload
    :type clean: bool
    :param sort: sort data while loading
    :type sort: bool
    :return: dataframe with filled arrays
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    # add the default flag
    if clean:
        df.loc[:,'clean'] = False

    df_list = []
    no_path_df = df.loc[df.path.isna()]

    path_groups = df.groupby(df.path)
    for _,path_df in path_groups:
        
        if ('file_modification_time' in path_df.columns) and (not (path_df.iloc[0]['file_modification_time'] is None)):
            compare_modification_times(path_df.iloc[0]['file_modification_time'], path_df.iloc[0]['path'],rmn.FST_RO, 'load_data',StandardFileReaderError)

        unit,  _ = open_fst(path_df.iloc[0]['path'],rmn.FST_RO,'load_data',StandardFileReaderError)

        for i in path_df.index:
            if isinstance(path_df.at[i,'d'],np.ndarray):
                continue
            path_df.at[i,'d'] = rmn.fstluk(int(path_df.at[i,'key']))['d']
            path_df.at[i,'clean'] = True if clean else False

        df_list.append(path_df)
        
        close_fst(unit,path_df.iloc[0]['path'],'load_data')


    if len(df_list):
        if not no_path_df.empty:
            df_list.append(no_path_df)
        res_df = pd.concat(df_list,ignore_index=True)
    else:
        res_df = df

    return res_df

def unload_data(df:pd.DataFrame,only_marked:bool=False) -> pd.DataFrame:
    """Removes the loaded data for every record in a dataframe if it can be loaded from file

    :param df: dataframe to remove data from
    :type df: pd.DataFrame
    :param only_marked: unloads only marked o rows with clean column at True
    :type only_marked: bool
    :return: dataframe with arrays removed
    :rtype: pd.DataFrame
    """

    for i in df.index:
        if isinstance(df.at[i,'d'],np.ndarray) and not(df.at[i,'key'] is None) and ( df.at[i,'clean'] if only_marked else True):
            df.at[i,'d'] = None

    df = df.drop(columns=['clean'],errors='ignore')
    
    return df
