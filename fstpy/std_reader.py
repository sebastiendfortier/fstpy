# -*- coding: utf-8 -*-
from fstpy.xarray_utils import comvert_to_cmc_xarray
import itertools
import multiprocessing as mp
import os

import numpy as np
import pandas as pd
from .dataframe import (add_columns, add_data_column, add_shape_column,
                        drop_duplicates)
from .std_io import (get_dataframe_from_file, get_file_modification_time,
                     parallel_get_dataframe_from_file)
from .utils import initializer, to_numpy



class StandardFileReaderError(Exception):
    pass


class StandardFileReader:
    """Class to handle fst files.  
        Opens, reads the contents of an fst file or files   
        into a pandas dataframe and closes.   
        Extra metadata columns are added to the dataframe if specified.    

        :param filenames: path to file or list of paths to files  
        :type filenames: str|list[str], does not accept wildcards (numpy has 
                         many tools for this)
        :param decode_metadata: adds extra columns, defaults to False  
            'unit':str, unit name   
            'unit_converted':bool  
            'description':str, field description   
            'date_of_observation':datetime, of the date of 
                                    observation   
            'date_of_validity': datetime, of the date of 
                                validity   
            'level':float32, decoded ip1 level   
            'ip1_kind':int32, decoded ip1 kind   
            'ip1_pkind':str, string repr of ip1_kind int   
            'data_type_str':str, string repr of data type   
            'label':str, label derived from etiket   
            'run':str, run derived from etiket   
            'implementation': str, implementation derived 
                                from etiket   
            'ensemble_member': str, ensemble member derived 
                                from etiket   
            'surface':bool, True if the level is a 
                        surface level   
            'follow_topography':bool, indicates if this type 
                                of level follows topography   
            'ascending':bool, indicates if this type of 
                        level is in ascending order   
            'vctype':str, vertical level type   
            'forecast_hour': timedelta, forecast hour 
                            obtained from deet * npas / 3600   
            'ip2_dec':value of decoded ip2    
            'ip2_kind':kind of decoded ip2    
            'ip2_pkind':printable kind of decoded ip2   
            'ip3_dec':value of decoded ip3   
            'ip3_kind':kind of decoded ip3   
            'ip3_pkind':printable kind of decoded ip3   
        :type decode_metadata: bool, optional  
        :param query: parameter to pass to dataframe.query method, to select 
                      specific records  
        :type query: str, optional  
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1", "PN"]

    @initializer
    def __init__(self, filenames, decode_metadata=False, query=None):
        """init instance"""
        if isinstance(self.filenames, str):
            self.filenames = os.path.abspath(str(self.filenames))
        elif isinstance(self.filenames, list):
            self.filenames = [os.path.abspath(str(f)) for f in filenames]
        else:
            raise StandardFileReaderError('Filenames must be str or list\n')

    def to_pandas(self) -> pd.DataFrame:
        """creates the dataframe from the provided file metadata

        :return: df
        :rtype: pd.Dataframe
        """

        if isinstance(self.filenames, list):
            # convert to list of tuple (path,query)
            self.filenames = list(
                zip(self.filenames, itertools.repeat(self.query)))

            df = parallel_get_dataframe_from_file(
                self.filenames,  get_dataframe_from_file, n_cores=min(mp.cpu_count(), len(self.filenames)))

        else:
            df = get_dataframe_from_file(self.filenames, self.query)

        df = add_data_column(df)

        df = add_shape_column(df)

        if self.decode_metadata:
            df = add_columns(df)

        df = drop_duplicates(df)

        return df
     
    def to_cmc_xarray(self):
        df = self.to_pandas()
        return comvert_to_cmc_xarray(df)

def to_cmc_xarray(df):
    return comvert_to_cmc_xarray(df)
    
class ComputeError(Exception):
    pass


def compute(df: pd.DataFrame) -> pd.DataFrame:
    no_path_df = df.loc[df.path.isna()]

    groups = df.groupby('path')
    
    df_list = []
    
    if not no_path_df.empty:
        df_list.append(no_path_df)
        
    for path, current_df in groups:
        file_modification_time = get_file_modification_time(path)
        if np.datetime64(current_df.iloc[0]['file_modification_time'])-np.datetime64(file_modification_time) != np.timedelta64(0):
            raise ComputeError(
                'File has been modified since it was first read - keys are invalid, make sure file stays intact during processing\n')
        current_df = current_df.sort_values('key')

        for i in current_df.index:
             current_df.at[i, 'd'] = to_numpy( current_df.at[i, 'd'])

        df_list.append(current_df)

    df = pd.concat(df_list).sort_index()

    return df
