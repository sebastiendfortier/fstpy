# -*- coding: utf-8 -*-
import os

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from .dataframe import sort_dataframe
from .exceptions import StandardFileWriterError
from .logger_config import logger
from .std_io import close_fst, get_grid_metadata_fields, open_fst
from .std_reader import load_data
from .utils import initializer, validate_df_not_empty


class StandardFileWriter:
    """Writes a standard file Dataframe to file. if no metada fields like ^^ and >> are found, 
    an attempt will be made to load them from the original file so that they can be added to the output if not already present

    :param filename: path of file to write to
    :type filename: str
    :param df: dataframe to write
    :type df: pd.DataFrame
    :param update_meta_only: if True and dataframe inputfile is the same as the output file, 
    only the metadata will be updated. The actual data will not be loaded or modified, defaults to False
    :type update_meta_only: bool, optional
    """
    @initializer
    def __init__(self, filename:str, df:pd.DataFrame, update_meta_only=False):

        validate_df_not_empty(self.df,'StandardFileWriter',StandardFileWriterError)
        self.filename = os.path.abspath(self.filename)
       
    def to_fst(self):
        """get the metadata fields if not already present and adds them to the dataframe. 
        If not in update only mode, loads the actual data. opens the file writes the dataframe and closes.
        """
        
        self.meta_df = get_grid_metadata_fields(self.df)

        if not self.meta_df.empty:
            self.df = pd.concat([self.df, self.meta_df])
            self.df.drop_duplicates(subset=['grtyp','nomvar','typvar','etiket','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'],inplace=True, ignore_index=True,keep='first')
        self.df.reset_index(drop=True,inplace=True)        
        
        if not self.update_meta_only:
            self.df = load_data(self.df)
        self.file_id, self.file_modification_time = open_fst(self.filename,rmn.FST_RW, 'StandardFileWriter', StandardFileWriterError)
        self._write()
        close_fst(self.file_id,self.filename,'StandardFileWriter')


    def _write(self):
        logger.info('StandardFileWriter - writing to file %s', self.filename)  
        self.df.drop_duplicates(subset=['grtyp','nomvar','typvar','etiket','ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas','nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'dateo', 'datyp'],inplace=True, ignore_index=True,keep='first')
        self.df = sort_dataframe(self.df)
        
        for i in self.df.index:
            #set_typvar(self.df,i)
            record_path = self.df.at[i,'path']
            if identical_destination_and_record_path(record_path,self.filename):
                if self.update_meta_only:
                    logger.warning('StandardFileWriter - record path and output file are identical, updating record meta data only')
                    update_meta_data(self.df,i)
                else:
                    logger.warning('StandardFileWriter - record path and output file are identical, adding  new records')
                    write_dataframe_record_to_file(self.file_id,self.df,i)  
            else:
                write_dataframe_record_to_file(self.file_id,self.df,i)     

def write_dataframe_record_to_file(file_id,df,i):
    #df = change_etiket_if_a_plugin_name(df,i)
    df = reshape_data_to_original_shape(df,i)
    rmn.fstecr(file_id, data=np.asfortranarray(df.at[i,'d']), meta=df.loc[i].to_dict())     
 

def update_meta_data(df, i):
    d = df.loc[i].to_dict()
    key = d['key']
    del d['key']
    rmn.fst_edit_dir(int(key),dateo=int(d['dateo']),deet=int(d['deet']),npas=int(d['npas']),ni=int(d['ni']),nj=int(d['nj']),nk=int(d['nk']),datyp=int(d['datyp']),ip1=int(d['ip1']),ip2=int(d['ip2']),ip3=int(d['ip3']),typvar=d['typvar'],nomvar=d['nomvar'],etiket=d['etiket'],grtyp=d['grtyp'],ig1=int(d['ig1']),ig2=int(d['ig2']),ig3=int(d['ig3']),ig4=int(d['ig4']), keep_dateo=False)


def identical_destination_and_record_path(record_path, filename):
    return record_path == filename

def reshape_data_to_original_shape(df, i):
    if df.at[i,'d'].shape != tuple(df.at[i,'shape']):
        df.at[i,'d'] = df.at[i,'d'].reshape(df.at[i,'shape'])
    return df

# def change_etiket_if_a_plugin_name(df, i):
#     df.at[i,'etiket'] = get_std_etiket(df.at[i,'etiket'])
#     return df

# def remove_df_columns(df,keys_to_keep = {'key','dateo', 'deet', 'npas', 'ni', 'nj', 'nk', 'datyp', 'nbits', 'ip1', 'ip2', 'ip3', 'typvar', 'nomvar', 'etiket', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'}):
#     d = df.iloc[0].to_dict()
#     d_keys = set(d.keys())
#     keys_to_remove = list(d_keys-keys_to_keep)
#     #keys_to_keep = {'dateo', 'datev', 'deet', 'npas', 'ni', 'nj', 'nk', 'nbits', 'datyp', 'ip1', 'ip2', 'ip3', 'typvar', 'nomvar', 'etiket', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'}
#     df = df.drop(columns=keys_to_remove,errors='ignore')
#     return df

# def get_std_etiket(plugin_name:str):
#     """get_std_etiket get the etiket corresponding to the plugin name

#     :param plugin_name: plugin name
#     :type plugin_name: str
#     :return: etiket corresponding to plugin name in etiket db
#     :rtype: str
#     """
#     etiket = get_etikey_by_name(plugin_name)
#     if len(etiket.index) == 0:
#         return plugin_name
#     return get_column_value_from_row(etiket, 'etiket')    

# def keys_to_remove(keys, the_dict):
#     for key in keys:
#         if key in the_dict:
#             del the_dict[key]    

# def set_typvar(df, i):
#     if ('typvar' in df.columns) and ('unit_converted' in df.columns) and (df.at[i,'unit_converted'] == True) and (len(df.at[i,'typvar']) == 1):
#         df.at[i,'typvar']  += 'U'            
