# -*- coding: utf-8 -*-
from .exceptions import StandardFileWriterError
import pandas as pd
import os
import rpnpy.librmn.all as rmn
from .utils import initializer, validate_df_not_empty
from .dataframe import sort_dataframe
from .std_io import get_meta_data_fields,open_fst,close_fst
from .logger_config import logger


class StandardFileWriter:
    
    """fst file writer  

    :param filename: file to write to  
    :type filename: str  
    :param df: dataframe to write  
    :type df: pd.DataFrame  
    :param erase: erase the file before writing to it, defaults to False  
    :type erase: bool, optional  
    :param add_meta_fields: write the metadata associated with the data frame if it exists, defaults to True  
    :type add_meta_fields: bool, optional  
    """
    @initializer
    def __init__(self, filename:str, df:pd.DataFrame, update_meta_only=False):

        #logger.info('StandardFileWriter %s' % filename)
        validate_df_not_empty(self.df,'StandardFileWriter',StandardFileWriterError)
        self.filename = os.path.abspath(self.filename)
       
    def to_fst(self):
        from .std_reader import load_data
        self.meta_df = get_meta_data_fields(self.df,'to_fst',StandardFileWriterError)
        if not self.meta_df.empty:
            int_df = pd.merge(self.meta_df, self.df, how ='inner', on =['ni', 'nj', 'nk', 'ip1', 'ip2', 'ip3', 'deet', 'npas',
                'nbits' , 'ig1', 'ig2', 'ig3', 'ig4', 'datev','swa', 'lng', 'ubc',
                'xtra1', 'xtra2', 'xtra3', 'dateo', 'datyp']) 

            if len(int_df.index) and len(self.meta_df.index) != len(int_df.index):
                self.df = pd.concat(self.df, self.meta_df).drop_duplicates(keep=False)

        if not self.update_meta_only:
            self.df = load_data(self.df)

        self.file_id, self.file_modification_time = open_fst(self.filename, rmn.FST_RW, 'StandardFileWriter', StandardFileWriterError)
        self.write()
        close_fst(self.file_id,self.filename,'StandardFileWriter')


    def write(self):
        logger.info('StandardFileWriter - writing to file %s', self.filename)  

        self.df = sort_dataframe(self.df)
        for i in self.df.index:
            set_typvar(self.df,i)
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
    df = change_etiket_if_a_plugin_name(df,i)
    df = reshape_data_to_original_shape(df,i)
    rmn.fstecr(file_id, np.asfortranarray(df.at[i,'d']), df.loc[i].to_dict())     

def update_meta_data(df, i):
    d = df.loc[i].to_dict()
    key = d['key']
    del d['key']
    rmn.fst_edit_dir(int(key),dateo=int(d['dateo']),deet=int(d['deet']),npas=int(d['npas']),ni=int(d['ni']),nj=int(d['nj']),nk=int(d['nk']),datyp=int(d['datyp']),ip1=int(d['ip1']),ip2=int(d['ip2']),ip3=int(d['ip3']),typvar=d['typvar'],nomvar=d['nomvar'],etiket=d['etiket'],grtyp=d['grtyp'],ig1=int(d['ig1']),ig2=int(d['ig2']),ig3=int(d['ig3']),ig4=int(d['ig4']), keep_dateo=False)


def identical_destination_and_record_path(record_path, filename):
    return record_path == filename

def reshape_data_to_original_shape(df, i):
    if df.at[i,'d'].shape != df.at[i,'shape']:
        df.at[i,'d'] = df.at[i,'d'].reshape(df.at[i,'shape'])
    return df

def change_etiket_if_a_plugin_name(df, i):
    df.at[i,'etiket'] = get_std_etiket(df.at[i,'etiket'])
    return df

def remove_df_columns(df,keys_to_keep = {'key','dateo', 'deet', 'npas', 'ni', 'nj', 'nk', 'datyp', 'nbits', 'ip1', 'ip2', 'ip3', 'typvar', 'nomvar', 'etiket', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'}):
    d = df.iloc[0].to_dict()
    d_keys = set(d.keys())
    keys_to_remove = list(d_keys-keys_to_keep)
    #keys_to_keep = {'dateo', 'datev', 'deet', 'npas', 'ni', 'nj', 'nk', 'nbits', 'datyp', 'ip1', 'ip2', 'ip3', 'typvar', 'nomvar', 'etiket', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'}
    df = df.drop(columns=keys_to_remove)
    return df

def get_std_etiket(plugin_name:str):
    """get_std_etiket get the etiket corresponding to the plugin name

    :param plugin_name: plugin name
    :type plugin_name: str
    :return: etiket corresponding to plugin name in etiket db
    :rtype: str
    """
    etiket = get_etikey_by_name(plugin_name)
    if len(etiket.index) == 0:
        return plugin_name
    return get_column_value_from_row(etiket, 'etiket')    

def keys_to_remove(keys, the_dict):
    for key in keys:
        if key in the_dict:
            del the_dict[key]    

def set_typvar(df, i):
    if ('typvar' in df.columns) and ('unit_converted' in df.columns) and (df.at[i,'unit_converted'] == True) and (len(df.at[i,'typvar']) == 1):
        df.at[i,'typvar']  += 'U'            