# -*- coding: utf-8 -*-
import os

import numpy as np
import pandas as pd
import math
import rpnpy.librmn.all as rmn

from .dataframe_utils import metadata_cleanup
from .exceptions import StandardFileWriterError
from .logger_config import logger
from .std_io import get_grid_metadata_fields
from .std_reader import load_data, unload_data
from .utils import initializer
import sys


class StandardFileWriter:
    """Writes a standard file Dataframe to file. if no metada fields like ^^ and >> are found,
    an attempt will be made to load them from the original file so that they can be added to the output if not already present

    :param filename: path of file to write to
    :type filename: str
    :param df: dataframe to write
    :type df: pd.DataFrame
    :param mode: In 'dump' mode, no processing will be done on the dataframe before writing, data must be present in the dataframe.
    If set to 'update', path must be an existing file. Only the field metadata will be updated, the data itself will not be modified.
    In 'write' mode, the data will be loaded, metadata fields like '>>' will be added if not present default 'write'
    :type mode: str
    :param no_meta: if true these fields ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"] will be removed from the dataframe
    :type no_meta: bool
    :param overwrite: if True and dataframe inputfile is the same as the output file, records will be added to the file, defaults to False
    :type overwrite: bool, optional
    :param rewrite: overrides default rewrite value for fstecr, default None
    :type rewrite: bool, optional
    """
    modes = ['write','update','dump']
    @initializer
    def __init__(self, filename:str, df:pd.DataFrame, mode='write', no_meta=False, overwrite=False,rewrite=None):
        self.validate_input()

    def validate_input(self):
        if self.df.empty:
            raise StandardFileWriterError('StandardFileWriter - no records to process')

        if self.mode not in self.modes:
            raise StandardFileWriterError(f'StandardFileWriter - mode must have one of these values {self.modes}, you entered {self.mode}')

        self.filename = os.path.abspath(self.filename)
        self.file_exists = os.path.exists(self.filename)

        if self.file_exists and self.overwrite==False:
            raise StandardFileWriterError('StandardFileWriter - file exists, use overwrite flag to avoid this error')

    def to_fst(self):
        """in write mode,gets the metadata fields if not already present and adds them to the dataframe.
        If not in update only mode, loads the actual data. opens the file writes the dataframe and closes.
        """
        if self.mode=='dump':
            for i in self.df.index:
                if not isinstance(self.df.at[i,'d'],np.ndarray):
                    raise StandardFileWriterError(f'StandardFileWriter - all data must be loaded in dump mode - row[{i}] does not have its data loaded')

        # remove meta
        if self.no_meta:
            self.df = self.df.loc[~self.df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"])]


        if self.mode == 'dump':
            self._dump()
        elif self.mode == 'update':
            self._update()
        else:
            self._write()


    def _dump(self):
        if not(self.rewrite is None):
            rewrite = self.rewrite
        else:
            rewrite = True
        file_id = rmn.fstopenall(self.filename,rmn.FST_RW)
        for i in self.df.index:
            rmn.fstecr(file_id, data=np.asfortranarray(self.df.at[i,'d']), meta=self.df.loc[i].to_dict(),rewrite=rewrite)
        rmn.fstcloseall(file_id)

    def _update(self):
        if not self.file_exists:
            raise StandardFileWriterError('StandardFileWriter - file does not exist, cant update records')

        path = self.path.unique()
        if len(path) != 1:
            raise StandardFileWriterError('StandardFileWriter - more than one path, cant update records')

        if path[0] != self.filename:
            raise StandardFileWriterError('StandardFileWriter - path in dataframe is different from destination file path, cant update records')

        file_id = rmn.fstopenall(self.filename,rmn.FST_RW)
        for i in self.df.index:
            d = self.df.loc[i].to_dict()
            key = d['key']
            del d['key']
            rmn.fst_edit_dir(int(key),dateo=int(d['dateo']),deet=int(d['deet']),npas=int(d['npas']),ni=int(d['ni']),nj=int(d['nj']),nk=int(d['nk']),datyp=int(d['datyp']),ip1=int(d['ip1']),ip2=int(d['ip2']),ip3=int(d['ip3']),typvar=d['typvar'],nomvar=d['nomvar'],etiket=d['etiket'],grtyp=d['grtyp'],ig1=int(d['ig1']),ig2=int(d['ig2']),ig3=int(d['ig3']),ig4=int(d['ig4']), keep_dateo=False)
        rmn.fstcloseall(file_id)

    def _write(self):
        if not self.no_meta:
            meta_df = get_grid_metadata_fields(self.df)
            self.df = pd.concat([self.df,meta_df],ignore_index=True)
        self.df = metadata_cleanup(self.df)

        if self.rewrite is None:
            rewrite = set_rewrite(self.df)
        else:
            rewrite = self.rewrite

        df_list = np.array_split(self.df, math.ceil(len(self.df.index)/256)) #256 records per block

        for df in df_list:
            df = load_data(df,clean=True) # partial load to keep memory usage low

            file_id = rmn.fstopenall(self.filename,rmn.FST_RW)
            # print(df)
            for i in df.index:
                # if df.at[i,'nomvar']=='!!':
                #     print(df.at[i,'d'].dtype,df.at[i,'ni'],df.at[i,'nj'],df.at[i,'nk'],df.at[i,'shape'],df.at[i,'d'].shape,df.at[i,'d'])
                record_path = self.df.at[i,'path']
                if identical_destination_and_record_path(record_path,self.filename):
                    logger.warning('StandardFileWriter - record path and output file are identical, adding  new records')
                write_dataframe_record_to_file(file_id,df,i,rewrite)
            df = unload_data(df,only_marked=True)
            rmn.fstcloseall(file_id)

def set_rewrite(df):
    original_df_length = len(df.index)
    dropped_df = df.drop_duplicates(subset=['nomvar','typvar','etiket','ip1', 'ip2', 'ip3'], ignore_index=True)
    dropped_df_length = len(dropped_df.index)
    rewrite = True

    if original_df_length != dropped_df_length:
        rewrite=False
        sys.stderr.write('StandardFileWriter - duplicates found, activating rewrite\n')
    return rewrite

def write_dataframe_record_to_file(file_id,df,i,rewrite):
    #df = change_etiket_if_a_plugin_name(df,i)
    df = reshape_data_to_original_shape(df,i)
    # if df.at[i,'nomvar']=='!!':
    #     print(df.loc[i].to_dict(),rewrite)
    rmn.fstecr(file_id, data=np.asfortranarray(df.at[i,'d']), meta=df.loc[i].to_dict(),rewrite=rewrite)



def update_records(df, i):
    d = df.loc[i].to_dict()
    key = d['key']
    del d['key']
    rmn.fst_edit_dir(int(key),dateo=int(d['dateo']),deet=int(d['deet']),npas=int(d['npas']),ni=int(d['ni']),nj=int(d['nj']),nk=int(d['nk']),datyp=int(d['datyp']),ip1=int(d['ip1']),ip2=int(d['ip2']),ip3=int(d['ip3']),typvar=d['typvar'],nomvar=d['nomvar'],etiket=d['etiket'],grtyp=d['grtyp'],ig1=int(d['ig1']),ig2=int(d['ig2']),ig3=int(d['ig3']),ig4=int(d['ig4']), keep_dateo=False)


def identical_destination_and_record_path(record_path, filename):
    return record_path == filename

def reshape_data_to_original_shape(df, i):
    if df.at[i,'nomvar'] not in ['>>','^^','^>','!!','HY','!!SF']:
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
#     if etiket.empty:
#         return plugin_name
#     return get_column_value_from_row(etiket, 'etiket')

# def keys_to_remove(keys, the_dict):
#     for key in keys:
#         if key in the_dict:
#             del the_dict[key]

# def set_typvar(df, i):
#     if ('typvar' in df.columns) and ('unit_converted' in df.columns) and (df.at[i,'unit_converted'] == True) and (len(df.at[i,'typvar']) == 1):
#         df.at[i,'typvar']  += 'U'
