# -*- coding: utf-8 -*-
import logging
import os

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from fstpy.std_reader import compute

from .dataframe_utils import metadata_cleanup
from .std_io import get_field_dtype, get_grid_metadata_fields
from .utils import get_num_rows_for_reading, initializer


class StandardFileWriterError(Exception):
    pass


class StandardFileWriter:
    """Writes a standard file Dataframe to file. If no metada fields like ^^ and >> are found,
    an attempt will be made to load them from the original file so that they can be added to the output if not already present

    :param filename: path of file to write to
    :type filename: str
    :param df: dataframe to write
    :type df: pd.DataFrame
    :param mode: In 'dump' mode, no processing will be done on the dataframe 
                before writing, data must be present in the dataframe (df = compute(df)).
                If set to 'update', path must be an existing file. Only the 
                field metadata will be updated, the data itself will not be 
                modified. In 'write' mode, the data will be loaded, metadata 
                fields like '>>' will be added if not present default 'write'
    :type mode: str
    :param no_meta: if true these fields ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"] will be removed from the dataframe
    :type no_meta: bool
    :param overwrite: if True and dataframe inputfile is the same as the output file, records will be added to the file, defaults to False
    :type overwrite: bool, optional
    :param rewrite: overrides default rewrite value for fstecr, default None
    :type rewrite: bool, optional
    """
    modes = ['write', 'update', 'dump']

    @initializer
    def __init__(self, filename: str, df: pd.DataFrame, mode='write', no_meta=False, overwrite=False, rewrite=None):
        self.validate_input()

    def validate_input(self):
        if self.df.empty:
            raise StandardFileWriterError(
                'StandardFileWriter - no records to process')

        if self.mode not in self.modes:
            raise StandardFileWriterError(
                f'StandardFileWriter - mode must have one of these values {self.modes}, you entered {self.mode}')

        self.filename = os.path.abspath(self.filename)
        self.file_exists = os.path.exists(self.filename)

        if self.file_exists and self.overwrite == False:
            raise StandardFileWriterError(
                'StandardFileWriter - file exists, use overwrite flag to avoid this error')

    def to_fst(self):
        """In write mode, gets the metadata fields if not already present and adds them to the dataframe.
        If not in update only mode, loads the actual data, opens the file writes the dataframe and closes.
        """
        if self.mode == 'dump':
            for i in self.df.index:
                if not isinstance(self.df.at[i, 'd'], np.ndarray):
                    raise StandardFileWriterError(
                        f'StandardFileWriter - all data must be loaded in dump mode - row[{i}] does not have its data loaded')

        # remove meta
        if self.no_meta:
            self.df = self.df.loc[~self.df.nomvar.isin(
                ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1", "PN"])]

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
        file_id = rmn.fstopenall(self.filename, rmn.FST_RW)
        for i in self.df.index:
            rmn.fstecr(file_id, data=np.asfortranarray(
                self.df.at[i, 'd']), meta=self.df.loc[i].to_dict(), rewrite=rewrite)
        rmn.fstcloseall(file_id)

    def _update(self):
        if not self.file_exists:
            raise StandardFileWriterError(
                'StandardFileWriter - file does not exist, cant update records')

        path = self.df.path.unique()
        if len(path) != 1:
            raise StandardFileWriterError(
                'StandardFileWriter - more than one path, cant update records')

        if path[0] != self.filename:
            raise StandardFileWriterError(
                'StandardFileWriter - path in dataframe is different from destination file path, cant update records')

        file_id = rmn.fstopenall(self.filename, rmn.FST_RW)
        for i in self.df.index:
            d = self.df.loc[i].to_dict()
            key = d['key']
            del d['key']
            rmn.fst_edit_dir(int(key), dateo=int(d['dateo']), deet=int(d['deet']), npas=int(d['npas']), ni=int(d['ni']), nj=int(d['nj']), nk=int(d['nk']), datyp=int(d['datyp']), ip1=int(d['ip1']), ip2=int(
                d['ip2']), ip3=int(d['ip3']), typvar=d['typvar'], nomvar=d['nomvar'], etiket=d['etiket'], grtyp=d['grtyp'], ig1=int(d['ig1']), ig2=int(d['ig2']), ig3=int(d['ig3']), ig4=int(d['ig4']), keep_dateo=False)
        rmn.fstcloseall(file_id)

    def _write(self):

        meta = ["^^", ">>", "^>", "!!", "HY", "!!SF", "E1", "P0", "PT", "PN"]
        cols = ['nomvar', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3',
                'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']

        self.df = metadata_cleanup(self.df)

        if not self.no_meta:
            original_meta_df = self.df.loc[self.df.nomvar.isin(meta)]
            original_meta_to_cmp_df = original_meta_df[cols].sort_values(cols).reset_index(drop=True)
            meta_df = get_grid_metadata_fields(self.df)
            self.df = pd.concat([meta_df, self.df], ignore_index=True)
            self.df = metadata_cleanup(self.df)

            added_meta_df = self.df.loc[self.df.nomvar.isin(meta)]
            added_meta_to_cmp_df = added_meta_df[cols].sort_values(cols).reset_index(drop=True)

            if (not original_meta_to_cmp_df.empty) and (not added_meta_to_cmp_df.empty):
                if original_meta_to_cmp_df.equals(added_meta_to_cmp_df):
                    self.df = self.df.loc[~self.df.nomvar.isin(meta)]
                    self.df = pd.concat([original_meta_df, self.df], ignore_index=True)


        if self.rewrite is None:
            rewrite = set_rewrite(self.df)
        else:
            rewrite = self.rewrite

        num_rows = get_num_rows_for_reading(self.df)

        df_list = np.array_split(self.df, num_rows)  # of records per block

        for df in df_list:
            df = compute(df)
            file_id = rmn.fstopenall(self.filename, rmn.FST_RW)

            for i in df.index:
                record_path = self.df.at[i, 'path']
                if identical_destination_and_record_path(record_path, self.filename):
                    logging.warning(
                        'StandardFileWriter - record path and output file are identical, adding  new records')
                write_dataframe_record_to_file(file_id, df, i, rewrite)
            rmn.fstcloseall(file_id)


def set_rewrite(df):
    original_df_length = len(df.index)
    dropped_df = df.drop_duplicates(
        subset=['nomvar', 'typvar', 'etiket', 'ip1', 'ip2', 'ip3'], ignore_index=True)
    dropped_df_length = len(dropped_df.index)
    rewrite = True

    if original_df_length != dropped_df_length:
        rewrite = False
        logging.warning(
            'StandardFileWriter - duplicates found, activating rewrite')
    return rewrite


def write_dataframe_record_to_file(file_id, df, i, rewrite):
    df = reshape_data_to_original_shape(df, i)
    field_dtype = get_field_dtype(df.at[i, 'datyp'], df.at[i, 'nbits'])
    
    if isinstance(df.at[i, 'd'], np.ndarray):
        data = df.at[i, 'd']
    else:
        data = df.at[i, 'd'].compute()
    if str(data.dtype) != field_dtype:
        logging.warning(f'For record at index {i}, nomvar:{df.at[i,"nomvar"]} datyp:{df.at[i,"datyp"]} nbits:{df.at[i,"nbits"]} array.dtype:{df.at[i,"d"].dtype}')  
        logging.warning('Difference in field dtype detected! - check dataframe nbits datyp and array dtype for mismatch')    
        
    rmn.fstecr(file_id, data=np.asfortranarray(data.astype(field_dtype)),
               meta=df.loc[i].to_dict(), rewrite=rewrite)


def update_records(df, i):
    d = df.loc[i].to_dict()
    key = d['key']
    del d['key']
    rmn.fst_edit_dir(int(key), dateo=int(d['dateo']), deet=int(d['deet']), npas=int(d['npas']), ni=int(d['ni']), nj=int(d['nj']), nk=int(d['nk']), datyp=int(d['datyp']), ip1=int(d['ip1']), ip2=int(
        d['ip2']), ip3=int(d['ip3']), typvar=d['typvar'], nomvar=d['nomvar'], etiket=d['etiket'], grtyp=d['grtyp'], ig1=int(d['ig1']), ig2=int(d['ig2']), ig3=int(d['ig3']), ig4=int(d['ig4']), keep_dateo=False)


def identical_destination_and_record_path(record_path, filename):
    return record_path == filename


def reshape_data_to_original_shape(df, i):
    if df.at[i, 'nomvar'] not in ['>>', '^^', '^>', '!!', 'HY', '!!SF']:
        if df.at[i, 'd'].shape != tuple(df.at[i, 'shape']):
            df.at[i, 'd'] = df.at[i, 'd'].reshape(df.at[i, 'shape'])
    return df
