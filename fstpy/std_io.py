# -*- coding: utf-8 -*-
import datetime
import multiprocessing as mp
import os.path
import pathlib
import time
import logging
from typing import Type
import logging
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn
from .std_dec import get_grid_identifier


def parallel_get_dataframe_from_file(files, get_records_func, n_cores):
    # Step 1: Init multiprocessing.Pool()
    with mp.Pool(processes=n_cores) as pool:
        df_list = pool.starmap(get_records_func, [file for file in files])

    df = pd.concat(df_list,ignore_index=True)
    return df

def add_grid_column(df : pd.DataFrame) -> pd.DataFrame:
    vcreate_grid_identifier = np.vectorize(get_grid_identifier,otypes=['str'])
    df.loc[:,'grid'] = vcreate_grid_identifier(df['nomvar'].values,df['ip1'].values,df['ip2'].values,df['ig1'].values,df['ig2'].values)
    return df

def get_dataframe_from_file(path: str, query: str, load_data=False):
    
    records = get_records_from_file(path,load_data)

    df = pd.DataFrame(records)

    df = add_grid_column(df)

    hy_df = df.loc[df.nomvar == "HY"]

    df = df.loc[df.nomvar != "HY"]
    
    if (query is None) == False:

        query_result_df = df.query(query)

        # get metadata
        df = add_metadata_to_query_results(df, query_result_df, hy_df)

    # check HY count
    df = process_hy(hy_df, df)

    return df

class AddPathAndModTimeError(Exception):
    pass

def add_path_and_mod_time(path, df):
    f_mod_time = get_file_modification_time(path,rmn.FST_RO,'add_path_and_mod_time',AddPathAndModTimeError)
    df.loc[:,'path'] = path
    df.loc[:,'file_modification_time'] = f_mod_time
    return df

def open_fst(path:str, mode:str, caller_class:str, error_class:Type):
    file_modification_time = get_file_modification_time(path,mode,caller_class,error_class)
    file_id = rmn.fstopenall(path, mode)
    logging.info(f'{caller_class} - opening file {path}')
    return file_id, file_modification_time

def close_fst(file_id:int, path:str,caller_class:str):
    logging.info(f'{caller_class} - closing file {path}')
    rmn.fstcloseall(file_id)

def get_dataframe_from_file_and_load(path:str, query:str):
    df = get_dataframe_from_file(path, query, load_data=True)
    return df

class GetRecordsFromFile(Exception):
    pass

def get_records_from_file(path:str,load_data=False) -> 'list[dict]':
    file_id, file_modification_time = open_fst(path,rmn.FST_RO,'get_records_from_file',GetRecordsFromFile)

    keys=rmn.fstinl(file_id)
    
    desired_order_list = ['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3','deet','npas','datyp','nbits','grtyp','ig1','ig2','ig3','ig4','datev','key','path','file_modification_time']
    
    if load_data:
        rmn_function = rmn.fstluk
        desired_order_list.extend('d')
    else:
        rmn_function = rmn.fstprm
        
    rec_list = []
    for k in keys:
        rec = rmn_function(k)
        if rec['dltf'] == 1:
            continue
        rec = remove_extra_keys(rec)
        rec = strip_string_values(rec)
        rec['file_modification_time'] = file_modification_time
        rec['path'] = path
        
        reordered_dict = {k: rec[k] for k in desired_order_list}
        
        rec_list.append(reordered_dict)

    close_fst(file_id, path,'get_records_from_file')
    
    return rec_list

def add_metadata_to_query_results(df, query_result_df, hy_df):
    meta_df = df.loc[df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "P0", "PT", "E1","PN"])]

    query_result_metadata_df = meta_df.loc[meta_df.grid.isin(list(query_result_df.grid.unique()))]

    if (not query_result_df.empty) and (not query_result_metadata_df.empty):
        df = pd.concat([query_result_df,query_result_metadata_df],ignore_index=True)
    elif (not query_result_df.empty) and (query_result_metadata_df.empty):
        df = query_result_df
    elif query_result_df.empty:
        df = query_result_df

    if (not df.empty) and (not hy_df.empty):
        df = pd.concat([df,hy_df],ignore_index=True)
        
    return df

def process_hy(hy_df:pd.DataFrame, df:pd.DataFrame) -> pd.DataFrame:
    """Make sure there is only one HY, add it to the dataframe and set its grid

    :param hy_df: dataframe of all hy fields
    :type hy_df: pd.DataFrame
    :param df: original dataframe without hy
    :type df: pd.DataFrame
    :return: modified dataframe with one HY field
    :rtype: pd.DataFrame
    """
    if hy_df.empty:
        return df
        
    # check HY count
    hy_count = hy_df.nomvar.count()

    if hy_count >= 1:
        if hy_count > 1:
            logging.warning('More than one HY in this file! - UNKNOWN BEHAVIOR, continue at your own risk\n')
        hy_df = pd.DataFrame([hy_df.iloc[0].to_dict()])
        grid = df.grid.unique()[0]
        hy_df['grid'] = grid
            
        df = pd.concat([df,hy_df],ignore_index=True)
    return df




# written by Micheal Neish creator of fstd2nc
# Lightweight test for FST files.
# Uses the same test for fstd98 random files from wkoffit.c (librmn 16.2).
#
# The 'isFST' test from rpnpy calls c_wkoffit, which has a bug when testing
# many small (non-FST) files.  Under certain conditions the file handles are
# not closed properly, which causes the application to run out of file handles
# after testing ~1020 small non-FST files.
def maybeFST(filename):
  with open(filename, 'rb') as f:
    buf = f.read(16)
    if len(buf) < 16: return False
    # Same check as c_wkoffit in librmn
    return buf[12:] == b'STDR'

def get_file_modification_time(path:str,mode:str,caller_class:str,exception_class:Type) -> datetime.datetime:
    """Gets the file modification time

    :param path: file path to get modification time from
    :type path: str
    :param mode: [description]
    :type mode: str
    :param caller_class: name of calling class for logging
    :type caller_class: str
    :param exception_class: exception to raise
    :type exception_class: Type
    :raises exception_class: exception to raise
    :return: last modification time of file
    :rtype: datetime.datetime
    """
    file = pathlib.Path(path)
    if not file.exists():
        return datetime.datetime.now()
    if (mode == rmn.FST_RO) and (not maybeFST(path)):
        raise exception_class(caller_class + 'not an fst standard file!')

    file_modification_time = time.ctime(os.path.getmtime(path))
    file_modification_time = datetime.datetime.strptime(file_modification_time, "%a %b %d %H:%M:%S %Y")

    return file_modification_time


def strip_string_values(record):
    record['nomvar'] = record['nomvar'].strip()
    record['typvar'] = record['typvar'].strip()
    record['etiket'] = record['etiket'].strip()
    record['grtyp'] = record['grtyp'].strip()
    return record

def remove_extra_keys(record):
    for k in ['swa','dltf','ubc','lng','xtra1','xtra2','xtra3']:
        record.pop(k,None)
    return record    

class Get2dLatLonError(Exception):
    pass

def get_2d_lat_lon(df:pd.DataFrame) -> pd.DataFrame:

    """get_2d_lat_lon Gets the latitudes and longitudes as 2d arrays associated with the supplied grids

    :return: a pandas Dataframe object containing the lat and lon meta data of the grids
    :rtype: pd.DataFrame
    :raises StandardFileError: no records to process
    """
    if  df.empty:
        raise Get2dLatLonError('get_2d_lat_lon- no records to process')

    #remove record wich have X grid type
    without_x_grid_df = df.loc[df.grtyp != "X"]

    latlon_df = get_lat_lon(df)

    if latlon_df.empty:
        raise Get2dLatLonError('get_2d_lat_lon - while trying to find [">>","^^"] - no data to process')

    no_meta_df = without_x_grid_df.loc[~without_x_grid_df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"])]

    latlons = []
    path_groups = no_meta_df.groupby(no_meta_df.path)
    for _, path_group in path_groups:
        path = path_group.iloc[0]['path']
        file_id = rmn.fstopenall(path, rmn.FST_RO)
        grid_groups = path_group.groupby(path_group.grid)
        for _, grid_group in grid_groups:
            row = grid_group.iloc[0]
            rec = rmn.fstlir(file_id, nomvar='%s'%row['nomvar'])
            try:
                g = rmn.readGrid(file_id, rec)
            except Exception:
                logging.warning('get_2d_lat_lon - no lat lon for this record')
                continue

            grid = rmn.gdll(g)

            tictic_df = latlon_df.loc[(latlon_df.nomvar=="^^") & (latlon_df.grid==row['grid'])].reset_index(drop=True)
            tactac_df = latlon_df.loc[(latlon_df.nomvar==">>") & (latlon_df.grid==row['grid'])].reset_index(drop=True)

            tictic_df.at[0,'nomvar'] = 'LA'
            tictic_df.at[0,'d'] = grid['lat']
            tictic_df.at[0,'ni'] = grid['lat'].shape[0]
            tictic_df.at[0,'nj'] = grid['lat'].shape[1]
            tictic_df.at[0,'shape'] = grid['lat'].shape
            tictic_df.at[0,'file_modification_time'] = None

            tactac_df.at[0,'nomvar'] = 'LO'
            tactac_df.at[0,'d'] = grid['lon']
            tactac_df.at[0,'ni'] = grid['lon'].shape[0]
            tactac_df.at[0,'nj'] = grid['lon'].shape[1]
            tactac_df.at[0,'shape'] = grid['lon'].shape
            tactac_df.at[0,'file_modification_time'] = None

            latlons.append(tictic_df)
            latlons.append(tactac_df)

        rmn.fstcloseall(file_id)
    latlon_df = pd.concat(latlons,ignore_index=True)
    return latlon_df

def get_lat_lon(df):
    return get_grid_metadata_fields(df,pressure=False, vertical_descriptors=False)

def get_grid_metadata_fields(df,latitude_and_longitude=True, pressure=True, vertical_descriptors=True):

    path_groups = df.groupby(df.path)
    df_list = []
    #for each files in the df
    for _, rec_df in path_groups:
        path = rec_df.iloc[0]['path']

        if path is None:
            continue
        records = get_metadata(path)
        meta_df = pd.DataFrame(records)
        if meta_df.empty:
            return pd.DataFrame(dtype=object)
        grid_groups = rec_df.groupby(rec_df.grid)
        #for each grid in the current file
        for _,grid_df in grid_groups:
            this_grid = grid_df.iloc[0]['grid']
            if vertical_descriptors:
                vertical_df = meta_df.loc[(meta_df.nomvar.isin(["!!", "HY", "!!SF", "E1"])) & (meta_df.grid==this_grid)]
                df_list.append(vertical_df)
            if pressure:
                pressure_df = meta_df.loc[(meta_df.nomvar.isin(["P0", "PT"])) & (meta_df.grid==this_grid)]
                df_list.append(pressure_df)
            if latitude_and_longitude:
                latlon_df = meta_df.loc[(meta_df.nomvar.isin(["^>", ">>", "^^"])) & (meta_df.grid==this_grid)]
                df_list.append(latlon_df)


    if len(df_list):
        result = pd.concat(df_list,ignore_index=True)
        return result
    else:
        return pd.DataFrame(dtype=object)


class GetMetaDataError(Exception):
    pass
    
def get_metadata(path):

    unit, _ = open_fst(path, rmn.FST_RO, 'get_metadata',GetMetaDataError)
    lat_keys = rmn.fstinl(unit,nomvar='^^')
    lon_keys = rmn.fstinl(unit,nomvar='>>')
    tictac_keys = rmn.fstinl(unit,nomvar='^>')
    toctoc_keys = rmn.fstinl(unit,nomvar='!!')
    hy_keys = rmn.fstinl(unit,nomvar='HY')
    sf_keys = rmn.fstinl(unit,nomvar='!!SF')
    e1_keys = rmn.fstinl(unit,nomvar='E1')
    p0_keys = rmn.fstinl(unit,nomvar='P0')
    pt_keys = rmn.fstinl(unit,nomvar='PT')
    pn_keys = rmn.fstinl(unit,nomvar='PN')
    keys = lat_keys + lon_keys + tictac_keys + toctoc_keys + hy_keys + sf_keys + e1_keys + p0_keys + pt_keys + pn_keys
    records=[]
    for key in keys:
        record = rmn.fstluk(key)
        if record['dltf'] == 1:
            continue
        record = strip_string_values(record)
        #create a grid identifier for each record
        record['grid'] = get_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
        record = remove_extra_keys(record)
        record['path'] = path
        record['file_modification_time'] = get_file_modification_time(path,rmn.FST_RO,'get_all_meta_data_fields_from_std_file',GetMetaDataError)
        records.append(record)
    close_fst(unit,path,'get_metadata')
    return records


def compare_modification_times(df_file_modification_time, path:str, mode:str, caller:str,error_class:Type):
    file_modification_time = get_file_modification_time(path,mode,caller, error_class)

    if np.datetime64(df_file_modification_time)-np.datetime64(file_modification_time) != np.timedelta64(0):
        raise error_class(caller + ' - original file has been modified since the start of the operation, keys might be invalid - exiting!')
