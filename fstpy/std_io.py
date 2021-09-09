# -*- coding: utf-8 -*-
import datetime
import multiprocessing as mp
import os.path
import pathlib
import sys
import time

# import dask.array as da
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

from fstpy.extra import get_std_file_header

from .exceptions import StandardFileError, StandardFileReaderError
from .logger_config import logger
from .std_dec import get_grid_identifier



def open_fst(path:str, mode:str, caller_class:str, error_class:Exception):
    file_modification_time = get_file_modification_time(path,mode,caller_class,error_class)
    file_id = rmn.fstopenall(path, mode)
    logger.info(caller_class + ' - opening file %s', path)
    return file_id, file_modification_time

def close_fst(file_id:int, file:str,caller_class:str):
    logger.info(caller_class + ' - closing file %s', file)
    rmn.fstcloseall(file_id)

def parallel_get_dataframe_from_file(files, get_records_func, n_cores):
    # Step 1: Init multiprocessing.Pool()
    with mp.Pool(processes=n_cores) as pool:
        df_list = pool.starmap(get_records_func, [file for file in files])

    df = pd.concat(df_list,ignore_index=True)
    return df

def add_grid_column(df):
    vcreate_grid_identifier = np.vectorize(get_grid_identifier,otypes=['str'])
    df.loc[:,'grid'] = vcreate_grid_identifier(df['nomvar'].values,df['ip1'].values,df['ip2'].values,df['ig1'].values,df['ig2'].values)
    return df

def get_dataframe_from_file(file: str, query: str):
    records = get_header(file)

    df = pd.DataFrame(records)

    df = add_path_and_mod_time(file, df)

    df = add_grid_column(df)


    hy_df = df.loc[df.nomvar== "HY"]

    if (query is None) == False:

        sub_df = df.query(query)

        # get metadata
        df = add_meta_to_query_results(df, sub_df, hy_df)


    # check HY count
    df = process_hy(hy_df, df)
    df = df[df.dltf==0]
    return df

def add_path_and_mod_time(file, df):
    f_mod_time = get_file_modification_time(file,rmn.FST_RO,'add_path_and_mod_time',StandardFileReaderError)
    df.loc[:,'path'] = file
    df.loc[:,'file_modification_time'] = f_mod_time
    return df

def get_header(file):
    unit = rmn.fstopenall(file)

    records = get_std_file_header(unit)

    rmn.fstcloseall(unit)
    return records

def add_meta_to_query_results(df, sub_df, hy_df):
    # get metadata
    # meta_df = df.query('nomvar in ["^>", ">>", "^^", "!!", "!!SF", "P0", "PT", "E1","PN"]')
    meta_df = df.loc[df.nomvar.isin(["^>", ">>", "^^", "!!", "!!SF", "P0", "PT", "E1","PN"])]
    # print(meta_df.query('grid in %s'%list(sub_df.grid.unique())) )
    # print(list(sub_df.grid.unique()))
    # print(sub_df)
    # subdfmeta = meta_df.query('grid in %s'%list(sub_df.grid.unique()))
    subdfmeta = meta_df.loc[meta_df.grid.isin(list(sub_df.grid.unique()))]
    # print(subdfmeta)

    if (not sub_df.empty) and (not subdfmeta.empty):
        df = pd.concat([sub_df,subdfmeta],ignore_index=True)
    elif (not sub_df.empty) and (subdfmeta.empty):
        df = sub_df
    elif sub_df.empty:
        df = sub_df

    if (not df.empty) and (not hy_df.empty):
        df = pd.concat([df,hy_df],ignore_index=True)
    return df

def process_hy(hy_df, df):
    # check HY count
    hy_count = hy_df.nomvar.count()
    if hy_count >= 1:
        if hy_count > 1:
            sys.stderr.write('More than one HY in this file! - UNKNOWN BEHAVIOR, continue at your own risk\n')

        grids = [ x for x in list(df.grid.unique()) if x != 'None' ]
        if len(grids):
            grid = grids[0]
            hy_df = df.loc[df.nomvar=="HY"]
            hy_df.loc[:,'grid'] = grid
        #remove HY
        df = df[df['grid']!= 'None']
        df = pd.concat([df,hy_df],ignore_index=True)
    return df

def _fstluk(key):
    return rmn.fstluk(int(key))['d']

def _fstluk_dask(key):
    return da.from_array(rmn.fstluk(int(key))['d'])

def add_numpy_data_column(df):
    vfstluk = np.vectorize(_fstluk,otypes='O')
    df.loc[:,'d'] = vfstluk(df['key'].values)
    # print(df[['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'key']])
    # df.loc[:,'d'] = None
    # print('dtype:\n',df['d'].dtype)
    # for i in df.index:
    #     print('-------------------------------------------------------------')
    #     print('d:\n',df.iloc[i].to_dict())
    #     print('d:\n',df.at[i,'d'])
    #     df.at[i,'d'] = None
    #     key = df.at[i,'key']
    #     print('luk rec:\n',rmn.fstluk(int(key)))
    #     print('luk d:\n',rmn.fstluk(int(key))['d'])
    #     df.at[i,'d'] = rmn.fstluk(int(key))['d']
    return df

def add_dask_data_column(df):
    vfstluk = np.vectorize(_fstluk_dask,otypes='O')
    df.loc[:,'d'] = vfstluk(df['key'].values)
    return df

def get_dataframe_from_file_and_load(file:str,query:str):
    df = get_dataframe_from_file(file,query)
    unit=rmn.fstopenall(file,rmn.FST_RO)
    df = add_numpy_data_column(df)
    # df['d'] = None
    # for i in df.index:
    #     df.at[i,'d'] = rmn.fstluk(int(df.at[i,'key']))['d']

    rmn.fstcloseall(unit)
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

def get_file_modification_time(path:str,mode:str,caller_class,exception_class):
    file = pathlib.Path(path)
    if not file.exists():
        return datetime.datetime.now()
    if (mode == rmn.FST_RO) and (not maybeFST(path)):
        raise exception_class(caller_class + 'not an fst standard file!')

    file_modification_time = time.ctime(os.path.getmtime(path))
    file_modification_time = datetime.datetime.strptime(file_modification_time, "%a %b %d %H:%M:%S %Y")

    return file_modification_time

def read_record(key:int):
    return rmn.fstluk(int(key))['d']

def strip_string_values(record):
    record['nomvar'] = record['nomvar'].strip()
    record['etiket'] = record['etiket'].strip()
    record['typvar'] = record['typvar'].strip()

def remove_extra_keys(record):
    for k in ['swa','dltf','ubc','lng','xtra1','xtra2','xtra3']:
        record.pop(k,None)

def get_2d_lat_lon(df:pd.DataFrame) -> pd.DataFrame:

    """get_2d_lat_lon Gets the latitudes and longitudes as 2d arrays associated with the supplied grids

    :return: a pandas Dataframe object containing the lat and lon meta data of the grids
    :rtype: pd.DataFrame
    :raises StandardFileError: no records to process
    """
    if  df.empty:
        raise StandardFileError('get_2d_lat_lon- no records to process')

    #remove record wich have X grid type
    without_x_grid_df = df.loc[df.grtyp != "X"]

    latlon_df = get_lat_lon(df)

    if latlon_df.empty:
        raise StandardFileError('get_2d_lat_lon - while trying to find [">>","^^"] - no data to process')

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
                # lat=grid['lat']
                # lon=grid['lon']
            except Exception:
                logger.warning('get_2d_lat_lon - no lat lon for this record')
                continue

            grid = rmn.gdll(g)

            tictic_df = latlon_df.loc[(latlon_df.nomvar=="^^") & (latlon_df.grid==row['grid'])].reset_index(drop=True)
            tactac_df = latlon_df.loc[(latlon_df.nomvar==">>") & (latlon_df.grid==row['grid'])].reset_index(drop=True)
            # lat_df = create_1row_df_from_model(tictic_df)

            tictic_df.at[0,'nomvar'] = 'LA'
            tictic_df.at[0,'d'] = grid['lat']
            tictic_df.at[0,'ni'] = grid['lat'].shape[0]
            tictic_df.at[0,'nj'] = grid['lat'].shape[1]
            tictic_df.at[0,'shape'] = grid['lat'].shape
            tictic_df.at[0,'file_modification_time'] = None
            # lon_df = create_1row_df_from_model(tactac_df)

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
    # print(latlon_df[['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3']])
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
        # file_modification_time = rec_df.iloc[0]['file_modification_time']
        # compare_modification_times(file_modification_time,path,rmn.FST_RO,caller,error_class)
        records = get_all_grid_metadata_fields_from_std_file(path)
        meta_df = pd.DataFrame(records)
        #print(meta_df[['nomvar','grid']])
        if meta_df.empty:
            # sys.stderr.write('get_grid_metadata_fields - no metatada in file %s\n'%path)
            return pd.DataFrame(dtype=object)
        grid_groups = rec_df.groupby(rec_df.grid)
        #for each grid in the current file
        for _,grid_df in grid_groups:
            this_grid = grid_df.iloc[0]['grid']
            if vertical_descriptors:
                #print('vertical_descriptors')
                vertical_df = meta_df.loc[(meta_df.nomvar.isin(["!!", "HY", "!!SF", "E1"])) & (meta_df.grid==this_grid)]
                df_list.append(vertical_df)
            if pressure:
                #print('pressure')
                pressure_df = meta_df.loc[(meta_df.nomvar.isin(["P0", "PT"])) & (meta_df.grid==this_grid)]
                df_list.append(pressure_df)
            if latitude_and_longitude:
                #print('lati and longi')
                latlon_df = meta_df.loc[(meta_df.nomvar.isin(["^>", ">>", "^^"])) & (meta_df.grid==this_grid)]
                #print(latlon_df)
                df_list.append(latlon_df)
                #print(latlon_df)

    if len(df_list):
        result = pd.concat(df_list,ignore_index=True)
        return result
    else:
        return pd.DataFrame(dtype=object)

def get_all_grid_metadata_fields_from_std_file(path):

    unit = rmn.fstopenall(path)
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
        #record['fstinl_params'] = None
        #del record['key']
        strip_string_values(record)
        #create a grid identifier for each record
        record['grid'] = get_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
        remove_extra_keys(record)
        record['path'] = path
        record['file_modification_time'] = get_file_modification_time(path,rmn.FST_RO,'get_all_meta_data_fields_from_std_file',StandardFileError)
        records.append(record)
    rmn.fstcloseall(unit)
    return records


def compare_modification_times(df_file_modification_time, path:str,mode:str, caller:str,error_class:Exception):
    file_modification_time = get_file_modification_time(path,mode,caller, error_class)
    # print( np.datetime64(df_file_modification_time)-np.datetime64(file_modification_time) == np.timedelta64(0))
    if np.datetime64(df_file_modification_time)-np.datetime64(file_modification_time) != np.timedelta64(0):
        #print(df_file_modification_time, file_modification_time,df_file_modification_time != file_modification_time)
        raise error_class(caller + ' - original file has been modified since the start of the operation, keys might be invalid - exiting!')



#df_file_modification_time = df.iloc[0]['file_modification_time']


# def get_all_record_keys(file_id, query):
#     if (query is None) == False:
#         keys = rmn.fstinl(file_id,**query)
#     else:
#         keys = rmn.fstinl(file_id)
#     return keys

# def get_records(keys,load_data):
#     # from .std_dec import get_grid_identifier,decode_metadata
#     records = []
#     if load_data:
#         for k in keys:
#             record = rmn.fstprm(k)
#             if record['dltf'] == 1:
#                 continue
#             del record['dltf']
#             record = rmn.fstluk(k)
#             # if array_container == 'dask.array':
#             #     record['d'] = da.from_array(record['d'])
#             # record['fstinl_params'] = None
#             # #del record['key']
#             # strip_string_values(record)
#             # #create a grid identifier for each record
#             # record['grid'] = get_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
#             # remove_extra_keys(record)

#             # if decode:
#             #     record['stacked'] = False
#             #     record.update(decode_metadata(record['nomvar'],record['etiket'],record['dateo'],record['datev'],record['deet'],record['npas'],record['datyp'],record['ip1'],record['ip2'],record['ip3']))
#             records.append(record)
#     else:
#         for k in keys:
#             record = rmn.fstprm(k)
#             if record['dltf'] == 1:
#                 continue
#             del record['dltf']
#             # record['fstinl_params'] = {
#             #     'datev':record['datev'],
#             #     'etiket':record['etiket'],
#             #     'ip1':record['ip1'],
#             #     'ip2':record['ip2'],
#             #     'ip3':record['ip3'],
#             #     'typvar':record['typvar'],
#             #     'nomvar':record['nomvar']
#             # }
#             # key  = record['key']
#             # def read_record(array_container,key):
#             #     if array_container == 'dask.array':
#             #         return da.from_array(rmn.fstluk(key)['d'])
#             #     elif array_container == 'numpy':
#             #         return rmn.fstluk(key)['d']

#             # record['d'] = (read_record,array_container,key)

#             # #del record['key'] #i don't know if we need
#             # strip_string_values(record)
#             # #create a grid identifier for each record
#             # record['grid'] = get_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
#             # remove_extra_keys(record)

#             # if decode:
#             #     record['stacked'] = False
#             #     record.update(decode_metadata(record['nomvar'],record['etiket'],record['dateo'],record['datev'],record['deet'],record['npas'],record['datyp'],record['ip1'],record['ip2'],record['ip3']))

#             records.append(record)

#         # if decode:
#         #     records = parallelize_records(records,massage_and_decode_record)
#         #     # for i in range(len(records)):
#         #     #     records[i] = massage_and_decode_record(records[i])
#         # else:
#         #     records = parallelize_records(records,massage_record)
#             # for i in range(len(records)):
#             #     records[i] = massage_record(records[i])
#     return records


    # if dateo == 0:
    #     return str(dateo)
    # dt = rmn_dateo_to_datetime_object(dateo)
    # return dt.strftime('%Y%m%d %H%M%S')

# def rmn_dateo_to_datetime_object(dateo:int):
#     import datetime
#     res = rmn.newdate(rmn.NEWDATE_STAMP2PRINT, dateo)
#     date_str = str(res[0])
#     if res[1]:
#         time_str = str(res[1])[:-2]
#     else:
#         time_str = '000000'
#     date_str = "".join([date_str,time_str])
#     return datetime.datetime.strptime(date_str, '%Y%m%d%H%M%S')





# def convert_rmnkind_to_string(ip1_kind):
#     return constants.KIND_DICT[int(ip1_kind)]
