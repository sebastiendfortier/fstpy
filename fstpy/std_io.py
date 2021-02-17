# -*- coding: utf-8 -*-
import rpnpy.librmn.all as rmn
from .logger_config import logger
import os.path
import datetime
import time
import pathlib
import dask.array as da
from .exceptions import StandardFileError
import sys
import pandas as pd

def open_fst(path:str, mode:str, caller_class:str, error_class:Exception):
    file_modification_time = get_file_modification_time(path,mode,caller_class,error_class)
    file_id = rmn.fstopenall(path, mode)
    logger.info(caller_class + ' - opening file %s', path)
    return file_id, file_modification_time

def close_fst(file_id:int, file:str,caller_class:str):
    logger.info(caller_class + ' - closing file %s', file)
    rmn.fstcloseall(file_id)

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
    
def get_all_record_keys(file_id, subset):
    if (subset is None) == False:
        keys = rmn.fstinl(file_id,**subset)
    else:
        keys = rmn.fstinl(file_id)
    return keys  

def get_records(keys,load_data,decode,path,file_modification_time,array_container):
    from .std_dec import create_grid_identifier,decode_meta_data
    records = []    
    if load_data:
        for k in keys:
            record = rmn.fstprm(k)
            if record['dltf'] == 1:
                continue
            record = rmn.fstluk(k)
            if array_container == 'dask.array':
                record['d'] = da.from_array(record['d'])
            elif array_container == 'numpy':
                record['d'] = record['d']    

            del record['dltf']
            record['fstinl_params'] = None
            #del record['key']
            strip_string_values(record)
            #create a grid identifier for each record
            record['grid'] = create_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
            remove_extra_keys(record)
            record['path'] = path
            record['file_modification_time'] = file_modification_time
            if decode:
                record['stacked'] = False
                record.update(decode_meta_data(record['nomvar'],record['etiket'],record['dateo'],record['datev'],record['deet'],record['npas'],record['datyp'],record['ip1'],record['ip2'],record['ip3']))
            records.append(record)
    else:    
        for k in keys:
            record = rmn.fstprm(k)
            if record['dltf'] == 1:
                continue
            del record['dltf']
            record['fstinl_params'] = {
                'datev':record['datev'],
                'etiket':record['etiket'],
                'ip1':record['ip1'],
                'ip2':record['ip2'],
                'ip3':record['ip3'],
                'typvar':record['typvar'],
                'nomvar':record['nomvar']
            }
            key  = record['key']
            def read_record(array_container,key):
                if array_container == 'dask.array':
                    return da.from_array(rmn.fstluk(key)['d'])
                elif array_container == 'numpy':
                    return rmn.fstluk(key)['d']

            record['d'] = (read_record,array_container,key)

            #del record['key'] #i don't know if we need
            strip_string_values(record)
            #create a grid identifier for each record
            record['grid'] = create_grid_identifier(record['nomvar'],record['ip1'],record['ip2'],record['ig1'],record['ig2'])
            remove_extra_keys(record)
            record['path'] = path
            record['file_modification_time'] = file_modification_time
            if decode:
                record['stacked'] = False
                record.update(decode_meta_data(record['nomvar'],record['etiket'],record['dateo'],record['datev'],record['deet'],record['npas'],record['datyp'],record['ip1'],record['ip2'],record['ip3']))
                
            records.append(record)

                
    return records   



def strip_string_values(record):
    record['nomvar'] = record['nomvar'].strip()
    record['etiket'] = record['etiket'].strip()
    record['typvar'] = record['typvar'].strip()

def remove_extra_keys(record):
    del record['swa']
    del record['ubc']
    del record['lng']
    del record['xtra1']
    del record['xtra2']
    del record['xtra3']

def get_2d_lat_lon(df:pd.DataFrame) -> pd.DataFrame:
    from .utils import validate_df_not_empty,create_1row_df_from_model
    from .dataframe_utils import select,zap
    from .exceptions import StandardFileReader
    
    """get_2d_lat_lon Gets the latitudes and longitudes as 2d arrays associated with the supplied grids

    :return: a pandas Dataframe object containing the lat and lon meta data of the grids
    :rtype: pd.DataFrame
    :raises StandardFileError: no records to process
    """
    validate_df_not_empty(df,'get_2d_lat_lon',StandardFileError)
    #remove record wich have X grid type
    without_x_grid_df = select(df,'grtyp != "X"',no_fail=True)

    latlon_df = get_lat_lon(df)

    validate_df_not_empty(latlon_df,'get_2d_lat_lon - while trying to find [">>","^^"]',StandardFileError)
    
    no_meta_df = select(without_x_grid_df,'nomvar not in %s'%StandardFileReader.meta_data, no_fail=True)

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
            tictic_df = select(latlon_df,'(nomvar=="^^") and (grid=="%s")'%row['grid'],no_fail=True)
            tactac_df = select(latlon_df,'(nomvar==">>") and (grid=="%s")'%row['grid'],no_fail=True)
            lat_df = create_1row_df_from_model(tictic_df)
            lat_df = zap(lat_df, mark=False,nomvar='LA')
            lat_df.at[0,'d'] = grid['lat']
            lat_df.at[0,'ni'] = grid['lat'].shape[0]
            lat_df.at[0,'nj'] = grid['lat'].shape[1]
            lat_df.at[0,'shape'] = grid['lat'].shape
            lon_df = create_1row_df_from_model(tactac_df)
            lon_df = zap(lon_df, mark=False, nomvar='LO')
            lon_df.at[0,'d'] = grid['lon']
            lon_df.at[0,'ni'] = grid['lon'].shape[0]
            lon_df.at[0,'nj'] = grid['lon'].shape[1]
            lon_df.at[0,'shape'] = grid['lon'].shape
            latlons.append(lat_df)
            latlons.append(lon_df)

        rmn.fstcloseall(file_id)
    latlon = pd.concat(latlons)
    latlon.reset_index(inplace=True,drop=True)
    return latlon

def get_lat_lon(df):
    return get_meta_data_fields(df,'get_lat_lon',StandardFileError,pressure=False, vertical_descriptors=False)

def get_meta_data_fields(df,caller,error_class,latitude_and_longitude=True, pressure=True, vertical_descriptors=True):
    from .std_reader import StandardFileReader,load_data
    path_groups = df.groupby(df.path)
    meta_dfs = []
    #for each files in the df
    for _, rec_df in path_groups:
        path = rec_df.iloc[0]['path']
        file_modification_time = rec_df.iloc[0]['file_modification_time']
        compare_modification_times(file_modification_time,path,rmn.FST_RO,caller,error_class)
        records = get_all_meta_data_fields_from_std_file(path, rec_df)
        meta_df = pd.DataFrame(records)
        #print(meta_df[['nomvar','grid']])
        if meta_df.empty:
            sys.stderr.write('get_meta_data_fields - no metatada in file %s'%path)
            return df
        grid_groups = rec_df.groupby(rec_df.grid)
        #for each grid in the current file
        for _,grid_df in grid_groups:
            this_grid = grid_df.iloc[0]['grid']
            if vertical_descriptors:
                #print('vertical_descriptors')
                vertical_df = meta_df.query('(nomvar in ["!!", "HY", "!!SF", "E1"]) and (grid=="%s")'%this_grid)
                meta_dfs.append(vertical_df)
            if pressure:
                #print('pressure')
                pressure_df = meta_df.query('(nomvar in ["P0", "PT"]) and (grid=="%s")'%this_grid)
                meta_dfs.append(pressure_df)
            if latitude_and_longitude:
                #print('lati and longi')
                latlon_df = meta_df.query('(nomvar in ["^>", ">>", "^^"]) and (grid=="%s")'%this_grid)
                #print(latlon_df)
                meta_dfs.append(latlon_df)
                #print(latlon_df)
              
    if len(meta_dfs):
        result = pd.concat(meta_dfs)
        return result
    else:
        return pd.DataFrame(dtype=object)

def get_all_meta_data_fields_from_std_file(path):
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
    keys = lat_keys + lon_keys + tictac_keys + toctoc_keys + hy_keys + sf_keys + e1_keys + p0_keys + pt_keys
    records = [rmn.fstluk(key) for key in keys]
    rmn.fstcloseall(unit)  
    return records


def compare_modification_times(df_file_modification_time, path:str,mode:str, caller:str,error_class:Exception):
    file_modification_time = get_file_modification_time(path,mode,caller, error_class)
    if df_file_modification_time != file_modification_time:
        raise error_class(caller + ' - original file has been modified since the start of the operation, keys might be invalid - exiting!')
#df_file_modification_time = df.iloc[0]['file_modification_time']






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





# def convert_rmnkind_to_string(kind):
#     return constants.KIND_DICT[int(kind)]

  
