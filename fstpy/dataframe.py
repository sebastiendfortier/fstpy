# -*- coding: utf-8 -*-
import concurrent.futures
import datetime
from fstpy import DATYP_DICT
import multiprocessing as mp

import dask.array as da
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

# from .dataframe_utils import add_empty_columns
from .exceptions import StandardFileError
from .logger_config import logger
from .std_dec import (convert_rmndate_to_datetime, 
                      get_unit_and_description, is_surface,
                      level_type_follows_topography, parse_etiket)



def add_decoded_columns( df:pd.DataFrame,decode_metadata:bool,array_container:str='numpy', attributes_to_decode:list=['flags','etiket','vctype','unit','dateo','datev','forecast_hour','ip2','ip3','datyp','level_info']) -> pd.DataFrame:
    """Adds basic and decoded columns to the dataframe. 

    :param df: input dataframe
    :type df: pd.DataFrame
    :param decode_metadata: if true, decodes attributes specified in attributes_to_decode list 
    :type decode_metadata: bool
    :param array_container: array container, defaults to 'numpy'
    :type array_container: str, optional
    :param attributes_to_decode: attributes to decode, defaults to ['flags','etiket','unit','dateo','datev','forecast_hour','ip2','ip3','datyp','level_info']
    :type attributes_to_decode: list, optional
    :return: dataframe with decoded columns
    :rtype: pd.DataFrame
    """
    df = post_process_dataframe(df)
    
    df = add_composite_columns(df,decode_metadata,array_container)
    # df = parallel_add_composite_columns_tr(df,decode_metadata,array_container,attributes_to_decode,n_cores=min(mp.cpu_count(),len(df.index),1))   
    
    return df

def clean_dataframe(df,decode_metadata,attributes_to_decode=['flags','etiket','unit','dateo','datev','forecast_hour','ip1','ip2','ip3','datyp','level_info']):
    # df = convert_df_dtypes(df,decode_metadata,attributes_to_decode)

    df = reorder_columns(df)  

    df = sort_dataframe(df)
    return df


def create_shape(ni,nj):
    return (ni,nj)

def add_shape_column(df):
    vmake_shape = np.vectorize(create_shape,otypes=['object'])
    df['shape'] = vmake_shape(df['ni'],df['nj'])
    return df




def create_data(d,key:int,array_container:str):
    if not ((isinstance(d,np.ndarray)) or (isinstance(d,da.core.Array))):
        d = (array_container,int(key))
    return d

def add_data_column(df,array_container):
    vcreate_data = np.vectorize(create_data,otypes=['object'])    
    df['d'] = vcreate_data(df['d'],df['key'],array_container)
    return df


def add_parsed_etiket_columns(df):
    # df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = parse_etiket(df.at[i,'etiket'])
    vparse_etiket = np.vectorize(parse_etiket,otypes=['str','str','str','str'])    
    df['label'],df['run'],df['implementation'],df['ensemble_member'] = vparse_etiket(df['etiket'])
    return df

def add_unit_and_description_columns(df):
    # df.at[i,'unit'],df.at[i,'description']=get_unit_and_description(df.at[i,'nomvar'])
    vget_unit_and_description = np.vectorize(get_unit_and_description,otypes=['str','str'])    
    df['unit'],df['description'] = vget_unit_and_description(df['nomvar'])
    return df

def add_flags_columns(df):
    df['unit_converted'] = False
    df['zapped'] = False
    df['filtered'] = False
    df['interpolated'] = False
    df['bounded'] = False
    df['ensemble_extra_info'] = False
    df['multiple_modifications'] = False
    return df

def add_decoded_date_column(df,attr='dateo'):
    vconvert_rmndate_to_datetime = np.vectorize(convert_rmndate_to_datetime,otypes=['datetime64'])  
    if attr == 'dateo':  
        df['date_of_observation'] = vconvert_rmndate_to_datetime(df['dateo'])
    else:    
        df['date_of_validity'] = vconvert_rmndate_to_datetime(df['datev'])
    return df

def create_forecast_hour(deet,npas):
    return datetime.timedelta(seconds=int(npas * deet))

def add_forecast_hour_column(df):
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vcreate_forecast_hour = np.vectorize(create_forecast_hour,otypes=['timedelta64'])    
    df['forecast_hour'] = vcreate_forecast_hour(df['deet'],df['npas'])
    return df

# def decode_ip1(ip1):
#     i1,_,_ = rmn.DecodeIp(ip1,0,0) 
#     pkind = '' if i1.kind in [-1,3,15,17] else rmn.kindToString(i1.kind).strip()
#     return i1.v1,i1.kind,pkind

def decode_ip2(ip2):
    _,i2,_ = rmn.DecodeIp(0,ip2,0) 
    pkind = '' if i2.kind in [-1,3,15,17] else rmn.kindToString(i2.kind).strip()
    return i2.v1,i2.kind,pkind

def decode_ip3(ip3):
    _,_,i3 = rmn.DecodeIp(0,0,ip3) 
    pkind = '' if i3.kind in [-1,3,15,17] else rmn.kindToString(i3.kind).strip()
    return i3.v1,i3.kind,pkind

# def add_decoded_ip1_columns(df):
#     vdecode_ip1 = np.vectorize(decode_ip1,otypes=['float32','int32','str'])  
#     df['level'],df['ip1_kind'],df['ip1_pkind'] = vdecode_ip1(df['ip1'])
#     return df

def add_decoded_ip2_columns(df):
    vdecode_ip2 = np.vectorize(decode_ip2,otypes=['float32','int32','str'])  
    df['ip2_dec'],df['ip2_kind'],df['ip2_pkind'] = vdecode_ip2(df['ip2'])
    return df

def add_decoded_ip3_columns(df):
    vdecode_ip3 = np.vectorize(decode_ip3,otypes=['float32','int32','str'])  
    df['ip3_dec'],df['ip3_kind'],df['ip3_pkind'] = vdecode_ip3(df['ip3'])    
    return df

def create_data_type_str(datyp):
    return DATYP_DICT[datyp]

def add_data_type_str_column(df):
    vcreate_data_type_str = np.vectorize(create_data_type_str,otypes=['str'])  
    df['data_type_str'] = vcreate_data_type_str(df['datyp'])    
    return df

def create_level_info(ip1):
    i1,_,_ = rmn.DecodeIp(ip1,0,0) 
    ip1_pkind = '' if i1.kind in [-1,3,15,17] else rmn.kindToString(i1.kind).strip()
    level=i1.v1
    ip1_kind = i1.kind
    surface = is_surface(ip1_kind,level)
    follow_topography = level_type_follows_topography(ip1_kind)
    return level,ip1_kind,ip1_pkind,surface,follow_topography

def add_level_info_columns(df):
    vcreate_level_info = np.vectorize(create_level_info,otypes=['float32','int32','str','bool','bool'])  
    df['level'],df['ip1_kind'],df['ip1_pkind'],df['surface'],df['follow_topography'] = vcreate_level_info(df['ip1'])    
    return df

def add_composite_columns(df,decode,array_container, attributes_to_decode=['flags','etiket','unit','dateo','datev','forecast_hour','ip2','ip3','datyp','level_info']):
    
    df = add_data_column(df,array_container)
    # df = add_grid_column(df)
    df = add_shape_column(df)
    
    if decode:
        if 'etiket' in attributes_to_decode:    
            df = add_parsed_etiket_columns(df)

        if 'unit' in attributes_to_decode:
            df = add_unit_and_description_columns(df)

        if 'dateo' in attributes_to_decode:    
            df = add_decoded_date_column(df,'dateo')

        if 'datev' in attributes_to_decode:
            df = add_decoded_date_column(df,'datev')  

        if 'forecast_hour' in attributes_to_decode:
            df = add_forecast_hour_column(df) 

        if 'ip2' in attributes_to_decode:
            df = add_decoded_ip2_columns(df)     

        if 'ip3' in attributes_to_decode:
            df = add_decoded_ip3_columns(df)   

        if 'datyp' in attributes_to_decode:    
            df = add_data_type_str_column(df)   

        if ('level_info' in  attributes_to_decode): 
            df = add_level_info_columns(df)                

        if 'flags' in attributes_to_decode:
            df = add_flags_columns(df)

  

    if decode and ('level_info' in attributes_to_decode):
        df = set_vertical_coordinate_type(df)    
    return df

def add_unit_column(df):
    if 'unit' not in df.columns:
        df['unit'] = None
    if 'unit_converted' not in df.columns:    
        df['unit_converted'] = None
    if 'description' not  in df.columns:
        df['description'] = None    
    for i in df.index:
        df.at[i,'unit'],df.at[i,'description']=get_unit_and_description(df.at[i,'nomvar'])
    return df    
    
def add_empty_columns(df, columns, init, dtype_str):
    for col in columns:
        if col not in df.columns:
            df.insert(len(df.columns),col,init)
            df = df.astype({col:dtype_str})
    return df         

def post_process_dataframe(df):
    if 'dltf' in df.columns:
        df = df[df.dltf == 0]
    df.drop(columns=['dltf'], inplace=True,errors='ignore')
    if 'd' not in df.columns:
        df['d']=None
        
    return df

# def convert_df_dtypes(df,decoded,attributes_to_decode=['ip1','ip2','ip3']):
#     if not df.empty:
#         if decoded:    
#             if ('ip1' in attributes_to_decode) and ('level' in df.columns) and ('ip1_kind' in df.columns):
#                 df = df.astype({'level':'float32','ip1_kind':'int32'})
#             if ('ip2' in attributes_to_decode) and ('ip2_dec' in df.columns) and ('ip2_kind' in df.columns):    
#                 df = df.astype({'ip2_dec':'float32','ip2_kind':'int32'})
#             if ('ip3' in attributes_to_decode) and ('ip3_dec' in df.columns) and ('ip3_kind' in df.columns):    
#                 df = df.astype({'ip3_dec':'float32','ip3_kind':'int32'})
#     return df      

def reorder_columns(df) -> pd.DataFrame:
    ordered = ['nomvar','typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas','datyp', 'nbits' , 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    if df.empty:
        return df
    all_columns = set(df.columns.to_list())    

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns)) 

    df = df[ordered]    
    return df    

def sort_dataframe(df) -> pd.DataFrame:
    if df.empty:
        return df
    if ('grid' in df.columns) and ('forecast_hour' in df.columns)and ('nomvar' in df.columns) and ('level' in df.columns): 
        df.sort_values(by=['grid','forecast_hour','nomvar','level'],ascending=False,inplace=True)
    else:     
        df.sort_values(by=['nomvar'],ascending=False,inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df    

def set_vertical_coordinate_type(df) -> pd.DataFrame:
    from fstpy import VCTYPES
    newdfs=[]
    df['vctype'] = 'UNKNOWN'
    grid_groups = df.groupby(df.grid)
    #print(df[['ip1','ip2','ig1','ig2']])
    for _, grid in grid_groups:
        toctoc, p0, e1, pt, hy, sf, vcode = get_meta_fields_exists(grid)
        ip1_kind_groups = grid.groupby(grid.ip1_kind)
        for _, ip1_kind_group in ip1_kind_groups:
            #these ip1_kinds are not defined
            without_meta = ip1_kind_group.query('(ip1_kind not in [-1,3,6])')
            if not without_meta.empty:
                #logger.debug(without_meta.iloc[0]['nomvar'])
                ip1_kind = without_meta.iloc[0]['ip1_kind']
                ip1_kind_group['vctype'] = 'UNKNOWN'
                #vctype_dict = {'ip1_kind':ip1_kind,'toctoc':toctoc,'P0':p0,'E1':e1,'PT':pt,'HY':hy,'SF':sf,'vcode':vcode}
                vctyte_df = VCTYPES.query('(ip1_kind==%s) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%s)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,vcode))
                if not vctyte_df.empty:
                    if len(vctyte_df.index)>1:
                        logger.warning('set_vertical_coordinate_type - more than one match!!!')
                    ip1_kind_group['vctype'] = vctyte_df.iloc[0]['vctype']
            newdfs.append(ip1_kind_group)

    df = pd.concat(newdfs)
    return df    

def get_meta_fields_exists(grid):
    toctoc = grid.query('nomvar=="!!"')
    if not toctoc.empty:
        vcode = toctoc.iloc[0]['ig1']
        toctoc = True
    else:
        vcode = -1
        toctoc = False
    p0 = meta_exists(grid,"P0")
    e1 = meta_exists(grid,"E1")
    pt = meta_exists(grid,"PT")
    hy = meta_exists(grid,"HY")
    sf = meta_exists(grid,"!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode

def meta_exists(grid, nomvar) -> bool:
    df = grid.query('nomvar=="%s"'%nomvar)
    return not df.empty

def remove_from_df(df_to_remove_from:pd.DataFrame, df_to_remove:pd.DataFrame) -> pd.DataFrame:
    """Removes a dataframe from another. 
    As an example, if you selected a variable from a dataframe and want to remove it from the original dataframe.

    :param df_to_remove_from: original dataframe we want to modify
    :type df_to_remove_from: pd.DataFrame
    :param df_to_remove: the dataframe containing records we want to remove
    :type df_to_remove: pd.DataFrame
    :return: the dataframe resulting in the removal of the other dataframes rows
    :rtype: pd.DataFrame
    """
    columns = df_to_remove.columns.values.tolist()
    columns.remove('d')
    #columns.remove('fstinl_params')
    tmp_df = pd.concat([df_to_remove_from, df_to_remove]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df)
    tmp_df.reset_index(inplace=True,drop=True) 
    return tmp_df

def get_intersecting_levels(df:pd.DataFrame, nomvars:list) -> pd.DataFrame:
    """Gets the records of all intersecting levels for nomvars in list.
    if TT,UU and VV are in the list, the output dataframe will contain all 3 
    varaibles at all the intersectiong levels between the 3 variables 

    :param df: input dataframe
    :type df: pd.DataFrame
    :param nomvars: list of nomvars to select
    :type nomvars: list
    :raises StandardFileError: if a problem occurs this exception will be raised
    :return: dataframe subset
    :rtype: pd.DataFrame
    """
    #logger.debug('1',df[['nomvar','surface','level','ip1_kind']])
    if len(nomvars)<=1:
        logger.error('get_intersecting_levels - not enough nomvars to process')
        raise StandardFileError('not enough nomvars to process')
    firstdf = df.query( 'nomvar == "%s"' % nomvars[0])
    if df.empty:
        logger.error('get_intersecting_levels - no records to intersect')
        raise StandardFileError('get_intersecting_levels - no records to intersect')
    common_levels = set(firstdf.level.unique())
    query_strings = []
    for name in nomvars:
        current_query = 'nomvar == "%s"' % name
        currdf = df.query('%s' % current_query)
        levels = set(currdf.level.unique())
        common_levels = common_levels.intersection(levels)
        query_strings.append(current_query)
    query_strings = " or ".join(tuple(query_strings))
    query_res = df.query('(%s) and (level in %s)' % (query_strings, list(common_levels)))
    if query_res.empty:
        logger.error('get_intersecting_levels - no intersecting levels found')
        return
    return query_res    

def parallel_add_composite_columns_tr(df, decode_metadata, array_container, attributes_to_decode,n_cores):
    dataframes = []
    df_split = np.array_split(df, n_cores)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_cores) as executor:
        # Start the load operations and mark each future with its URL
        #df,decode,array_container
        future_to_df = {executor.submit(add_composite_columns, dfp, decode_metadata, array_container, attributes_to_decode): dfp for dfp in df_split}
        for future in concurrent.futures.as_completed(future_to_df):
            dfp = future_to_df[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (dfp, exc))
            else:
                dataframes.append(data)
                #print('%r %s d is %d ' % (d, data, len(data)))
    df = pd.concat(dataframes)
    df = sort_dataframe(df)
    return df

# def parallel_add_composite_columns(df, decode_metadata, array_container, n_cores):
#     df_split = np.array_split(df, n_cores)
#     df_with_params = list(zip(df_split,itertools.repeat(decode_metadata),itertools.repeat(array_container)))
#     pool = mp.Pool(n_cores)
#     df = pd.concat(pool.starmap(add_composite_columns, df_with_params))
#     pool.close()
#     pool.join()
#     return df

# def create_dataframe(file,decode_metadata,load_data,subset) -> pd.DataFrame:
#     path = os.path.abspath(file)
#     file_id, file_modification_time = open_fst(path,rmn.FST_RO,'StandardFileReader',StandardFileReaderError)
#     df = read_and_fill_dataframe(file_id,path, load_data,subset,decode_metadata)
#     close_fst(file_id,path,'StandardFileReader')
#     df['file_modification_time'] = file_modification_time
#     df['path'] = path
#     return df            

# def read_and_fill_dataframe(file_id,load_data,subset,decode_metadata) ->pd.DataFrame:
#     """reads the meta data of an fst file and puts it into a pandas dataframe  

#     :return: dataframe of records in file  
#     :rtype: pd.DataFrame  
#     """
#     #get the basic rmnlib dataframe
#     df = get_all_records_from_file_and_format(file_id,load_data,subset)

#     df = convert_df_dtypes(df,decode_metadata)

#     df = reorder_columns(df)  

#     df = sort_dataframe(df)
#     return df

# def get_all_records_from_file_and_format(file_id,load_data,subset):
    
#     keys = get_all_record_keys(file_id, subset)

#     records = get_records(keys,load_data)

#     #create a dataframe correspondinf to the fst file
#     df = pd.DataFrame(records)

    

    # return df    
# def resize_data(df:pd.DataFrame, dim1:int,dim2:int) -> pd.DataFrame:
#     from .std_reader import load_data
#     df = load_data(df)
#     for i in df.index:
#         df.at[i,'d'] = df.at[i,'d'][:dim1,:dim2].copy(deep=True)
#         df.at[i,'shape']  = df.at[i,'d'].shape
#         df.at[i,'ni'] = df.at[i,'shape'][0]
#         df.at[i,'nj'] = df.at[i,'shape'][1]
#     df = sort_dataframe(df)    
#     return df




# def add_path_and_modification_time(df, file, file_modification_time):
#     # create the file column and init
#     df['path'] = file
#     #create the file mod time column and init
#     df['file_modification_time'] = file_modification_time
#     return df



# def remove_data_fields(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.query('nomvar in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]')
#     return df    

# def add_extra_cols(df):
#     df = add_empty_columns(df, ['ip1_kind'], 0, 'int32')
#     df = add_empty_columns(df, ['level'],np.nan,'float32')
#     df = add_empty_columns(df, ['surface','follow_topography','dirty','unit_converted'],False,'bool')
#     df = add_empty_columns(df, ['date_of_observation','date_of_validity','forecast_hour'],None,'datetime64')
#     df = add_empty_columns(df, ['vctype','ip1_pkind','data_type_str','grid','run','implementation','ensemble_member','label','unit'],'','O')
#     df = fill_columns(df)
#     return df   

# def add_empty_columns(df, columns, init, dtype_str):
#     for col in columns:
#         df.insert(len(df.columns),col,init)
#         df = df.astype({col:dtype_str})
#     return df       

# def fill_columns(df:pd.DataFrame):
#     """fill_columns adds columns that are'nt in the fst file

#     :param records: DataFrame to add colmns to
#     :type records: pd.DataFrame
#     """

#     #add columns
#     meter_levels = np.arange(0.,10.5,.5).tolist()
#     for i in df.index:
#         nomvar = df.at[i,'nomvar']
#         #find unit name value for this nomvar
#         df = std_io.get_unit_and_description(df, i, nomvar)
#         #get level and ip1_kind
#         df = set_level_and_ip1_kind(df, i) #float("%.6f"%-1) if df.at[i,'ip1_kind'] == -1 else float("%.6f"%level)
#         #create a real date of observation
#         df.at[i,'date_of_observation'] = std_io.convert_rmndate_to_datetime(int(df.at[i,'dateo']))
#         #create a printable date of validity
#         df.at[i,'date_of_validity'] = std_io.convert_rmndate_to_datetime(int(df.at[i,'datev']))
#         #create a printable ip1_kind for voir
#         df.at[i,'ip1_pkind'] = convert_rmnip1_kind_to_string(df.at[i,'ip1_kind'])
#         #calculate the forecast hour
#         df.at[i,'forecast_hour'] = df.at[i,'npas'] * df.at[i,'deet'] / 3600.
#         #create a printable data type for voir
#         df.at[i,'data_type_str'] = constants.DATYP_DICT[df.at[i,'datyp']]
#         #create a grid identifier for each record
#         df.at[i,'grid'] = std_io.create_grid_identifier(df.at[i,'nomvar'],df.at[i,'ip1'],df.at[i,'ip2'],df.at[i,'ig1'],df.at[i,'ig2'])
#         #logger.debug(df.at[i,'ip1_kind'],df.at[i,'level'])
#         #set surface flag for surface levels
#         std_io.set_surface(df, i, meter_levels)
#         std_io.set_follow_topography(df, i)
#         df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = std_io.parse_etiket(df.at[i,'etiket'])
#         df.at[i,'d'] = (rmn.fstluk,int(df.at[i,'key']))
#     return df



# def set_level_and_ip1_kind(df, i):
#     level, ip1_kind = std_io.get_level_and_ip1_kind(df.at[i,'ip1'])
#     # level_ip1_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(df.at[i,'ip1']))
#     # ip1_kind = level_ip1_kind[1]
#     # level = level_ip1_kind[0]
#     df.at[i,'ip1_kind'] = int(ip1_kind)
#     df.at[i,'level'] = level #float("%.6f"%-1) if df.at[i,'ip1_kind'] == -1 else float("%.6f"%level)
#     return df    

# def strip_string_columns(df):
#     if ('etiket' in df.columns) and ('nomvar' in df.columns) and ('typvar' in df.columns):
#         df['etiket'] = df['etiket'].str.strip()
#         df['nomvar'] = df['nomvar'].str.strip()
#         df['typvar'] = df['typvar'].str.strip()
#     return df    
