# -*- coding: utf-8 -*-
import fstpy.std_io as std_io
import rpnpy.librmn.all as rmn
import pandas as pd
import os
import sys
from .exceptions import StandardFileReaderError
from .constants import VCTYPES
from .logger_config import logger


def create_dataframe(file,decode_meta_data,load_data,subset,array_container) -> pd.DataFrame:
    path = os.path.abspath(file)
    file_id, file_modification_time = std_io.open_fst(path,rmn.FST_RO,'StandardFileReader',StandardFileReaderError)
    df = read_and_fill_dataframe(file_id,path, file_modification_time, load_data,subset,decode_meta_data,array_container)
    std_io.close_fst(file_id,path,'StandardFileReader')
    return df            

def read_and_fill_dataframe(file_id,filenames,file_modification_time, load_data,subset,decode_meta_data,array_container) ->pd.DataFrame:
    """reads the meta data of an fst file and puts it into a pandas dataframe  

    :return: dataframe of records in file  
    :rtype: pd.DataFrame  
    """
    #get the basic rmnlib dataframe
    df = get_all_records_from_file_and_format(file_id,filenames,decode_meta_data,file_modification_time, load_data,subset,array_container)

    df = reorder_columns(df)  

    df = sort_dataframe(df)
    return df

def get_all_records_from_file_and_format(file_id,filenames,decode_meta_data,file_modification_time, load_data,subset,array_container):
    keys = std_io.get_all_record_keys(file_id, subset)

    records = std_io.get_records(keys,load_data,decode_meta_data,filenames,file_modification_time,array_container)

    #create a dataframe correspondinf to the fst file
    df = pd.DataFrame(records)

    df = convert_df_dtypes(df,decode_meta_data)

    return df    






# def add_path_and_modification_time(df, file, file_modification_time):
#     # create the file column and init
#     df['path'] = file
#     #create the file mod time column and init
#     df['file_modification_time'] = file_modification_time
#     return df

def remove_meta_data_fields(df: pd.DataFrame) -> pd.DataFrame:
    for meta in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]:
        df = df[df.nomvar != meta]
    return df

def remove_data_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.query('nomvar in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]')
    return df    

# def add_extra_cols(df):
#     df = add_empty_columns(df, ['kind'], 0, 'int32')
#     df = add_empty_columns(df, ['level'],np.nan,'float32')
#     df = add_empty_columns(df, ['surface','follow_topography','dirty','unit_converted'],False,'bool')
#     df = add_empty_columns(df, ['pdateo','pdatev','fhour'],None,'datetime64')
#     df = add_empty_columns(df, ['vctype','pkind','pdatyp','grid','run','implementation','ensemble_member','label','unit'],'','O')
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
#         #get level and kind
#         df = set_level_and_kind(df, i) #float("%.6f"%-1) if df.at[i,'kind'] == -1 else float("%.6f"%level)
#         #create a real date of observation
#         df.at[i,'pdateo'] = std_io.convert_rmndate_to_datetime(int(df.at[i,'dateo']))
#         #create a printable date of validity
#         df.at[i,'pdatev'] = std_io.convert_rmndate_to_datetime(int(df.at[i,'datev']))
#         #create a printable kind for voir
#         df.at[i,'pkind'] = convert_rmnkind_to_string(df.at[i,'kind'])
#         #calculate the forecast hour
#         df.at[i,'fhour'] = df.at[i,'npas'] * df.at[i,'deet'] / 3600.
#         #create a printable data type for voir
#         df.at[i,'pdatyp'] = constants.DATYP_DICT[df.at[i,'datyp']]
#         #create a grid identifier for each record
#         df.at[i,'grid'] = std_io.create_grid_identifier(df.at[i,'nomvar'],df.at[i,'ip1'],df.at[i,'ip2'],df.at[i,'ig1'],df.at[i,'ig2'])
#         #logger.debug(df.at[i,'kind'],df.at[i,'level'])
#         #set surface flag for surface levels
#         std_io.set_surface(df, i, meter_levels)
#         std_io.set_follow_topography(df, i)
#         df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = std_io.parse_etiket(df.at[i,'etiket'])
#         df.at[i,'d'] = (rmn.fstluk,int(df.at[i,'key']))
#     return df



# def set_level_and_kind(df, i):
#     level, kind = std_io.get_level_and_kind(df.at[i,'ip1'])
#     # level_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(df.at[i,'ip1']))
#     # kind = level_kind[1]
#     # level = level_kind[0]
#     df.at[i,'kind'] = int(kind)
#     df.at[i,'level'] = level #float("%.6f"%-1) if df.at[i,'kind'] == -1 else float("%.6f"%level)
#     return df    

# def strip_string_columns(df):
#     if ('etiket' in df.columns) and ('nomvar' in df.columns) and ('typvar' in df.columns):
#         df['etiket'] = df['etiket'].str.strip()
#         df['nomvar'] = df['nomvar'].str.strip()
#         df['typvar'] = df['typvar'].str.strip()
#     return df    

def convert_df_dtypes(df,decoded):
    if not df.empty:
        if not decoded:    
            df = df.astype(
                {'ni':'int32', 'nj':'int32', 'nk':'int32', 'ip1':'int32', 'ip2':'int32', 'ip3':'int32', 'deet':'int32', 'npas':'int32',
                'nbits':'int32' , 'ig1':'int32', 'ig2':'int32', 'ig3':'int32', 'ig4':'int32', 'datev':'int32',
                'dateo':'int32', 'datyp':'int32'}
                )
        else:
            df = df.astype(
                {'ni':'int32', 'nj':'int32', 'nk':'int32', 'ip1':'int32', 'ip2':'int32', 'ip3':'int32', 'deet':'int32', 'npas':'int32',
                'nbits':'int32' , 'ig1':'int32', 'ig2':'int32', 'ig3':'int32', 'ig4':'int32', 'datev':'int32',
                'dateo':'int32', 'datyp':'int32',
                'level':'float32','kind':'int32','ip2_dec':'float32','ip2_kind':'int32','ip3_dec':'float32','ip3_kind':'int32'}
                )
                
    return df      

def reorder_columns(df) -> pd.DataFrame:
    if df.empty:
        return df
    all_columns = set(df.columns.to_list())    
    
    ordered = ['nomvar','typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas',
            'datyp', 'nbits' , 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']
    all_columns = all_columns.difference(set(ordered))
    ordered.extend(list(all_columns)) 
    df = df[ordered]    
    return df    

def sort_dataframe(df) -> pd.DataFrame:
    if df.empty:
        return df
    if ('grid' in df.columns) and ('fhour' in df.columns)and ('nomvar' in df.columns) and ('level' in df.columns): 
        df.sort_values(by=['grid','fhour','nomvar','level'],ascending=False,inplace=True)
    else:     
        df.sort_values(by=['nomvar'],ascending=False,inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df    

def set_vertical_coordinate_type(df) -> pd.DataFrame:
    newdfs=[]
    grid_groups = df.groupby(df.grid)
    for _, grid in grid_groups:
        toctoc, p0, e1, pt, hy, sf, vcode = get_meta_fields_exists(grid)
        kind_groups = grid.groupby(grid.kind)
        for _, kind_group in kind_groups:
            #these kinds are not defined
            without_meta = kind_group.query('(kind not in [-1,3,6])')
            if not without_meta.empty:
                #logger.debug(without_meta.iloc[0]['nomvar'])
                kind = without_meta.iloc[0]['kind']
                kind_group['vctype'] = 'UNKNOWN'
                #vctype_dict = {'kind':kind,'toctoc':toctoc,'P0':p0,'E1':e1,'PT':pt,'HY':hy,'SF':sf,'vcode':vcode}
                vctyte_df = VCTYPES.query('(kind==%s) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%s)'%(kind,toctoc,p0,e1,pt,hy,sf,vcode))
                if not vctyte_df.empty:
                    if len(vctyte_df.index)>1:
                        logger.warning('set_vertical_coordinate_type - more than one match!!!')
                    kind_group['vctype'] = vctyte_df.iloc[0]['vctype']
                newdfs.append(kind_group)

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


def resize_data(df:pd.DataFrame, dim1:int,dim2:int) -> pd.DataFrame:
    from .std_reader import load_data
    df = load_data(df)
    for i in df.index:
        df.at[i,'d'] = df.at[i,'d'][:dim1,:dim2].copy(deep=True)
        df.at[i,'shape']  = df.at[i,'d'].shape
        df.at[i,'ni'] = df.at[i,'shape'][0]
        df.at[i,'nj'] = df.at[i,'shape'][1]
    df = sort_dataframe(df)    
    return df

def remove_from_df(df_to_remove_from:pd.DataFrame, df_to_remove) -> pd.DataFrame:
    columns = df_to_remove.columns.values.tolist()
    columns.remove('d')
    columns.remove('fstinl_params')
    tmp_df = pd.concat([df_to_remove_from, df_to_remove]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df)
    tmp_df.reset_index(inplace=True,drop=True) 
    return tmp_df

def get_intersecting_levels(df:pd.DataFrame, names:list) -> pd.DataFrame:
    from .exceptions import StandardFileError
    from .dataframe_utils import select
    #logger.debug('1',df[['nomvar','surface','level','kind']])
    if len(names)<=1:
        logger.error('get_intersecting_levels - not enough names to process')
        raise StandardFileError('not enough names to process')
    firstdf = select(df, 'nomvar == "%s"' % names[0])
    if df.empty:
        logger.error('get_intersecting_levels - no records to intersect')
        raise StandardFileError('get_intersecting_levels - no records to intersect')
    common_levels = set(firstdf.level.unique())
    query_strings = []
    for name in names:
        current_query = 'nomvar == "%s"' % name
        currdf = select(df,'%s' % current_query)
        levels = set(currdf.level.unique())
        common_levels = common_levels.intersection(levels)
        query_strings.append(current_query)
    query_strings = " or ".join(tuple(query_strings))
    query_res = select(df,'(%s) and (level in %s)' % (query_strings, list(common_levels)))
    if query_res.empty:
        logger.error('get_intersecting_levels - no intersecting levels found')
        return
    return query_res    