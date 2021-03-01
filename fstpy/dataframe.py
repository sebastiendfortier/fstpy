# -*- coding: utf-8 -*-
from .constants import DATYP_DICT,VCTYPES
# from .dataframe_utils import add_empty_columns
from .exceptions import StandardFileError
from .logger_config import logger
from .std_dec import get_unit_and_description,parse_etiket,convert_rmndate_to_datetime, decode_ip, is_surface, level_type_follows_topography
from .std_io import read_record
import dask.array as da
import datetime
import numpy as np
import pandas as pd


def add_decoded_columns( df,decode_metadata,array_container='numpy'):
    df = post_process_dataframe(df,decode_metadata)

    df = add_composite_columns(df,decode_metadata,array_container)
    #df = parallel_add_composite_columns(df,decode_metadata,array_container,n_cores=min(cpu_count(),len(df.index)))   
    
    return df

def clean_dataframe(df,decode_metadata):
    df = convert_df_dtypes(df,decode_metadata)

    df = reorder_columns(df)  

    df = sort_dataframe(df)
    return df

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

def add_composite_columns(df,decode,array_container):
    
    for i in df.index:            
        # df.at[i,'fstinl_params'] = {
        #     'datev':df.at[i,'datev'],
        #     'etiket':df.at[i,'etiket'],
        #     'ip1':df.at[i,'ip1'],
        #     'ip2':df.at[i,'ip2'],
        #     'ip3':df.at[i,'ip3'],
        #     'typvar':df.at[i,'typvar'],
        #     'nomvar':df.at[i,'nomvar']
        # }
        # print(type(df.at[i,'d'])) 
        # print((not isinstance(df.at[i,'d'],np.ndarray)))
        # print((not isinstance(df.at[i,'d'],da.core.Array)))
        # print(not ((isinstance(df.at[i,'d'],np.ndarray)) or (isinstance(df.at[i,'d'],da.core.Array))))
        if not ((isinstance(df.at[i,'d'],np.ndarray)) or (isinstance(df.at[i,'d'],da.core.Array))):
            df.at[i,'d'] = (read_record,array_container,int(df.at[i,'key']))

        #del record['key'] #i don't know if we need

        #create a grid identifier for each record
        if df.at[i,'nomvar'] in [">>", "^^", "!!", "!!SF", "HY"]:
            df.at[i,'grid'] = "".join([str(df.at[i,'ip1']),str(df.at[i,'ip2'])])
        else:
            df.at[i,'grid'] = "".join([str(df.at[i,'ig1']),str(df.at[i,'ig2'])])

        if decode:
            df['unit_converted'] = False
            df['zapped'] = False
            df['vctype'] = ''
            df['stacked'] = False
            df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = parse_etiket(df.at[i,'etiket'])
            df.at[i,'unit'],df.at[i,'description']=get_unit_and_description(df.at[i,'nomvar'])
            df.at[i,'date_of_observation'] = convert_rmndate_to_datetime(int(df.at[i,'dateo']))
            df.at[i,'date_of_validity'] = convert_rmndate_to_datetime(int(df.at[i,'datev']))    
            df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))         
            df.at[i,'level'],df.at[i,'ip1_kind'],df.at[i,'ip1_pkind'] = decode_ip(int(df.at[i,'ip1']))
            df.at[i,'ip2_dec'],df.at[i,'ip2_kind'],df.at[i,'ip2_pkind'] = decode_ip(int(df.at[i,'ip2']))
            df.at[i,'ip3_dec'],df.at[i,'ip3_kind'],df.at[i,'ip3_pkind'] = decode_ip(int(df.at[i,'ip3']))
            df.at[i,'data_type_str'] = DATYP_DICT[df.at[i,'datyp']]
            df.at[i,'surface'] = is_surface(df.at[i,'ip1_kind'],df.at[i,'level'])
            df.at[i,'follow_topography'] = level_type_follows_topography(df.at[i,'ip1_kind'])
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
        df.insert(len(df.columns),col,init)
        df = df.astype({col:dtype_str})
    return df         

def post_process_dataframe(df,decode):
    if 'dltf' in df.columns:
        df = df[df.dltf == 0]
    df.drop(columns=['swa', 'ubc','lng','xtra1','xtra2','xtra3','dltf'], inplace=True,errors='ignore')
    
    df['nomvar'] = df['nomvar'].str.strip()
    df['etiket'] = df['etiket'].str.strip()
    df['typvar'] = df['typvar'].str.strip()
    if 'd' not in df.columns:
        df['d']=None
    if decode:
        df = add_empty_columns(df, ['data_type_str','description','ensemble_member','implementation','ip1_pkind','ip2_pkind','ip3_pkind','label','run','vctype','unit'],'', 'O')
        df = add_empty_columns(df, ['follow_topography','stacked','surface','unit_converted','zapped'], False, 'bool')
        df = add_empty_columns(df, ['ip1_kind','ip2_kind','ip3_kind'], 0, 'int32')
        df = add_empty_columns(df, ['level','ip2_dec','ip3_dec'], 0., 'float32')
        for col in ['date_of_observation','date_of_validity','forecast_hour']:
            df[col] = None
        
    return df




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

def convert_df_dtypes(df,decoded):
    if not df.empty:
        if not decoded:    
            df = df.astype(
                {'key':'int32','ni':'int32', 'nj':'int32', 'nk':'int32', 'ip1':'int32', 'ip2':'int32', 'ip3':'int32', 'deet':'int32', 'npas':'int32',
                'nbits':'int32' , 'ig1':'int32', 'ig2':'int32', 'ig3':'int32', 'ig4':'int32', 'datev':'int32',
                'dateo':'int32', 'datyp':'int32'}
                )
        else:
            df = df.astype(
                {'key':'int32','ni':'int32', 'nj':'int32', 'nk':'int32', 'ip1':'int32', 'ip2':'int32', 'ip3':'int32', 'deet':'int32', 'npas':'int32',
                'nbits':'int32' , 'ig1':'int32', 'ig2':'int32', 'ig3':'int32', 'ig4':'int32', 'datev':'int32',
                'dateo':'int32', 'datyp':'int32',
                'level':'float32','ip1_kind':'int32','ip2_dec':'float32','ip2_kind':'int32','ip3_dec':'float32','ip3_kind':'int32'}
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
    if ('grid' in df.columns) and ('forecast_hour' in df.columns)and ('nomvar' in df.columns) and ('level' in df.columns): 
        df.sort_values(by=['grid','forecast_hour','nomvar','level'],ascending=False,inplace=True)
    else:     
        df.sort_values(by=['nomvar'],ascending=False,inplace=True)
    df.reset_index(drop=True,inplace=True)
    return df    

def set_vertical_coordinate_type(df) -> pd.DataFrame:
    newdfs=[]
    grid_groups = df.groupby(df.grid)
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

def remove_from_df(df_to_remove_from:pd.DataFrame, df_to_remove) -> pd.DataFrame:
    columns = df_to_remove.columns.values.tolist()
    columns.remove('d')
    #columns.remove('fstinl_params')
    tmp_df = pd.concat([df_to_remove_from, df_to_remove]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df)
    tmp_df.reset_index(inplace=True,drop=True) 
    return tmp_df

def get_intersecting_levels(df:pd.DataFrame, names:list) -> pd.DataFrame:

    #logger.debug('1',df[['nomvar','surface','level','ip1_kind']])
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