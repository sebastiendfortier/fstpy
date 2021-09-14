# -*- coding: utf-8 -*-
import concurrent.futures

import numpy as np
import pandas as pd
import sys
import logging
from .std_dec import (convert_rmndate_to_datetime,
                      get_parsed_etiket, get_unit_and_description)



# get modifier information from the second character of typvar
def parse_typvar(typvar:str):
    multiple_modifications = False
    zapped = False
    filtered = False
    interpolated = False
    unit_converted = False
    bounded = False
    missing_data = False
    ensemble_extra_info = False
    if len(typvar) == 2:
        typvar2 = typvar[1]
        if (typvar2 == 'M'):
            # Il n'y a pas de façon de savoir quelle modif a ete faite
            multiple_modifications = True
        elif (typvar2 == 'Z'):
            zapped = True
        elif (typvar2 == 'F'):
            filtered = True
        elif (typvar2 == 'I'):
            interpolated = True
        elif (typvar2 == 'U'):
            unit_converted = True
        elif (typvar2 == 'B'):
            bounded = True
        elif (typvar2 == '?'):
            missing_data = True
        elif (typvar2 == '!'):
            ensemble_extra_info = True
    return multiple_modifications,zapped,filtered,interpolated,unit_converted,bounded,missing_data,ensemble_extra_info

def add_flag_values(df:pd.DataFrame) -> pd.DataFrame:
    """adds the correct flag values derived from parsing the typvar

    :param df:dataframe
    :type df: pd.DataFrame
    :return: flag values set according to second character of typvar if present
    :rtype: pd.DataFrame
    """
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vparse_typvar = np.vectorize(parse_typvar,otypes=['bool','bool','bool','bool','bool','bool','bool','bool'])
    df.loc[:,'multiple_modifications'],df.loc[:,'zapped'],df.loc[:,'filtered'],df.loc[:,'interpolated'],df.loc[:,'unit_converted'],df.loc[:,'bounded'],df.loc[:,'missing_data'],df.loc[:,'ensemble_extra_info'] = vparse_typvar(df['typvar'].values)
    return df


# def add_decoded_columns( df:pd.DataFrame,decode_metadata:bool=True) -> pd.DataFrame:
#     """Adds basic and decoded columns to the dataframe.

#     :param df: input dataframe
#     :type df: pd.DataFrame
#     :param decode_metadata: if true, decodes extra columns
#     :type decode_metadata: bool
#     :return: dataframe with decoded columns
#     :rtype: pd.DataFrame
#     """
#     df = post_process_dataframe(df)

#     df = add_columns(df,decode_metadata)
#     # df = parallel_add_composite_columns_tr(df,decode_metadata,array_container,attributes_to_decode,n_cores=min(mp.cpu_count(),len(df.index),1))

#     return df

def drop_duplicates(df:pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows from dataframe
        
    :param df: original dataframe
    :type df: pd.DataFrame
    :return: dataframe without duplicate rows
    :rtype: pd.DataFrame
    """
    init_row_count = len(df.index)
    columns = ['nomvar','typvar','etiket','ni','nj','nk','dateo',
        'ip1','ip2','ip3','deet','npas','datyp','nbits',
        'grtyp','ig1','ig3','ig4','datev','key']

    df = df.drop_duplicates(subset=columns,keep='first')

    row_count = len(df.index)
    if init_row_count != row_count:
        logging.warning('Found duplicate rows in dataframe!')
        
    return df


# def get_data_holder(d):
#     # import dask.array as da
#     if not (isinstance(d,np.ndarray)):# or (isinstance(d,da.core.Array))):
#         d = None
#     return d

def get_shape(ni,nj):
    return (ni,nj)


def add_shape_column(df):
    print('shape' in df.columns)
    if 'shape' in df.columns:
        return df
    vmake_shape = np.vectorize(get_shape,otypes=['object'])
    df.loc[:,'shape'] = vmake_shape(df['ni'].values,df['nj'].values)
    return df


def add_data_column(df):
    if 'd' in df.columns:
        return df
    df.loc[:,'d']=None
    return df


def add_parsed_etiket_columns(df:pd.DataFrame) ->pd.DataFrame:
    """adds label,run,implementation and ensemble_member columns from the parsed etikets to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with label,run,implementation and ensemble_member columns added
    :rtype: pd.DataFrame
    """
    # df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = get_parsed_etiket(df.at[i,'etiket'])
    vparse_etiket = np.vectorize(get_parsed_etiket,otypes=['str','str','str','str'])
    df.loc[:,'label'],df.loc[:,'run'],df.loc[:,'implementation'],df.loc[:,'ensemble_member'] = vparse_etiket(df['etiket'].values)
    return df

def add_unit_and_description_columns(df:pd.DataFrame) ->pd.DataFrame:
    """adds unit and description from the nomvars to a dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: dataframe with unit and description columns added
    :rtype: pd.DataFrame
    """

    vget_unit_and_description = np.vectorize(get_unit_and_description,otypes=['str','str'])
    if 'unit' in df.columns:
        sub_df = df.loc[df['unit'].isna()].copy()
        assign_unit_and_description(vget_unit_and_description, sub_df)
        df.loc[df['unit'].isna()] = sub_df
    else:
        df.loc[:,'unit'] = None
        assign_unit_and_description(vget_unit_and_description, df)
    return df

def assign_unit_and_description(vget_unit_and_description, df):
    df.loc[:,'unit'],df.loc[:,'description'] = vget_unit_and_description(df['nomvar'].values)

# def add_flags_columns(df:pd.DataFrame) ->pd.DataFrame:
#     df.loc[:,'unit_converted'] = False
#     df.loc[:,'missing_data'] = False
#     df.loc[:,'zapped'] = False
#     df.loc[:,'filtered'] = False
#     df.loc[:,'interpolated'] = False
#     df.loc[:,'bounded'] = False
#     df.loc[:,'ensemble_extra_info'] = False
#     df.loc[:,'multiple_modifications'] = False
#     return df

def add_decoded_date_column(df:pd.DataFrame,attr:str='dateo') ->pd.DataFrame:
    """adds the decoded dateo or datev column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :param attr: selected date to decode, defaults to 'dateo'
    :type attr: str, optional
    :return: either date_of_observation or date_of_validity column added to the dataframe
    :rtype: pd.DataFrame
    """
    vconvert_rmndate_to_datetime = np.vectorize(convert_rmndate_to_datetime)#,otypes=['datetime64']
    if attr == 'dateo':
        df.loc[:,'date_of_observation'] = vconvert_rmndate_to_datetime(df['dateo'].values)
    else:
        df.loc[:,'date_of_validity'] = vconvert_rmndate_to_datetime(df['datev'].values)
    return df


def add_forecast_hour_column(df:pd.DataFrame) -> pd.DataFrame:
    """adds the forecast_hour column derived from the deet and npas columns

    :param df:dataframe
    :type df: pd.DataFrame
    :return: forecast_hour column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_forecast_hour
    # df.at[i,'forecast_hour'] = datetime.timedelta(seconds=int((df.at[i,'npas'] * df.at[i,'deet'])))
    vcreate_forecast_hour = np.vectorize(get_forecast_hour)#,otypes=['timedelta64[ns]']
    df.loc[:,'forecast_hour'] = vcreate_forecast_hour(df['deet'].values, df['npas'].values)
    return df



# def add_decoded_ip2_columns(df:pd.DataFrame) -> pd.DataFrame:
#     """adds the decoded ip2 columns to the dataframe

#     :param df: dataframe
#     :type df: pd.DataFrame
#     :return: ip2_dec,ip2_kind and ip2_pkind columns added to the dataframe
#     :rtype: pd.DataFrame
#     """
#     from .std_dec import decode_ip2
#     vdecode_ip2 = np.vectorize(decode_ip2,otypes=['float32','int32','str'])
#     df.loc[:,'ip2_dec'],df.loc[:,'ip2_kind'],df.loc[:,'ip2_pkind'] = vdecode_ip2(df['ip2'])
#     return df

# def add_decoded_ip3_columns(df:pd.DataFrame) -> pd.DataFrame:
#     """adds the decoded ip3 columns to the dataframe

#     :param df: dataframe
#     :type df: pd.DataFrame
#     :return: ip3_dec,ip3_kind and ip3_pkind columns added to the dataframe
#     :rtype: pd.DataFrame
#     """
#     from .std_dec import decode_ip3
#     vdecode_ip3 = np.vectorize(decode_ip3,otypes=['float32','int32','str'])
#     df.loc[:,'ip3_dec'],df.loc[:,'ip3_kind'],df.loc[:,'ip3_pkind'] = vdecode_ip3(df['ip3'])
#     return df

def add_data_type_str_column(df:pd.DataFrame) -> pd.DataFrame:
    """adds the data type decoded to string value column to the dataframe

    :param df: dataframe
    :type df: pd.DataFrame
    :return: data_type_str column added to the dataframe
    :rtype: pd.DataFrame
    """
    from .std_dec import get_data_type_str
    vcreate_data_type_str = np.vectorize(get_data_type_str,otypes=['str'])
    df.loc[:,'data_type_str'] = vcreate_data_type_str(df['datyp'].values)
    return df


def add_ip_info_columns(df:pd.DataFrame) -> pd.DataFrame:
    """adds all relevant level info from the ip1 column values

    :param df: dataframe
    :type df: pd.DataFrame
    :return: level, ip1_kind, ip1_pkind,surface and follow_topography columns added to the dataframe.
    :rtype: pd.DataFrame
    """
    from .std_dec import get_ip_info
    vcreate_ip_info = np.vectorize(get_ip_info,otypes=['float32','int32','str','float32','int32','str','float32','int32','str','bool','bool','bool','object'])
    df.loc[:,'level'],df.loc[:,'ip1_kind'],df.loc[:,'ip1_pkind'],df.loc[:,'ip2_dec'],df.loc[:,'ip2_kind'],df.loc[:,'ip2_pkind'],df.loc[:,'ip3_dec'],df.loc[:,'ip3_kind'],df.loc[:,'ip3_pkind'],df.loc[:,'surface'],df.loc[:,'follow_topography'],df.loc[:,'ascending'],df.loc[:,'interval'] = vcreate_ip_info(df['ip1'].values,df['ip2'].values,df['ip3'].values)
    return df

def add_columns(df:pd.DataFrame, decode:bool, columns:'list[str]'=['flags','etiket','unit','dateo','datev','forecast_hour','datyp','ip_info']) -> pd.DataFrame:
    """If not already present, adds the data('d') and shape columns to a dataframe. If decode is True and valid columns are provided, they will be added.

    :param df: dataframe to modify (meta data needs to be present in dataframe)
    :type df: pd.DataFrame
    :param decode: if decode is True, add the specified columns
    :type decode: bool
    :param columns: [description], defaults to ['flags','etiket','unit','dateo','datev','forecast_hour','datyp','ip_info']
    :type columns: list[str], optional
    :return: dataframe with coluns added
    :rtype: pd.DataFrame
    """

    cols = ['flags','etiket','unit','dateo','datev','forecast_hour','datyp','ip_info']
    for col in columns:
        if col not in cols:
            logging.warning(f'{col} not found in {cols}\n')

    if decode:
        if 'etiket' in columns:
            df = add_parsed_etiket_columns(df)

        if 'unit' in columns:
            df = add_unit_and_description_columns(df)

        if 'dateo' in columns:
            df = add_decoded_date_column(df,'dateo')

        if 'datev' in columns:
            df = add_decoded_date_column(df,'datev')

        if 'forecast_hour' in columns:
            df = add_forecast_hour_column(df)

        if 'datyp' in columns:
            df = add_data_type_str_column(df)

        if ('ip_info' in  columns):
            df = add_ip_info_columns(df)

        if 'flags' in columns:
            # df = add_flags_columns(df)
            df = add_flag_values(df)


    if decode and ('ip_info' in columns):
        df = set_vertical_coordinate_type(df)

    return df

# def add_unit_column(df):
#     if 'unit' not in df.columns:
#         df.loc[:,'unit'] = None
#     if 'unit_converted' not in df.columns:
#         df.loc[:,'unit_converted'] = None
#     if 'description' not  in df.columns:
#         df.loc[:,'description'] = None

#     for i in df.index:
#         df.at[i,'unit'],df.at[i,'description']=get_unit_and_description(df.at[i,'nomvar'])
#     return df

def add_empty_columns(df, columns, init, dtype_str):
    for col in columns:
        if col not in df.columns:
            df.insert(len(df.columns),col,init)
            df = df.astype({col:dtype_str})
    return df

def post_process_dataframe(df):
    if 'dltf' in df.columns:
        df=df.loc[df.dltf==0]
        # df.query('dltf == 0',inplace=True)
        df.drop(columns=['dltf'], inplace=True,errors='ignore')
    # if 'd' not in df.columns:
    #     df['d']=None

    return df

# def convert_df_dtypes(df,decoded,columns=['ip1','ip2','ip3']):
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
    all_columns = set(df.columns)
    # all_columns = set(df.columns.to_list())

    extra_columns = all_columns.difference(set(ordered))
    if len(extra_columns) > 0:
        ordered.extend(list(extra_columns))

    df = df[ordered]
    return df

# def sort_dataframe(df) -> pd.DataFrame:
#     if df.empty:
#         return df
#     if ('grid' in df.columns) and ('datev' in df.columns)and ('nomvar' in df.columns) and ('level' in df.columns):
#         df = df.sort_values(by=['grid','datev','nomvar','level'],ascending=[True,True,True,False])
#     else:
#         df = df.sort_values(by=['datev','nomvar'],ascending=[True,True])
#     return df

def set_vertical_coordinate_type(df) -> pd.DataFrame:
    from fstpy import VCTYPES
    if 'level' not in df.columns:
        df = add_ip_info_columns(df)
    newdfs=[]
    df.loc[:,'vctype'] = 'UNKNOWN'
    grid_groups = df.groupby(df.grid)
    #print(df[['ip1','ip2','ig1','ig2']])
    for _, grid in grid_groups:
        toctoc, p0, e1, pt, hy, sf, vcode = get_meta_fields_exists(grid)
        ip1_kind_groups = grid.groupby(grid.ip1_kind)
        for _, ip1_kind_group in ip1_kind_groups:
            #these ip1_kinds are not defined
            # without_meta = ip1_kind_group.query('(ip1_kind not in [-1,3,6]) and (nomvar not in ["!!","HY","P0","PT",">>","^^","PN"])')
            without_meta = ip1_kind_group.loc[(~ip1_kind_group.ip1_kind.isin([-1,3,6])) & (~ip1_kind_group.nomvar.isin(["!!","HY","P0","PT",">>","^^","PN"]))]
            if not without_meta.empty:
                #logger.debug(without_meta.iloc[0]['nomvar'])
                ip1_kind = without_meta.iloc[0]['ip1_kind']
                # print(vcode)
                if len(vcode) > 1:
                    for vc in vcode:
                        d,_=divmod(vc,1000)
                        if ip1_kind == d:
                            this_vcode = vc
                            continue
                else:
                    this_vcode = vcode[0]
                ip1_kind_group.loc[:,'vctype'] = 'UNKNOWN'
                #vctype_dict = {'ip1_kind':ip1_kind,'toctoc':toctoc,'P0':p0,'E1':e1,'PT':pt,'HY':hy,'SF':sf,'vcode':vcode}
                # print(VCTYPES)
                # print(VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(5,False,True,False,False,False,False,-1)))
                # print('\n(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                # vctyte_df = VCTYPES.query('(ip1_kind==%d) and (toctoc==%s) and (P0==%s) and (E1==%s) and (PT==%s) and (HY==%s) and (SF==%s) and (vcode==%d)'%(ip1_kind,toctoc,p0,e1,pt,hy,sf,this_vcode))
                vctyte_df = VCTYPES.loc[(VCTYPES.ip1_kind==ip1_kind) & (VCTYPES.toctoc==toctoc) & (VCTYPES.P0==p0) & (VCTYPES.E1==e1) & (VCTYPES.PT==pt) & (VCTYPES.HY==hy) & (VCTYPES.SF==sf) & (VCTYPES.vcode==this_vcode)]
                # print(vctyte_df)
                if not vctyte_df.empty:
                    if len(vctyte_df.index)>1:
                        logger.warning('set_vertical_coordinate_type - more than one match!!!')
                    ip1_kind_group.loc[:,'vctype'] = vctyte_df.iloc[0]['vctype']
            newdfs.append(ip1_kind_group)

    df = pd.concat(newdfs,ignore_index=True)
    df.loc[df['nomvar'].isin([">>","^^","!!","P0","PT","HY","PN","!!SF"]), "vctype"] = "UNKNOWN"
    return df

def get_meta_fields_exists(grid):
    toctoc = grid.loc[grid.nomvar=="!!"]
    vcode = []
    if not toctoc.empty:
        for i in toctoc.index:
            vcode.append(toctoc.at[i,'ig1'])
        toctoc = True
    else:
        vcode.append(-1)
        toctoc = False
    p0 = meta_exists(grid,"P0")
    e1 = meta_exists(grid,"E1")
    pt = meta_exists(grid,"PT")
    hy = meta_exists(grid,"HY")
    sf = meta_exists(grid,"!!SF")
    return toctoc, p0, e1, pt, hy, sf, vcode

def meta_exists(grid, nomvar) -> bool:
    df = grid.loc[grid.nomvar==nomvar]
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
    tmp_df = pd.concat([df_to_remove_from, df_to_remove],ignore_index=True).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df)
    tmp_df.reset_index(inplace=True,drop=True)
    return tmp_df

# def get_intersecting_levels(df:pd.DataFrame, nomvars:list) -> pd.DataFrame:
#     """Gets the records of all intersecting levels for nomvars in list.
#     if TT,UU and VV are in the list, the output dataframe will contain all 3
#     varaibles at all the intersectiong levels between the 3 variables

#     :param df: input dataframe
#     :type df: pd.DataFrame
#     :param nomvars: list of nomvars to select
#     :type nomvars: list
#     :raises StandardFileError: if a problem occurs this exception will be raised
#     :return: dataframe subset
#     :rtype: pd.DataFrame
#     """
#     #logger.debug('1',df[['nomvar','surface','level','ip1_kind']])
#     if len(nomvars)<=1:
#         logger.error('get_intersecting_levels - not enough nomvars to process')
#         raise StandardFileError('not enough nomvars to process')
#     first_df = df.query( 'nomvar == "%s"' % nomvars[0])
#     if df.empty:
#         logger.error('get_intersecting_levels - no records to intersect')
#         raise StandardFileError('get_intersecting_levels - no records to intersect')
#     common_levels = set(first_df.level.unique())
#     query_strings = []
#     for name in nomvars:
#         current_query = 'nomvar == "%s"' % name
#         curr_df = df.query('%s' % current_query)
#         levels = set(curr_df.level.unique())
#         common_levels = common_levels.intersection(levels)
#         query_strings.append(current_query)
#     query_strings = " or ".join(tuple(query_strings))
#     query_res = df.query('(%s) and (level in %s)' % (query_strings, list(common_levels)))
#     if query_res.empty:
#         logger.error('get_intersecting_levels - no intersecting levels found')
#         return pd.DataFrame(dtype='object')
#     return query_res

def parallel_add_composite_columns_tr(df, decode_metadata, attributes_to_decode,n_cores):
    dataframes = []
    df_split = np.array_split(df, n_cores)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_cores) as executor:
        # Start the load operations and mark each future with its URL
        #df,decode,array_container
        future_to_df = {executor.submit(add_columns, dfp, decode_metadata, attributes_to_decode): dfp for dfp in df_split}
        for future in concurrent.futures.as_completed(future_to_df):
            dfp = future_to_df[future]
            try:
                data = future.result()
            except Exception as exc:
                logging.warning('%r generated an exception: %s' % (dfp, exc))
            else:
                dataframes.append(data)
                #print('%r %s d is %d ' % (d, data, len(data)))
    df = pd.concat(dataframes,ignore_index=True)
    df = sort_dataframe(df)
    return df

# def parallel_add_columns(df, decode_metadata, array_container, n_cores):
#     df_split = np.array_split(df, n_cores)
#     df_with_params = list(zip(df_split,itertools.repeat(decode_metadata),itertools.repeat(array_container)))
#     pool = mp.Pool(n_cores)
#     df = pd.concat(pool.starmap(add_columns, df_with_params))
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
#         df.at[i,'grid'] = std_io.get_grid_identifier(df.at[i,'nomvar'],df.at[i,'ip1'],df.at[i,'ip2'],df.at[i,'ig1'],df.at[i,'ig2'])
#         #logger.debug(df.at[i,'ip1_kind'],df.at[i,'level'])
#         #set surface flag for surface levels
#         std_io.set_surface(df, i, meter_levels)
#         std_io.set_follow_topography(df, i)
#         df.at[i,'label'],df.at[i,'run'],df.at[i,'implementation'],df.at[i,'ensemble_member'] = std_io.get_parsed_etiket(df.at[i,'etiket'])
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
