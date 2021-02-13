# -*- coding: utf-8 -*-
from .std_io import open_fst, close_fst
import rpnpy.librmn.all as rmn
import pandas as pd

def to_pandas(self) -> pd.DataFrame:
        """creates the dataframe from the provided files  

        :return: df  
        :rtype: pd.Dataframe  
        """
        if isinstance(self.filenames, list):
            dfs = []
            for file in set(self.filenames):
                df = create_dataframe(file,self.read_meta_fields_only,self.add_extra_columns,self.materialize,self.subset)
                dfs.append(df)
            df = pd.concat(dfs)   
            return df
        else:
            df = create_dataframe(self.filenames,self.read_meta_fields_only,self.add_extra_columns,self.materialize,self.subset)
            return df

def create_dataframe(file,read_meta_fields_only,add_extra_columns,materialize,subset) -> pd.DataFrame:
    import os
    import exceptions
    path = os.path.abspath(file)
    file_id, file_modification_time = open_fst(path,rmn.FST_RO,'StandardFileReader',exceptions.StandardFileReaderError)
    df = read(file_id,path,file_modification_time, materialize,subset,read_meta_fields_only,add_extra_columns)
    close_fst(file_id,path,'StandardFileReader')
    del os
    del exceptions
    return df            
    
def get_basic_dataframe(file_id,filenames,file_modification_time, materialize,subset):
    import std_io
    keys = std_io.get_all_record_keys(file_id, subset)

    records = std_io.get_records(keys,materialize)

    #create a dataframe correspondinf to the fst file
    df = pd.DataFrame(records)

    df = std_io.add_path_and_modification_time(df,filenames,file_modification_time)

    if not materialize:
        #add missing stuff when materialized
        df = add_empty_columns(df, ['d'],None,'O')

    df = add_empty_columns(df, ['materialize_info'],None,'O')

    df = strip_string_columns(df)

    df = convert_df_dtypes(df)
    del std_io
    return df    

def read(file_id,filenames,file_modification_time, materialize,subset,read_meta_fields_only,add_extra_columns) ->pd.DataFrame:
    """reads the meta data of an fst file and puts it into a pandas dataframe  

    :return: dataframe of records in file  
    :rtype: pd.DataFrame  
    """
    #get the basic rmnlib dataframe
    df = get_basic_dataframe(file_id,filenames,file_modification_time, materialize,subset)

    if read_meta_fields_only:
        df = remove_data_fields(df)

    if add_extra_columns:
        df = add_extra_cols(df)

    df = reorder_columns(df,add_extra_columns)  

    df = reorder_dataframe(df)
    return df

def add_path_and_modification_time(df, file, file_modification_time):
    # create the file column and init
    df['path'] = file
    #create the file mod time column and init
    df['file_modification_time'] = file_modification_time
    return df

def remove_meta_data_fields(df: pd.DataFrame) -> pd.DataFrame:
    for meta in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]:
        df = df[df.nomvar != meta]
    return df

def remove_data_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.query('nomvar in ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]')
    return df    

def add_extra_cols(df):
    df = add_empty_columns(df, ['kind'], 0, 'int32')
    df = add_empty_columns(df, ['level'],np.nan,'float32')
    df = add_empty_columns(df, ['surface','follow_topography','dirty','unit_converted'],False,'bool')
    df = add_empty_columns(df, ['pdateo','pdatev','fhour'],None,'datetime64')
    df = add_empty_columns(df, ['vctype','pkind','pdatyp','grid','e_run','e_implementation','e_ensemble_member','e_label','unit'],'','O')
    df = fill_columns(df)
    return df   

def add_empty_columns(df, columns, init, dtype_str):
    for col in columns:
        df.insert(len(df.columns),col,init)
        df = df.astype({col:dtype_str})
    return df       

def fill_columns(df:pd.DataFrame):
    """fill_columns adds columns that are'nt in the fst file

    :param records: DataFrame to add colmns to
    :type records: pd.DataFrame
    """
    #add columns
    meter_levels = np.arange(0.,10.5,.5).tolist()
    for i in df.index:
        nomvar = df.at[i,'nomvar']
        #find unit name value for this nomvar
        df = get_unit(df, i, nomvar)
        #get level and kind
        df = set_level_and_kind(df, i) #float("%.6f"%-1) if df.at[i,'kind'] == -1 else float("%.6f"%level)
        #create a real date of observation
        df.at[i,'pdateo'] = create_printable_date_of_observation(int(df.at[i,'dateo']))
        #create a printable date of validity
        df.at[i,'pdatev'] = create_printable_date_of_observation(int(df.at[i,'datev']))
        #create a printable kind for voir
        df.at[i,'pkind'] = KIND_DICT[int(df.at[i,'kind'])]
        #calculate the forecast hour
        df.at[i,'fhour'] = df.at[i,'npas'] * df.at[i,'deet'] / 3600.
        #create a printable data type for voir
        df.at[i,'pdatyp'] = DATYP_DICT[df.at[i,'datyp']]
        #create a grid identifier for each record
        df.at[i,'grid'] = create_grid_identifier(df.at[i,'nomvar'],df.at[i,'ip1'],df.at[i,'ip2'],df.at[i,'ig1'],df.at[i,'ig2'])
        #logger.debug(df.at[i,'kind'],df.at[i,'level'])
        #set surface flag for surface levels
        set_surface(df, i, meter_levels)
        set_follow_topography(df, i)
        df.at[i,'e_label'],df.at[i,'e_run'],df.at[i,'e_implementation'],df.at[i,'e_ensemble_member'] = parse_etiket(df.at[i,'etiket'])
        df.at[i,'d'] = (rmn.fstluk,int(df.at[i,'key']))
    return df

def set_level_and_kind(df, i):
    import std_io
    level, kind = std_io.get_level_and_kind(df.at[i,'ip1'])
    # level_kind = rmn.convertIp(rmn.CONVIP_DECODE,int(df.at[i,'ip1']))
    # kind = level_kind[1]
    # level = level_kind[0]
    df.at[i,'kind'] = int(kind)
    df.at[i,'level'] = level #float("%.6f"%-1) if df.at[i,'kind'] == -1 else float("%.6f"%level)
    return df    

def strip_string_columns(df):
    if ('etiket' in df.columns) and ('nomvar' in df.columns) and ('typvar' in df.columns):
        df['etiket'] = df['etiket'].str.strip()
        df['nomvar'] = df['nomvar'].str.strip()
        df['typvar'] = df['typvar'].str.strip()
    return df    

def convert_df_dtypes(df):
    if not df.empty:
        df = df.astype(
            {'ni':'int32', 'nj':'int32', 'nk':'int32', 'ip1':'int32', 'ip2':'int32', 'ip3':'int32', 'deet':'int32', 'npas':'int32',
            'nbits':'int32' , 'ig1':'int32', 'ig2':'int32', 'ig3':'int32', 'ig4':'int32', 'datev':'int32','swa':'int32', 'lng':'int32', 'dltf':'int32', 'ubc':'int32',
            'xtra1':'int32', 'xtra2':'int32', 'xtra3':'int32', 'dateo':'int32', 'datyp':'int32', 'key':'int32'}
            )
    return df      