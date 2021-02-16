# -*- coding: utf-8 -*-
from fstpy.dataframe import sort_dataframe
from .utils import create_1row_df_from_model, initializer, validate_df_not_empty
import rpnpy.librmn.all as rmn
import pandas as pd
import numpy as np
from .constants import VCTYPES, KIND_DICT, get_column_value_from_row, get_etikey_by_name
from .config import logger
from .std_dec import get_level_and_kind
from .exceptions import StandardFileError
import sys
from .config import logger

class StandardFileReader:
    """Class to handle fst files   
        Opens, reads the contents of an fst files or files into a pandas Dataframe and closes   
        No data is loaded unless specified, only the meta data is read. Extra meta data is added to the dataframe if specified.
 
        :param filenames: path to file or list of paths to files   
        :type filenames: str|list[str]   
        :param decode_meta_data: adds extra columns, defaults to True    
                'unit':str, unit name
                'unit_converted':bool
                'description':str, field description
                'pdateo':datetime, of the date of observation
                'pdatev':datetime, of the date of validity
                'level':float32, decoded ip1 level
                'kind':int32, decoded ip1 kind
                'pkind':str, string repr of kind int
                'pdatyp':str, string repr of kind int
                'label':str, label derived from etiket
                'run':str, run derived from etiket
                'implementation':str, implementation derived from etiket
                'ensemble_member':str, ensemble member derived from etiket
                'surface':bool, True if the level is a surface level
                'follow_topography':bool, indicates if this type of level follows topography
                'vctype':str, vertical level type
                'fhour':time, forecast hour decoded from ip2
        :type decode_meta_data: bool, optional    
        :param load_data: if True, the data will be read, not just the meta data (fstluk vs fstprm)
        :type load_data: bool, optional
        :param subset: parameter to pass to fstinl to select specific records
        :type subset:dict
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]
    @initializer
    def __init__(self, filenames, keep_all_meta_data=False,decode_meta_data=False,load_data=False,subset=None,array_container='numpy',stack=False):
        #{'datev':-1, 'etiket':' ', 'ip1':-1, 'ip2':-1, 'ip3':-1, 'typvar':' ', 'nomvar':' '}
        """init instance"""
        if self.array_container not in ['numpy','dask.array']:
            sys.stderr.write('wrong type of array container specified, defaulting to numpy')
            self.array_container = 'numpy'
        pass
        
    def to_pandas(self) -> pd.DataFrame:
        """creates the dataframe from the provided files  

        :return: df  
        :rtype: pd.Dataframe  
        """
        from .dataframe import create_dataframe
        if isinstance(self.filenames, list):
            dfs = []
            for file in set(self.filenames):
                df = create_dataframe(file,self.decode_meta_data,self.load_data,self.subset,self.array_container)
                dfs.append(df)
            df = pd.concat(dfs)   
            # return df
        else:
            df = create_dataframe(self.filenames,self.decode_meta_data,self.load_data,self.subset,self.array_container)
            # return df
        return df    

def stack_arrays(df):
    import numpy as np
    import pandas as pd
    from .std_dec import decode_ip1
    df = load_data(df)
    df_list = []
    grid_groups = df.groupby(df.grid)
    for _, grid_df in grid_groups:
        for nomvar in grid_df.nomvar.unique():
            if nomvar in ['>>','^^','!!','!!SF','HY','P0','PT']:
                to_stack_df = grid_df.query('nomvar=="%s"'%nomvar)
                df_list.append(to_stack_df)
                continue
            to_stack_df = grid_df.query('nomvar=="%s"'%nomvar)
            if len(to_stack_df.d) == 1:
                df_list.append(to_stack_df)
                continue
            if 'level' not in  to_stack_df.columns:   
                to_stack_df['level'] = None
                to_stack_df['kind'] = None
                for i in to_stack_df.index:
                    to_stack_df.at[i,'level'], to_stack_df.at[i,'kind'], _ = decode_ip1(to_stack_df.at[i,'ip1'])
            to_stack_df.sort_values(by=['level'],ascending=False,inplace=True)    
            #print('stacking %s with %s levels'%(nomvar, len(to_stack_df.level)))
            var = create_1row_df_from_model(to_stack_df)
            #print(var.index)
            data_list = to_stack_df['d'].to_list()
            stacked_data = np.stack(data_list)
            #print(type(stacked_data),type(var.at[0,'d']))
            var['stacked'] = True
            var['ip1'] = None
            var['level'] = None
            var['kind'] = None
            var['key'] = None
            var.at[0,'level'] = to_stack_df['level'].to_numpy()
            var.at[0,'kind'] = to_stack_df['kind'].to_numpy()
            var.at[0,'d'] = stacked_data
            var.at[0,'ip1'] = to_stack_df['ip1'].to_numpy()
            var.at[0,'key'] = to_stack_df['key'].to_numpy()
            var.at[0,'shape'] = (len(var.at[0,'ip1']),var.at[0,'ni'],var.at[0,'nj'])
            var.at[0,'nk'] = len(var.at[0,'ip1'])
            df_list.append(var)

    if len(df_list) > 1:
        new_df = pd.concat(df_list)
    else:   
        new_df = df_list[0]
    new_df = sort_dataframe(new_df)    
    new_df.reset_index(inplace=True,drop=True)    
    return new_df


















        




        # for i in g.keys():
        #     logger.debug(i,type(g[i]))
        #     if isinstance(g[i],np.ndarray):
        #         logger.debug(len(g[i]))
        #logger.debug("CB13qd: %s grtyp/ref=%s/%s, ni/nj=%d,%d, gridID=%d" % (r['nomvar'], g['grtyp'], g['grref'], g['ni'], g['nj'], g['id']))
        #logger.debug("     lat0/lon0  =%f, %f" % (g['lat0'], g['lon0']))
        #logger.debug("     ldlat/dlon =%f, %f" % (g['dlat'], g['dlon']))
        #logger.debug("     xlat`/xlon1=%f, %f; xlat2/xlon2=%f, %f" % (g['xlat1'], g['xlon1'], g['xlat2'], g['xlon2']))
        #logger.debug("     ax: min=%f, max=%f; ay: min=%f, max=%f" % (g['ax'].min(), g['ax'].max(), g['ay'].min(), g['ay'].max()))





def load_data(df):
    import dask.array as da
    from .dataframe import sort_dataframe
    path_groups = df.groupby(df.path)
    res_list = []
    for _,path_df in path_groups:
        unit=rmn.fstopenall(path_df.iloc[0]['path'],rmn.FST_RO)
        #path_df.sort_values(by=['key'],inplace=True)
        for i in path_df.index:
            if isinstance(path_df.at[i,'d'],da.core.Array):
                continue
            if isinstance(path_df.at[i,'d'],np.ndarray):
                # if path_df.at[i,'d'].dtype == 'O':
                #     path_df.at[i,'d'] = path_df.at[i,'d'].tolist()
                #     j_list = []
                #     for j in path_df.at[i,'d']:
                #         j_list.append(j[0](j[1],j[2]))
                #     path_df.at[i,'d'] = j_list
                #     path_df.at[i,'fstinl_params'] = None   
                    continue
            #call stored function with param, rmn.fstluk(key) 
            path_df.at[i,'d'] = path_df.at[i,'d'][0](path_df.at[i,'d'][1],path_df.at[i,'d'][2])
            path_df.at[i,'fstinl_params'] = None
            # path_df.at[i,'path'] = None
        res_list.append(path_df)
        rmn.fstcloseall(unit)
    if len(res_list) >= 1:
        res_df = pd.concat(res_list)    
    else:
        res_df = df
    res_df = sort_dataframe(res_df)
    res_df.reset_index(drop=True,inplace=True)
    return res_df    







def get_2d_lat_lon(df:pd.DataFrame) -> pd.DataFrame:
    from .std_io import get_lat_lon
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





# def fst_to_df(file_id:int, exception_class, load_data:bool, subset) -> pd.DataFrame:
#     """[summary]

#     :param file_id: unit id of the fst file
#     :type file_id: int
#     :param exception_class: exception class to raise in case of error
#     :type exception_class: Exception
#     :param load_data: if True, reads the meta data and data, default False
#     :type load_data: bool
#     :param subset: passes parameters to fstinl to create a selection of records insteand of reading all records
#     :type subset: dict|None
#     :raises exception_class: raised exception in case of error
#     :return: all the read records as a pandas DataFrame
#     :rtype: pd.DataFrame
#     """
#     logger.info('read - reading records')

#     number_or_records = rmn.fstnbr(file_id)

#     meta_keys = get_meta_record_keys(file_id)
#     assert len(meta_keys) == len(set(meta_keys))

#     all_keys = get_all_record_keys(file_id, subset)

#     if subset is None:
#         assert number_or_records == len(all_keys)
#     assert len(all_keys) == len(set(all_keys))

#     keys = list(set(all_keys).difference(set(meta_keys)))
 
#     if len(keys) == 0:
#         logger.error('read - no records in file?')
#         raise exception_class('no records in file')

#     if load_data:
#         records = [rmn.fstluk(k) for k in keys]
#     else:    
#         records = [rmn.fstprm(k) for k in keys]
#     #create a dataframe correspondinf to the fst file
#     df = pd.DataFrame(records)
#     if load_data:
#         df['key']=None
#     assert len(df.index) == len(keys)

#     return df

# def get_meta_record_keys(file_id):
#     meta_keys = []
#     for meta_name in StandardFileReader.meta_data:
#         keys = rmn.fstinl(file_id,nomvar=meta_name)
#         if len(keys):
#             meta_keys += keys
#     return meta_keys




















# def load_data(df:pd.DataFrame) -> pd.DataFrame:
#     if 'path' not in df:
#         return df
#     # grou by path
#     path_groups = df.groupby(df.path)
    
#     mat_dfs = []
#     for _, rec_df_view in path_groups:
#         rec_df = rec_df_view.copy(deep=True)
#         #get the first path in this group
#         path = rec_df.iloc[0]['path']
#         compare_modification_times(rec_df, path, rmn.FST_RO,'load_data',StandardFileError)
#         #open the file
#         file_id = rmn.fstopenall(path, filemode=rmn.FST_RO)
#         for i in rec_df.index:
#             # if record is already load_datad, skip
#             if rec_df.at[i,'d'] is None:
#                 key=rec_df.at[i,'key']
#                 #logger.warning('load_data - record %s %s %s %s %s %s %s already load_datad'%(rec_df.at[i,'nomvar'], rec_df.at[i,'typvar'], rec_df.at[i,'etiket'], rec_df.at[i,'ip1'],rec_df.at[i,'ip2'],rec_df.at[i,'ip3'], rec_df.at[i,'datev']))
#                 if ('dirty' in rec_df.columns) and (rec_df.at[i,'dirty']):
#                     if ('fstinl_params' in rec_df.columns) and (rec_df.at[i,'fstinl_params'] is None == False):
#                         # find the old record from load_data info
#                         key_list = rmn.fstinl(file_id,**rec_df.at[i,'fstinl_params'])
#                         if len(key_list) != 1:
#                             raise StandardFileError('load_data - bad record match - either none or more than one match')
#                         #found one matching records
#                         key=key_list[0]    
                
#                 rec = rmn.fstluk(int(key))
#                 rec_df.at[i,'d'] = rec['d']
#                 rec_df.at[i,'key']=None
#         mat_dfs.append(rec_df)    
#         rmn.fstcloseall(file_id)
#     res_df = pd.concat(mat_dfs)   
#     res_df = sort_dataframe(res_df) 
#     return res_df








def remove_from_df(df_to_remove_from:pd.DataFrame, df_to_remove) -> pd.DataFrame:
    columns = df_to_remove.columns.values.tolist()
    columns.remove('d')
    columns.remove('fstinl_params')
    tmp_df = pd.concat([df_to_remove_from, df_to_remove]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df)
    tmp_df.reset_index(inplace=True,drop=True) 
    return tmp_df

class SelectError(Exception):
    pass

def select(df:pd.DataFrame, query_str:str, exclude:bool=False, no_meta_data=False, loose_match=False, no_fail=False, engine=None) -> pd.DataFrame:
    # print a summay ry of query
    #logger.info('select %s' % query_str[0:100])
    # warn if selecting by fhour
    if 'fhour' in query_str:
        logger.warning('select - selecting fhour might not return expected results - it is a claculated value (fhour = deet * npas / 3600.)')
        logger.info('select - avalable forecast hours are %s' % list(df.fhour.unique()))
    if isinstance(engine,str):
        view = df.query(query_str,engine=engine)
        tmp_df = view.copy(deep=True)
    else:
        view = df.query(query_str)
        tmp_df = view.copy(deep=True)
    if tmp_df.empty:
        if no_fail:
            return pd.DataFrame(dtype=object)
        else:
            logger.warning('select - no matching records for query %s' % query_str[0:200])
            raise SelectError('select - failed!')
    if exclude:
        columns = df.columns.values.tolist()
        columns.remove('d')
        columns.remove('fstinl_params')
        tmp_df = pd.concat([df, tmp_df]).drop_duplicates(subset=columns,keep=False)
    tmp_df = sort_dataframe(tmp_df) 
    return tmp_df


def select_zap(df:pd.DataFrame, query:str, **kwargs:dict) -> pd.DataFrame:
    selection_df = select(df,query)
    df = remove_from_df(df,selection_df)
    zapped_df = zap(selection_df,mark=False,**kwargs)
    res_df = pd.concat([df,zapped_df])
    res_df = sort_dataframe(res_df)
    return res_df

def get_intersecting_levels(df:pd.DataFrame, names:list) -> pd.DataFrame:
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

# def create_coordinate_type(meta:set, kind:int, vcode:int, coord_types:pd.DataFrame) ->str:
#     vctype = 'UNKNOWN'
#     toctoc_exists = '!!' in meta
#     p0_exists = 'P0' in meta
#     hy_exists = 'HY' in meta
#     pt_exists = 'PT' in meta
#     e1_exists = 'E1' in meta
#     sf_exists = '!!SF' in meta

#     result = coord_types.query(f'(kind == {kind}) and (toctoc == {toctoc_exists}) and (P0 == {p0_exists}) and (E1 == {e1_exists}) and (PT == {pt_exists}) and (HY == {hy_exists}) and (SF == {sf_exists}) and (vcode == {vcode})')

#     if len(result.index) > 1:
#         logger.debug(result)
#     if len(result.index):
#         vctype = result['vctype'].iloc[0]
#     return vctype


def add_empty_columns(df, columns, init, dtype_str):
    for col in columns:
        df.insert(len(df.columns),col,init)
        df = df.astype({col:dtype_str})
    return df         
    #df = df.reindex(columns = df.columns.tolist() + ['min','max','mean','std','min_pos','max_pos'])            
def compute_stats(df:pd.DataFrame) -> pd.DataFrame:
    add_empty_columns(df, ['min','max','mean','std'],np.nan,'float32')
    initializer = (np.nan,np.nan)
    add_empty_columns(df, ['min_pos','max_pos'],None)
    for i in df.index:
        df.at[i,'mean'] = df.at[i,'d'].mean()
        df.at[i,'std'] = df.at[i,'d'].std()
        df.at[i,'min'] = df.at[i,'d'].min()
        df.at[i,'max'] = df.at[i,'d'].max()
        df.at[i,'min_pos'] = np.unravel_index(df.at[i,'d'].argmin(), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'min_pos'] = (df.at[i,'min_pos'][0] + 1, df.at[i,'min_pos'][1]+1)
        df.at[i,'max_pos'] = np.unravel_index(df.at[i,'d'].argmax(), (df.at[i,'ni'],df.at[i,'nj']))
        df.at[i,'max_pos'] = (df.at[i,'max_pos'][0] + 1, df.at[i,'max_pos'][1]+1)
    #df = fstpy.dataframe.sort_dataframe(df)    
    return df


def voir(df:pd.DataFrame):
    """Displays the metadata of the supplied records in the rpn voir format"""
    validate_df_not_empty(df,'voir',StandardFileError)
    #logger.debug('    NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s) FORECASTHOUR      IP1        LEVEL        IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4')
    print(df[['nomvar','typvar','etiket','ni','nj','nk','pdateo','fhour','level','ip1','ip2','ip3','deet','npas','pdatyp','nbits','grtyp','ig1','ig2','ig3','ig4']].sort_values(by=['nomvar']).reset_index(drop=True).to_string(header=True, formatters={'level': '{:,.6f}'.format, 'fhour': '{:,.6f}'.format}))

def validate_zap_keys(**kwargs):
    available_keys = {'grid', 'fhour', 'nomvar', 'typvar', 'etiket', 'dateo', 'datev', 'ip1', 'ip2', 'ip3', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'level', 'kind', 'pkind','unit'}
    keys_to_modify = set(kwargs.keys())
    acceptable_keys = keys_to_modify.intersection(available_keys)
    if len(acceptable_keys) != len(keys_to_modify):
        logger.warning("zap - can't find modifiable key in available keys. asked for %s in %s"%(keys_to_modify,available_keys))
        raise StandardFileError("zap - can't find modifiable key in available keys")

def zap_ip1(df:pd.DataFrame, ip1_value:int) -> pd.DataFrame:
    logger.warning('zap - changed ip1, triggers updating level and kind')
    df.loc[:,'ip1'] = ip1_value
    level, kind = get_level_and_kind(ip1_value)
    df.loc[:,'level'] = level
    df.loc[:,'kind'] = kind
    df.loc[:,'pkind'] = KIND_DICT[int(kind)]
    return df

def zap_level(df:pd.DataFrame, level_value:float, kind_value:int) -> pd.DataFrame:
    logger.warning('zap - changed level, triggers updating ip1')
    df['level'] = level_value
    df['ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, level_value, kind_value)
    return df

def zap_kind(df:pd.DataFrame, kind_value:int) -> pd.DataFrame:
    logger.warning('zap - changed kind, triggers updating ip1 and pkind')
    df['kind'] = kind_value
    df['pkind'] = KIND_DICT[int(kind_value)]
    for i in df.index:
        df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], kind_value)
    return df

def zap_pkind(df:pd.DataFrame, pkind_value:str) -> pd.DataFrame:
    logger.warning('zap - changed pkind, triggers updating ip1 and kind')
    df['pkind'] = pkind_value
    #invert kind dict
    PKIND_DICT = {v: k for k, v in KIND_DICT.items()}
    df['kind'] = PKIND_DICT[pkind_value]
    for i in df.index:
        df.at[i,'ip1'] = rmn.convertIp(rmn.CONVIP_ENCODE, df.at[i,'level'], df.at[i,'kind'])
    return df

def zap_npas(df:pd.DataFrame, npas_value:int) -> pd.DataFrame:
    logger.warning('zap - changed npas, triggers updating fhour')
    df['npas'] = npas_value
    for i in df.index:
        df.at[i,'fhour'] =  df.at[i,'deet'] * df.at[i,'npas'] / 3600.
        df.at[i,'ip2'] = np.floor(df.at[i,'fhour']).astype(int)
    return df


def zap_fhour(df:pd.DataFrame, fhour_value:int) -> pd.DataFrame:
    logger.warning('zap - changed fhour, triggers updating npas')
    df['fhour'] = fhour_value
    df['ip2'] = np.floor(df['fhour']).astype(int)
    for i in df.index:
        df.at[i,'npas'] = df.at[i,'fhour'] * 3600. / df.at[i,'deet']
        df.at[i,'npas'] = df.at[i,'npas'].astype(int)
    return df

def create_load_data_info(df:pd.DataFrame) -> pd.DataFrame:
    for i in df.index:
        if df.at[i,'d'] is None == False:
            return df
        if df.at[i,'key'] != None:
            fstinl_params={
            'etiket':df.at[i,'etiket'],
            'datev':df.at[i,'datev'],
            'ip1':df.at[i,'ip1'],
            'ip2':df.at[i,'ip2'],
            'ip3':df.at[i,'ip3'],
            'typvar':df.at[i,'typvar'],
            'nomvar':df.at[i,'nomvar']}
            df.at[i,'fstinl_params'] = fstinl_params
    return df


def zap(df:pd.DataFrame, mark:bool=True, validate_keys=True,**kwargs:dict ) -> pd.DataFrame:
    """ Modifies records from the input file or other supplied records according to specific criteria
        kwargs: can contain these key, value pairs to select specific records from the input file or input records
                nomvar=str
                typvar=str
                etiket=str
                dateo=int
                datev=int
                ip1=int
                ip2=int
                ip3=int
                deet=int
                npas=int
                datyp=int
                nbits=int
                grtyp=str
                ig1=int
                ig2=int
                ig3=int
                ig4=int """
    validate_df_not_empty(df,'zap',StandardFileError)            
    if validate_zap_keys:
        validate_zap_keys(**kwargs)

    logger.info('zap - ' + str(kwargs)[0:100] + '...')

    res_df = create_load_data_info(df)
    res_df.loc[:,'dirty'] = True
    #res_df['key'] = np.nan
    for k,v in kwargs.items():
        if (k == 'level') and ('kind' in  kwargs.keys()):
            res_df = zap_level(res_df,v,kwargs['kind'])
            continue
        elif (k == 'level') and ('kind' not in  kwargs.keys()):
            logger.warning("zap - can't zap level without kind")
            continue
        if k == 'ip1':
            res_df = zap_ip1(res_df,v)
            continue
        if k == 'npas':
            res_df = zap_npas(res_df, v)
            continue
        if k == 'fhour':
            res_df = zap_fhour(res_df, v)
            continue
        if k == 'kind':
            pass
        if k == 'pkind':
            pass
        res_df.loc[:,k] = v
    if mark:
        res_df.loc[:,'typvar'] = res_df['typvar'].str.cat([ 'Z' for x in res_df.index])
    res_df = sort_dataframe(res_df) 
    return res_df

def fststat(df:pd.DataFrame):
    """ reads the data from the supplied records and calculates then displays the statistics for that record """
    logger.info('fststat')
    pd.options.display.float_format = '{:0.6E}'.format
    validate_df_not_empty(df,'fststat',StandardFileError)
    df = load_data(df)
    df = compute_stats(df)
    print(df[['nomvar','typvar','level','pkind','ip2','ip3','dateo','etiket','mean','std','min_pos','min','max_pos','max']].to_string(formatters={'level':'{:,.6f}'.format}))


def add_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
    diff['abs_diff'] = diff['d_x'].copy(deep=True)
    diff['e_rel_max'] = diff['d_x'].copy(deep=True)
    diff['e_rel_moy'] = diff['d_x'].copy(deep=True)
    diff['var_a'] = diff['d_x'].copy(deep=True)
    diff['var_b'] = diff['d_x'].copy(deep=True)
    diff['c_cor'] = diff['d_x'].copy(deep=True)
    diff['moy_a'] = diff['d_x'].copy(deep=True)
    diff['moy_b'] = diff['d_x'].copy(deep=True)
    diff['bias'] = diff['d_x'].copy(deep=True)
    diff['e_max'] = diff['d_x'].copy(deep=True)
    diff['e_moy'] = diff['d_x'].copy(deep=True)
    diff.drop(columns=['d_x', 'd_y'], inplace=True)
    return diff


def del_fstcomp_columns(diff: pd.DataFrame) -> pd.DataFrame:
    diff['etiket'] = diff['etiket_x']
    diff['pkind'] = diff['pkind_x']
    #diff['ip2'] = diff['ip2_x']
    #diff['ip3'] = diff['ip3_x']
    diff.drop(columns=['abs_diff'], inplace=True)
    return diff


def compute_fstcomp_stats(common: pd.DataFrame, diff: pd.DataFrame) -> bool:
    from math import isnan
    success = True

    for i in common.index:
        a = common.at[i, 'd_x'].flatten()
        b = common.at[i, 'd_y'].flatten()
        diff.at[i, 'abs_diff'] = np.abs(a-b)

        derr = np.where(a == 0, np.abs(1-a/b), np.abs(1-b/a))
        derr_sum=np.sum(derr)
        if isnan(derr_sum):
            diff.at[i, 'e_rel_max'] = 0.
            diff.at[i, 'e_rel_moy'] = 0.
        else:    
            diff.at[i, 'e_rel_max'] = 0. if isnan(np.nanmax(derr)) else np.nanmax(derr)
            diff.at[i, 'e_rel_moy'] = 0. if isnan(np.nanmean(derr)) else np.nanmean(derr)
        sum_a2 = np.sum(a**2)
        sum_b2 = np.sum(b**2)
        diff.at[i, 'var_a'] = np.mean(sum_a2)
        diff.at[i, 'var_b'] = np.mean(sum_b2)
        diff.at[i, 'moy_a'] = np.mean(a)
        diff.at[i, 'moy_b'] = np.mean(b)
        
        c_cor = np.sum(a*b)
        if sum_a2*sum_b2 != 0:
            c_cor = c_cor/np.sqrt(sum_a2*sum_b2)
        elif (sum_a2==0) and (sum_b2==0):
            c_cor = 1.0
        elif sum_a2 == 0:
            c_cor = np.sqrt(diff.at[i, 'var_b'])
        else:
            c_cor = np.sqrt(diff.at[i, 'var_a'])
        diff.at[i, 'c_cor'] = c_cor 
        diff.at[i, 'biais']=diff.at[i, 'moy_b']-diff.at[i, 'moy_a']
        diff.at[i, 'e_max'] = np.max(diff.at[i, 'abs_diff'])
        diff.at[i, 'e_moy'] = np.mean(diff.at[i, 'abs_diff'])
        
        nbdiff = np.count_nonzero(a!=b)
        diff.at[i, 'diff_percent'] = nbdiff / a.size * 100.0
        if not ((-1.000001 <= diff.at[i, 'c_cor'] <= 1.000001) and (-0.1 <= diff.at[i, 'e_rel_max'] <= 0.1) and (-0.1 <= diff.at[i, 'e_rel_moy'] <= 0.1)):
            diff.at[i, 'nomvar'] = '<' + diff.at[i, 'nomvar'] + '>'
            success = False
    return success        





#e_rel_max   E-REL-MOY   VAR-A        C-COR        MOY-A        BIAIS       E-MAX       E-MOY


def fstcomp_df(df1: pd.DataFrame, df2: pd.DataFrame, exclude_meta=True, columns=['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], print_unmatched=False) -> bool:
    df1.sort_values(by=columns,inplace=True)
    df2.sort_values(by=columns,inplace=True)
    success = False
    pd.options.display.float_format = '{:0.6E}'.format
    # check if both df have records
    if df1.empty or df2.empty:
        logger.error('you must supply files witch contain records')
        if df1.empty:
            logger.error('file 1 is empty')
        if df2.empty:
            logger.error('file 2 is empty')
        raise StandardFileError('fstcomp - one of the files is empty')
    # remove meta data {!!,>>,^^,P0,PT,HY,!!SF} from records to compare
    if exclude_meta:
        df1 = remove_meta_data_fields(df1)
        df2 = remove_meta_data_fields(df2)
    # logger.debug('A',df1['d'][:100].to_string())
    # logger.debug('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
    # logger.debug('----------')
    # logger.debug('B',df2['d'][:100].to_string())
    # logger.debug('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())    
    # check if they are exactly the same
    if df1.equals(df2):
        # logger.debug('files are indetical - excluding meta data fields')
        # logger.debug('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
        # logger.debug('----------')
        # logger.debug('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4','path']].to_string())
        return True
    #create common fields
    common = pd.merge(df1, df2, how='inner', on=columns)
    #Rows in df1 Which Are Not Available in df2
    common_with_1 = common.merge(df1, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
    #Rows in df2 Which Are Not Available in df1
    common_with_2 = common.merge(df2, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
    missing = pd.concat([common_with_1, common_with_2])
    missing = remove_meta_data_fields(missing)
    if len(common.index) != 0:
        if len(common_with_1.index) != 0:
            if print_unmatched:
                logger.info('df in file 1 that were not found in file 2 - excluded from comparison')
                logger.info(common_with_1.to_string())
        if len(common_with_2.index) != 0:
            if print_unmatched:
                logger.info('df in file 2 that were not found in file 1 - excluded from comparison')
                logger.info(common_with_2.to_string())
    else:
        logger.error('fstcomp - no common df to compare')
        logger.error('A',df1[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].to_string())
        logger.error('----------')
        logger.error('B',df2[['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4']].to_string())
        raise StandardFileError('fstcomp - no common df to compare')
    diff = common.copy()
    diff = add_fstcomp_columns(diff)
    success = compute_fstcomp_stats(common, diff)
    diff = del_fstcomp_columns(diff)
    if len(diff.index):
        logger.info(diff[['nomvar', 'etiket', 'level', 'pkind', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'biais', 'e_max', 'e_moy','diff_percent']].to_string(formatters={'level': '{:,.6f}'.format,'diff_percent': '{:,.1f}%'.format}))
        #logger.debug(diff[['nomvar', 'etiket', 'pkind', 'ip2', 'ip3', 'e_rel_max', 'e_rel_moy', 'var_a', 'var_b', 'c_cor', 'moy_a', 'moy_b', 'bias', 'e_max', 'e_moy']].to_string())
    if len(missing.index):
        logger.info('missing df')
        logger.info(missing[['nomvar', 'etiket', 'level', 'pkind', 'ip2', 'ip3']].to_string(header=False, formatters={'level': '{:,.6f}'.format}))
        #logger.debug(missing[['nomvar', 'etiket', 'pkind', 'ip2', 'ip3']].to_string(header=False))
    return success

def fstcomp(file1:str, file2:str, columns=['nomvar', 'ni', 'nj', 'nk', 'dateo', 'level', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'], verbose=False) -> bool:
    logger.info('fstcomp -A %s -B %s'%(file1,file2))
    import os
    if not os.path.exists(file1):
        logger.error('fstcomp - %s does not exist' % file1)
        raise StandardFileError('fstcomp - %s does not exist' % file1)
    if not os.path.exists(file2):
        logger.error('fstcomp - %s does not exist' % file2)
        raise StandardFileError('fstcomp - %s does not exist' % file2)    
    # open and read files
    df1 = StandardFileReader(file1)()
    df1 = load_data(df1)
    df2 = StandardFileReader(file2)()
    df2 = load_data(df2)
    return fstcomp_df(df1, df2, columns, print_unmatched=True if verbose else False)



def keys_to_remove(keys, the_dict):
    for key in keys:
        if key in the_dict:
            del the_dict[key]


def add_metadata_fields(df:pd.DataFrame, latitude_and_longitude=True, pressure=True, vertical_descriptors=True) -> pd.DataFrame:
    if 'path' not in df:
        logger.warning('add_metadata_fields - no path to get meta from')
        return df

    meta_df = get_meta_data_fields(df, 'add_metadata_fields',StandardFileError,latitude_and_longitude, pressure, vertical_descriptors)

    res = pd.concat([df,meta_df])

    return res
        


# def get_vertical_grid_descriptor(self, records:list):
#     """ Gets the vertical grid descriptor associated with the supplied records"""
#     logger.info('get_vertical_grid_descriptor')
#     if records == None or not len(records):
#         logger.error('get_vertical_grid_descriptor - no records to process')
#         return
# #    ip1      (int)  : Ip1 of the vgrid record to find, use -1 for any (I)
# #    ip2      (int)  : Ip2 of the vgrid record to find, use -1 for any (I)
# #    kind     (int)  : Kind of vertical coor
# #    version  (int)  : Version of vertical coor
# #https://wiki.cmc.ec.gc.ca/wiki/Python-RPN/2.1/rpnpy/vgd/const
#     for rec in records:
#         v = vgd.vgd_read(self._fileid_in, ip1=rec.ig1, ip2=rec.ig2)
#         # Get Some info about the vgrid
#         vkind    = vgd.vgd_get(v, 'KIND')
#         vver     = vgd.vgd_get(v, 'VERS')
#         ip1diagt = vgd.vgd_get(v, 'DIPT')
#         ip1diagm = vgd.vgd_get(v, 'DIPM')
#         tlvl     = vgd.vgd_get(v, 'VIPT')
#         # mlvl     = vgd.vgd_get(v, 'VIPM')

#         VGD_KIND_VER_INV = dict((v, k) for k, v in vgd.VGD_KIND_VER.items())
#         vtype = VGD_KIND_VER_INV[(vkind,vver)]
#         (ldiagval, ldiagkind) = rmn.convertIp(rmn.CONVIP_DECODE, ip1diagt)
#         (l2diagval, l2diagkind) = rmn.convertIp(rmn.CONVIP_DECODE, ip1diagm)
#         logger.debug("CB14: Found vgrid type=%s (kind=%d, vers=%d) with %d levels and diag levels=%7.2f%s (ip1=%d) and %7.2f%s (ip1=%d)" % (vtype, vkind, vver, len(tlvl), ldiagval, rmn.kindToString(ldiagkind), ip1diagt, l2diagval, rmn.kindToString(l2diagkind), ip1diagm))








###############################################################################
# Copyright 2017 - Climate Research Division
#                  Environment and Climate Change Canada
#
# This file is part of the "fstd2nc" package.
#
# "fstd2nc" is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# "fstd2nc" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with "fstd2nc".  If not, see <http://www.gnu.org/licenses/>.
###############################################################################

"""
Optional helper functions.
Note: These rely on some assumptions about the internal structures of librmn,
      and may fail for future libray verisons.
      These have been tested for librmn 15.2 and 16.2.
"""

from ctypes import Structure, POINTER, c_void_p, c_uint32, c_int32, c_int, c_uint, c_byte, c_char_p
from rpnpy.librmn import librmn


# From fnom.h
MAXFILES = 1024
class attributs(Structure):
  _fields_ = [
     ('stream',c_uint,1), ('std',c_uint,1), ('burp',c_uint,1), ('rnd',c_uint,1), ('wa',c_uint,1), ('ftn',c_uint,1),
     ('unf',c_uint,1), ('read_only',c_uint,1), ('old',c_uint,1), ('scratch',c_uint,1), ('notpaged',c_uint,1),
     ('pipe',c_uint,1), ('write_mode',c_uint,1), ('remote',c_uint,1), ('padding',c_uint,18),
  ]

class general_file_info(Structure):
  _fields_ = [
    ('file_name',c_char_p),            # complete file name
    ('subname',c_char_p),              # sub file name for cmcarc files
    ('file_type',c_char_p),            # file type and options
    ('iun',c_int),                    # fnom unit number
    ('fd',c_int),                     # file descriptor
    ('file_size',c_int),              # file size in words
    ('eff_file_size',c_int),          # effective file size in words
    ('lrec',c_int),                   # record length when appliable
    ('open_flag',c_int),              # open/close flag
    ('attr',attributs),
  ]


Fnom_General_File_Desc_Table = (general_file_info*MAXFILES).in_dll(librmn,'Fnom_General_File_Desc_Table')

# From rpnmacros.h
word = c_uint32

# From qstdir.h
MAX_XDF_FILES = 1024
ENTRIES_PER_PAGE = 256
MAX_DIR_PAGES = 1024
MAX_PRIMARY_LNG = 16
MAX_SECONDARY_LNG = 8
max_dir_keys = word*MAX_PRIMARY_LNG
max_info_keys = word*MAX_SECONDARY_LNG

class stdf_dir_keys(Structure): pass  #defined further below

class xdf_dir_page(Structure):
  _fields_ = [
        ('lng',word,24), ('idtyp',word,8), ('addr',word,32), # XDF record header
        ('reserved1',word,32), ('reserved2',word,32),
        ('nxt_addr',word,32), ('nent',word,32),
        ('chksum',word,32), ('reserved3',word,32),
        ('entry',stdf_dir_keys*ENTRIES_PER_PAGE),
  ]
# idtyp:     id type (usualy 0)
# lng:       header length (in 64 bit units)
# addr:      address of directory page (origin 1, 64 bit units)
# reserved1: idrep (4 ascii char 'DIR0')
# reserved2: reserved (0)
# nxt_addr:  address of next directory page (origin 1, 64 bit units)
# nent:      number of entries in page
# chksum:    checksum (not valid when in core)
# page_no, record_no, file_index: handle templage
# entry:     (real allocated dimension will be ENTRIES_PER_PAGE * primary_len)

class full_dir_page(Structure):
  pass
page_ptr = POINTER(full_dir_page)
full_dir_page._fields_ = [
        ('next_page',page_ptr),
        ('prev_page',page_ptr),
        ('modified',c_int),
        ('true_file_index',c_int),
        ('dir',xdf_dir_page),
]

class file_record(Structure):
  _fields_ = [
        ('lng',word,24), ('idtyp',word,8), ('addr',word,32),        #XDF record header
        ('data',word*2),                        # primary keys, info keys, data
  ]


stdf_dir_keys._fields_ = [
        ('lng',word,24), ('select',word,7), ('deleted',word,1), ('addr',word,32),
        ('nbits',word,8), ('deet',word,24), ('gtyp',word,8), ('ni',word,24),
        ('datyp',word,8), ('nj',word,24), ('ubc',word,12), ('nk',word,20),
        ('pad7',word,6), ('npas',word,26), ('ig2a',word,8), ('ig4',word,24),
        ('ig2b',word,8), ('ig1',word,24), ('ig2c',word,8), ('ig3',word,24),
        ('pad1',word,2), ('etik15',word,30), ('pad2',word,2), ('etik6a',word,30),
        ('pad3',word,8), ('typvar',word,12), ('etikbc',word,12), ('pad4',word,8), ('nomvar',word,24),
        ('levtyp',word,4), ('ip1',word,28), ('pad5',word,4), ('ip2',word,28),
        ('pad6',word,4), ('ip3',word,28), ('date_stamp',word,32),
  ]


class key_descriptor(Structure):
  _fields_ = [
        ('ncle',word,32), ('reserved',word,8), ('tcle',word,6), ('lcle',word,5), ('bit1',word,13),
  ]


class file_header(Structure):
  _fields_ = [
        ('lng',word,24), ('idtyp',word,8), ('addr',word,32),  # standard XDF record header
        ('vrsn',word),     ('sign',word),               #char[4]
        ('fsiz',word,32), ('nrwr',word,32),
        ('nxtn',word,32), ('nbd',word,32),
        ('plst',word,32), ('nbig',word,32),
        ('lprm',word,16), ('nprm',word,16), ('laux',word,16), ('naux',word,16),
        ('neff',word,32), ('nrec',word,32),
        ('rwflg',word,32), ('reserved',word,32),
        ('keys',key_descriptor*1024),
  ]
# idtyp:     id type (usualy 0)
# lng:       header length (in 64 bit units)
# addr:      address (exception: 0 for a file header)
# vrsn:      XDF version
# sign:      application signature
# fsiz:      file size (in 64 bit units)
# nrwr:      number of rewrites
# nxtn:      number of extensions
# nbd:       number of directory pages
# plst:      address of last directory page (origin 1, 64 bit units)
# nbig:      size of biggest record
# nprm:      number of primary keys
# lprm:      length of primary keys (in 64 bit units)
# naux:      number of auxiliary keys
# laux:      length of auxiliary keys
# neff:      number of erasures
# nrec:      number of valid records
# rwflg:     read/write flag
# reserved:  reserved
# keys:      key descriptor table


class file_table_entry(Structure):
  _fields_ = [
        ('dir_page',page_ptr*MAX_DIR_PAGES), # pointer to directory pages
        ('cur_dir_page',page_ptr),           # pointer to current directory page
        ('build_primary',c_void_p),       # pointer to primary key building function
        ('build_info',c_void_p),           # pointer to info building function
        ('scan_file',c_void_p),            # pointer to file scan function
        ('file_filter',c_void_p),          # pointer to record filter function
        ('cur_entry',POINTER(word)),              # pointer to current directory entry
        ('header',POINTER(file_header)),          # pointer to file header
        ('nxtadr',c_int32),                # next write address (in word units)
        ('primary_len',c_int),
        # length in 64 bit units of primary keys (including 64 bit header)
        ('info_len',c_int),                 #length in 64 bit units of info keys
        ('link',c_int),                     # file index to next linked file,-1 if none
        ('cur_info',POINTER(general_file_info)),
                                      # pointer to current general file desc entry
        ('iun',c_int),                      # FORTRAN unit number, -1 if not open, 0 if C file
        ('file_index',c_int),               # index into file table, -1 if not open
        ('modified',c_int),                 # modified flag
        ('npages',c_int),                   # number of allocated directory pages
        ('nrecords',c_int),                 # number of records in file
        ('cur_pageno',c_int),               # current page number
        ('page_record',c_int),              # record number within current page
        ('page_nrecords',c_int),            # number of records in current page
        ('file_version',c_int),             # version number
        ('valid_target',c_int),             # last search target valid flag
        ('xdf_seq',c_int),                  # file is sequential xdf
        ('valid_pos',c_int),                # last position valid flag (seq only)
        ('cur_addr',c_int),                 # current address (WA, sequential xdf)
        ('seq_bof',c_int),                  # address (WA) of first record (seq xdf)
        ('fstd_vintage_89',c_int),          # old standard file flag
        ('head_keys',max_dir_keys),       # header & primary keys for last record
        ('info_keys',max_info_keys),      # info for last read/written record
        ('cur_keys',max_dir_keys),        # keys for current operation
        ('target',max_dir_keys),          # current search target
        ('srch_mask',max_dir_keys),       # permanent search mask for this file
        ('cur_mask',max_dir_keys),        # current search mask for this file
  ]
file_table_entry_ptr = POINTER(file_table_entry)

librmn.file_index.argtypes = (c_int,)
librmn.file_index.restype = c_int
file_table = (file_table_entry_ptr*MAX_XDF_FILES).in_dll(librmn,'file_table')


def all_params (funit, out=None):
  '''
  Extract parameters for *all* records.
  Returns a dictionary similar to fstprm, only the entries are
  vectorized over all records instead of 1 record at a time.
  NOTE: This includes deleted records as well.  You can filter them out using
        the 'dltf' flag.
  '''
  from ctypes import cast
  import numpy as np
  # Get the raw (packed) parameters.
  index = librmn.file_index(funit)
  raw = []
  file_index_list = []
  pageno_list = []
  recno_list = []
  while index >= 0:
    f = file_table[index].contents
    for pageno in range(f.npages):

      page = f.dir_page[pageno].contents
      params = cast(page.dir.entry,POINTER(c_uint32))
      params = np.ctypeslib.as_array(params,shape=(ENTRIES_PER_PAGE,9,2))
      nent = page.dir.nent
      raw.append(params[:nent])
      recno_list.extend(list(range(nent)))
      pageno_list.extend([pageno]*nent)
      file_index_list.extend([index]*nent)
    index = f.link
  raw = np.concatenate(raw)
  
  # Start unpacking the pieces.
  # Reference structure (from qstdir.h):
  # 0      word deleted:1, select:7, lng:24, addr:32;
  # 1      word deet:24, nbits: 8, ni:   24, gtyp:  8;
  # 2      word nj:24,  datyp: 8, nk:   20, ubc:  12;
  # 3      word npas: 26, pad7: 6, ig4: 24, ig2a:  8;
  # 4      word ig1:  24, ig2b:  8, ig3:  24, ig2c:  8;
  # 5      word etik15:30, pad1:2, etik6a:30, pad2:2;
  # 6      word etikbc:12, typvar:12, pad3:8, nomvar:24, pad4:8;
  # 7      word ip1:28, levtyp:4, ip2:28, pad5:4;
  # 8      word ip3:28, pad6:4, date_stamp:32;
  nrecs = raw.shape[0]
  if out is None:
    out = {}
    out['lng'] = np.empty(nrecs, dtype='int32')
    out['dltf'] = np.empty(nrecs, dtype='ubyte')
    out['swa'] =  np.empty(nrecs, dtype='uint32')
    out['deet'] = np.empty(nrecs, dtype='int32')
    out['nbits'] = np.empty(nrecs, dtype='byte')
    out['grtyp'] = np.empty(nrecs, dtype='|S1')
    out['ni'] = np.empty(nrecs, dtype='int32')
    out['nj'] = np.empty(nrecs, dtype='int32')
    out['datyp'] = np.empty(nrecs, dtype='ubyte')
    out['nk'] = np.empty(nrecs, dtype='int32')
    out['ubc'] = np.empty(nrecs, dtype='uint16')
    out['npas'] = np.empty(nrecs, dtype='int32')
    out['ig1'] = np.empty(nrecs, dtype='int32')
    out['ig2'] = np.empty(nrecs, dtype='int32')
    out['ig3'] = np.empty(nrecs, dtype='int32')
    out['ig4'] = np.empty(nrecs, dtype='int32')
    out['etiket'] = np.empty(nrecs,dtype='|S12')
    out['typvar'] = np.empty(nrecs,dtype='|S2')
    out['nomvar'] = np.empty(nrecs,dtype='|S4')
    out['ip1'] = np.empty(nrecs, dtype='int32')
    out['ip2'] = np.empty(nrecs, dtype='int32')
    out['ip3'] = np.empty(nrecs, dtype='int32')
    out['datev'] = np.empty(nrecs, dtype='int32')
    out['dateo'] = np.empty(nrecs, dtype='int32')
    out['xtra1'] = np.empty(nrecs, dtype='uint32')
    out['xtra2'] = np.empty(nrecs, dtype='uint32')
    out['xtra3'] = np.empty(nrecs, dtype='uint32')
    out['key'] = np.empty(nrecs, dtype='int32')

  temp8 = np.empty(nrecs, dtype='ubyte')
  temp32 = np.empty(nrecs, dtype='int32')

  np.divmod(raw[:,0,0],2**24, temp8, out['lng'])
  np.divmod(temp8,128, out['dltf'], temp8)
  out['swa'][:] = raw[:,0,1]
  np.divmod(raw[:,1,0],256, out['deet'], out['nbits'])
  np.divmod(raw[:,1,1],256, out['ni'], out['grtyp'].view('ubyte'))
  np.divmod(raw[:,2,0],256, out['nj'], out['datyp'])
  np.divmod(raw[:,2,1],4096, out['nk'], out['ubc'])
  out['npas'][:] = raw[:,3,0]//64
  np.divmod(raw[:,3,1],256, out['ig4'], temp32)
  out['ig2'][:] = (temp32 << 16) # ig2a
  np.divmod(raw[:,4,0],256, out['ig1'], temp32)
  out['ig2'] |= (temp32 << 8) # ig2b
  np.divmod(raw[:,4,1],256, out['ig3'], temp32)
  out['ig2'] |= temp32 # ig2c
  etik15 = raw[:,5,0]//4
  etik6a = raw[:,5,1]//4
  et = raw[:,6,0]//256
  etikbc, _typvar = divmod(et, 4096)
  _nomvar = raw[:,6,1]//256
  np.divmod(raw[:,7,0],16, out['ip1'], temp8)
  out['ip2'][:] = raw[:,7,1]//16
  out['ip3'][:] = raw[:,8,0]//16
  date_stamp = raw[:,8,1]
  # Reassemble and decode.
  # (Based on fstd98.c)
  etiket_bytes = np.empty((nrecs,12),dtype='ubyte')
  for i in range(5):
    etiket_bytes[:,i] = ((etik15 >> ((4-i)*6)) & 0x3f) + 32
  for i in range(5,10):
    etiket_bytes[:,i] = ((etik6a >> ((9-i)*6)) & 0x3f) + 32
  etiket_bytes[:,10] = ((etikbc >> 6) & 0x3f) + 32
  etiket_bytes[:,11] = (etikbc & 0x3f) + 32
  out['etiket'][:] = etiket_bytes.flatten().view('|S12')
  nomvar_bytes = np.empty((nrecs,4),dtype='ubyte')
  for i in range(4):
    nomvar_bytes[:,i] = ((_nomvar >> ((3-i)*6)) & 0x3f) + 32
  out['nomvar'][:] = nomvar_bytes.flatten().view('|S4')
  typvar_bytes = np.empty((nrecs,2),dtype='ubyte')
  typvar_bytes[:,0] = ((_typvar >> 6) & 0x3f) + 32
  typvar_bytes[:,1] = ((_typvar & 0x3f)) + 32
  out['typvar'][:] = typvar_bytes.flatten().view('|S2')
  out['datev'][:] = (date_stamp >> 3) * 10 + (date_stamp & 0x7)
  # Note: this dateo calculation is based on my assumption that
  # the raw stamps increase in 5-second intervals.
  # Doing it this way to avoid a gazillion calls to incdat.
  date_stamp = date_stamp - (out['deet']*out['npas'])//5
  out['dateo'][:] = (date_stamp >> 3) * 10 + (date_stamp & 0x7)
  out['xtra1'][:] = out['datev']
  out['xtra2'][:] = 0
  out['xtra3'][:] = 0
  # Calculate the handles (keys)
  # Based on "MAKE_RND_HANDLE" macro in qstdir.h.
  out['key'][:] = (np.array(file_index_list)&0x3FF) | ((np.array(recno_list)&0x1FF)<<10) | ((np.array(pageno_list)&0xFFF)<<19)

  return out



