# -*- coding: utf-8 -*-
import itertools
import multiprocessing as mp
import os

import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn


# import xarray as xr

from .dataframe import add_columns, add_data_column, add_shape_column, drop_duplicates
from .exceptions import StandardFileError
from .std_io import (compare_modification_times, 
                     parallel_get_dataframe_from_file, get_dataframe_from_file,read_record)
from .utils import initializer


class StandardFileReader:
    """Class to handle fst files
        Opens, reads the contents of an fst files or files into a pandas Dataframe and closes  
        No data is loaded unless specified, only the metadata is read. Extra metadata is added to the dataframe if specified.  
  
        :param filenames: path to file or list of paths to files  
        :type filenames: str|list[str]  
        :param decode_metadata: adds extra columns, defaults to False  
                'unit':str, unit name   
                'unit_converted':bool  
                'description':str, field description   
                'date_of_observation':datetime, of the date of observation   
                'date_of_validity':datetime, of the date of validity   
                'level':float32, decoded ip1 level   
                'ip1_kind':int32, decoded ip1 kind   
                'ip1_pkind':str, string repr of ip1_kind int   
                'data_type_str':str, string repr of data type   
                'label':str, label derived from etiket   
                'run':str, run derived from etiket   
                'implementation':str, implementation derived from etiket   
                'ensemble_member':str, ensemble member derived from etiket   
                'surface':bool, True if the level is a surface level   
                'follow_topography':bool, indicates if this type of level follows topography   
                'ascending':bool, indicates if this type of level is in ascending order   
                'vctype':str, vertical level type   
                'forecast_hour':timedelta, forecast hour obtained from deet * npas / 3600   
                'ip2_dec':value of decoded ip2    
                'ip2_kind':kind of decoded ip2    
                'ip2_pkind':printable kind of decoded ip2   
                'ip3_dec':value of decoded ip3   
                'ip3_kind':kind of decoded ip3   
                'ip3_pkind':printable kind of decoded ip3   
        :type decode_metadata: bool, optional  
        :param load_data: if True, the data will be read, not just the metadata (fstluk vs fstprm), default False  
        :type load_data: bool, optional  
        :param query: parameter to pass to dataframe.query method, to select specific records  
        :type query: str, optional  
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1","PN"]
    @initializer
    def __init__(self, filenames, decode_metadata=False,load_data=False,query=None):
        """init instance"""
        if isinstance(self.filenames,str):
            self.filenames = os.path.abspath(str(self.filenames))
        elif isinstance(self.filenames,list):
            self.filenames = [os.path.abspath(str(f)) for f in filenames]
        else:
            raise StandardFileError('Filenames must be str or list\n')



    def to_pandas(self) -> pd.DataFrame:
        """creates the dataframe from the provided file metadata

        :return: df
        :rtype: pd.Dataframe
        """

        if isinstance(self.filenames, list):
            # convert to list of tuple (path,query,load_data)
            self.filenames = list(zip(self.filenames,itertools.repeat(self.query),itertools.repeat(self.load_data)))
            
            df = parallel_get_dataframe_from_file(self.filenames, get_dataframe_from_file, n_cores=min(mp.cpu_count(),len(self.filenames)))

        else:
            df = get_dataframe_from_file(self.filenames, self.query, self.load_data)

        df = add_data_column(df)

        df = add_shape_column(df)
    
        df = add_columns(df,decode=self.decode_metadata)

        df = drop_duplicates(df)

        return df

def load_data(df:pd.DataFrame,clean:bool=False) -> pd.DataFrame:
    """Gets the associated data for every record in a dataframe

    :param df: dataframe to add arrays to
    :type df: pd.DataFrame
    :param clean: mark loaded data for removal by unload
    :type clean: bool
    :param sort: sort data while loading
    :type sort: bool
    :return: dataframe with filled arrays
    :rtype: pd.DataFrame
    """
    if df.empty:
        return df
    # add the default flag
    if clean:
        df.loc[:,'clean'] = False

    df_list = []
    no_path_df = df.loc[df.path.isna()]

    path_groups = df.groupby(df.path)
    for _,path_df in path_groups:
        if ('file_modification_time' in path_df.columns) and ((path_df.iloc[0]['file_modification_time'] is None) == False):
            compare_modification_times(path_df.iloc[0]['file_modification_time'], path_df.iloc[0]['path'],rmn.FST_RO, 'std_reader.py::load_data',StandardFileError)

        unit=rmn.fstopenall(path_df.iloc[0]['path'],rmn.FST_RO)
        # loads faster when keys are in sequence
        # if sort:
            # path_df = path_df.sort_values(by=['key'])

        for i in path_df.index:
            # if isinstance(path_df.at[i,'d'],dask_array_type):
            #     continue
            if isinstance(path_df.at[i,'d'],np.ndarray):
                continue
            #call stored function with param, rmn.fstluk(key)
            path_df.at[i,'d'] = read_record(path_df.at[i,'key'])
            path_df.at[i,'clean'] = True if clean else False
            #path_df.at[i,'fstinl_params'] = None
            # path_df.at[i,'path'] = None
        df_list.append(path_df)
        rmn.fstcloseall(unit)


    if len(df_list):
        if not no_path_df.empty:
            df_list.append(no_path_df)
        res_df = pd.concat(df_list,ignore_index=True)
    else:
        res_df = df

    # if sort:
    #     res_df = sort_dataframe(res_df)
    # print('load_data\n',res_df[['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1']])
    return res_df

def unload_data(df:pd.DataFrame,only_marked:bool=False) -> pd.DataFrame:
    """Removes the loaded data for every record in a dataframe if it can be loaded from file

    :param df: dataframe to remove data from
    :type df: pd.DataFrame
    :param only_marked: unloads only marked o rows with clean column at True
    :type only_marked: bool
    :return: dataframe with arrays removed
    :rtype: pd.DataFrame
    """

    for i in df.index:
        if isinstance(df.at[i,'d'],np.ndarray) and not(df.at[i,'key'] is None) and ( df.at[i,'clean'] if only_marked else True):
            # df.at[i,'d'] = ('numpy',int(df.at[i,'key']))
            df.at[i,'d'] = None

    df = df.drop(columns=['clean'],errors='ignore')
    return df


# def stack_arrays(df):

#     df = load_data(df)
#     df_list = []
#     grid_groups = df.groupby(df.grid)
#     for _, grid_df in grid_groups:
#         for nomvar in grid_df.nomvar.unique():
#             if nomvar in ['>>','^^','!!','!!SF','HY','P0','PT']:
#                 to_stack_df = grid_df.query('nomvar=="%s"'%nomvar)
#                 df_list.append(to_stack_df)
#                 continue
#             to_stack_df = grid_df.query('nomvar=="%s"'%nomvar)
#             if len(to_stack_df.d) == 1:
#                 df_list.append(to_stack_df)
#                 continue
#             if 'level' not in  to_stack_df.columns:
#                 to_stack_df['level'] = None
#                 to_stack_df['ip1_kind'] = None
#                 for i in to_stack_df.index:
#                     to_stack_df.at[i,'level'], to_stack_df.at[i,'ip1_kind'], _ = decode_ip(to_stack_df.at[i,'ip1'])
#             to_stack_df.sort_values(by=['level'],ascending=False,inplace=True)
#             #print('stacking %s with %s levels'%(nomvar, len(to_stack_df.level)))
#             var = create_1row_df_from_model(to_stack_df)
#             #print(var.index)
#             data_list = to_stack_df['d'].to_list()
#             stacked_data = np.stack(data_list)
#             #print(type(stacked_data),type(var.at[0,'d']))
#             var['stacked'] = True
#             var['ip1'] = None
#             var['level'] = None
#             var['ip1_kind'] = None
#             var['key'] = None
#             var.at[0,'level'] = to_stack_df['level'].to_numpy()
#             var.at[0,'ip1_kind'] = to_stack_df['ip1_kind'].to_numpy()
#             var.at[0,'d'] = stacked_data
#             var.at[0,'ip1'] = to_stack_df['ip1'].to_numpy()
#             var.at[0,'key'] = to_stack_df['key'].to_numpy()
#             var.at[0,'shape'] = (len(var.at[0,'ip1']),var.at[0,'ni'],var.at[0,'nj'])
#             var.at[0,'nk'] = len(var.at[0,'ip1'])
#             df_list.append(var)

#     if len(df_list) > 1:
#         new_df = pd.concat(df_list)
#     else:
#         new_df = df_list[0]
#     new_df = sort_dataframe(new_df)
#     new_df.reset_index(inplace=True,drop=True)
#     return new_df


# def create_coordinate_type(meta:set, ip1_kind:int, vcode:int, coord_types:pd.DataFrame) ->str:
#     vctype = 'UNKNOWN'
#     toctoc_exists = '!!' in meta
#     p0_exists = 'P0' in meta
#     hy_exists = 'HY' in meta
#     pt_exists = 'PT' in meta
#     e1_exists = 'E1' in meta
#     sf_exists = '!!SF' in meta

#     result = coord_types.query(f'(ip1_kind == {ip1_kind}) and (toctoc == {toctoc_exists}) and (P0 == {p0_exists}) and (E1 == {e1_exists}) and (PT == {pt_exists}) and (HY == {hy_exists}) and (SF == {sf_exists}) and (vcode == {vcode})')

#     if len(result.index) > 1:
#         logger.debug(result)
#     if len(result.index):
#         vctype = result['vctype'].iloc[0]
#     return vctype



#e_rel_max   E-REL-MOY   VAR-A        C-COR        MOY-A        BIAIS       E-MAX       E-MOY

# def add_metadata_fields(df:pd.DataFrame, latitude_and_longitude=True, pressure=True, vertical_descriptors=True) -> pd.DataFrame:
#     if 'path' not in df:
#         logger.warning('add_metadata_fields - no path to get meta from')
#         return df

#     meta_df = get_grid_metadata_fields(df, 'add_metadata_fields',StandardFileError,latitude_and_longitude, pressure, vertical_descriptors)

#     res = pd.concat([df,meta_df])

#     return res

# def get_vertical_grid_descriptor(self, records:list):
#     """ Gets the vertical grid descriptor associated with the supplied records"""
#     logger.info('get_vertical_grid_descriptor')
#     if records == None or not len(records):
#         logger.error('get_vertical_grid_descriptor - no records to process')
#         return
# #    ip1      (int)  : Ip1 of the vgrid record to find, use -1 for any (I)
# #    ip2      (int)  : Ip2 of the vgrid record to find, use -1 for any (I)
# #    ip1_kind     (int)  : Kind of vertical coor
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
#         logger.debug("CB14: Found vgrid type=%s (ip1_kind=%d, vers=%d) with %d levels and diag levels=%7.2f%s (ip1=%d) and %7.2f%s (ip1=%d)" % (vtype, vkind, vver, len(tlvl), ldiagval, rmn.kindToString(ldiagkind), ip1diagt, l2diagval, rmn.kindToString(l2diagkind), ip1diagm))
