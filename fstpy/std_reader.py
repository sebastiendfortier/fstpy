# -*- coding: utf-8 -*-
from fstpy.dataframe import sort_dataframe
from .utils import create_1row_df_from_model, initializer
import rpnpy.librmn.all as rmn
import pandas as pd
import numpy as np
import sys
import xarray as xr

class StandardFileReader:
    """Class to handle fst files   
        Opens, reads the contents of an fst files or files into a pandas Dataframe and closes   
        No data is loaded unless specified, only the metadata is read. Extra metadata is added to the dataframe if specified.
 
        :param filenames: path to file or list of paths to files   
        :type filenames: str|list[str]   
        :param decode_metadata: adds extra columns, defaults to True    
                'unit':str, unit name
                'unit_converted':bool
                'description':str, field description
                'date_of_observation':datetime, of the date of observation
                'date_of_validity':datetime, of the date of validity
                'level':float32, decoded ip1 level
                'ip1_kind':int32, decoded ip1 kind
                'pkind':str, string repr of ip1_kind int
                'data_type_str':str, string repr of data type
                'label':str, label derived from etiket
                'run':str, run derived from etiket
                'implementation':str, implementation derived from etiket
                'ensemble_member':str, ensemble member derived from etiket
                'surface':bool, True if the level is a surface level
                'follow_topography':bool, indicates if this type of level follows topography
                'vctype':str, vertical level type
                'forecast_hour':timedelta, forecast hour decoded from ip2
                'ip2_dec':value of decoded ip2
                'ip2_kind':kind of decoded ip2
                'ip2_pkind':printable kind of decoded ip2
                'ip3_dec':value of decoded ip3
                'ip3_kind':kind of decoded ip3
                'ip3_pkind':printable kind of decoded ip3
        :type decode_metadata: bool, optional    
        :param load_data: if True, the data will be read, not just the metadata (fstluk vs fstprm)
        :type load_data: bool, optional
        :param subset: parameter to pass to fstinl to select specific records
        :type subset:dict
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]
    @initializer
    def __init__(self, filenames, decode_metadata=False,load_data=False,subset=None,array_container='numpy',stack=False):
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
                df = create_dataframe(file,self.decode_metadata,self.load_data,self.subset,self.array_container)
                dfs.append(df)
            df = pd.concat(dfs)   
            # return df
        else:
            df = create_dataframe(self.filenames,self.decode_metadata,self.load_data,self.subset,self.array_container)
            # return df
        return df    

    def to_xarray(self, timeseries=False, attributes=False):
        import dask
        dask.config.set(**{'array.slicing.split_large_chunks': True})
        from .xarray import get_variable_data_array,get_longitude_data_array,get_date_of_validity_data_array,get_latitude_data_array,get_level_data_array
        from .std_io import get_lat_lon
        self.array_container = 'dask.array'
        self.decode_metadata = True
        df = self.to_pandas()
        df = load_data(df)
        counter = 0
        data_list = []
        grid_groups = df.groupby(df.grid)
        
        for _,grid_df in grid_groups:
            counter += 1
            if len(grid_groups.size()) > 1:
                lat_name = 'rlat%s'%counter
                lon_name = 'rlon%s'%counter
                datev_name = 'time%s'%counter
                level_name = 'level%s'%counter
            else:
                lat_name = 'rlat'
                lon_name = 'rlon'
                datev_name = 'time'
                level_name = 'level'    
            lat_lon_df = get_lat_lon(grid_df)
            longitudes = get_longitude_data_array(lat_lon_df,lon_name)
            #print(longitudes.shape)
            latitudes = get_latitude_data_array(lat_lon_df,lat_name)
            #print(latitudes.shape)
            nomvar_groups = grid_df.groupby(grid_df.nomvar)
            for _,nomvar_df in nomvar_groups:
                nomvar_df.sort_values(by='level',inplace=True)
                if nomvar_df.iloc[0]['nomvar'] in ['!!','>>','^^','^>','HY']:
                    continue
                if len(nomvar_df.datev.unique()) > 1 and timeseries:
                    time_dim = get_date_of_validity_data_array(nomvar_df,datev_name)
                    nomvar_df.sort_values(by=['date_of_validity'],ascending=True,inplace=True)
                else: #nomvar_df.ip1.unique() > 1:
                    level_dim = get_level_data_array(nomvar_df,level_name)
                    nomvar_df.sort_values(by=['level'],ascending=False,inplace=True)
                attribs = {}
                if attributes:
                    attribs = set_data_array_attributes(attribs,nomvar_df, timeseries)
                
                nomvar = nomvar_df.iloc[-1]['nomvar']
                if timeseries:
                    data_list.append(get_variable_data_array(nomvar_df, nomvar, attribs, time_dim, datev_name, latitudes, lat_name, longitudes, lon_name, timeseries=True))    
                else:    
                    data_list.append(get_variable_data_array(nomvar_df, nomvar, attribs, level_dim, level_name, latitudes, lat_name, longitudes, lon_name, timeseries=False))    

        d = {}   
        for variable in data_list:
                d.update({variable.name:variable})

        ds = xr.Dataset(d)

        return ds

def set_data_array_attributes(attribs, nomvar_df, timeseries):
    from .xarray import set_attrib,remove_keys
    attribs = nomvar_df.iloc[-1].to_dict()
    attribs = remove_keys(attribs,['key','nomvar','etiket','ni','nj','nk','shape','ig1','ig2','ig3','ig4','ip1','ip2','ip3','datyp','dateo','pkind','datev','grid','fstinl_params','d','file_modification_time'])
    for k,v in attribs.items():
        attribs = set_attrib(nomvar_df,attribs,k)
    # attribs = set_attrib(nomvar_df,attribs,'ip1_kind')
    # attribs = set_attrib(nomvar_df,attribs,'surface')
    # attribs = set_attrib(nomvar_df,attribs,'date_of_observation')
    # attribs = set_attrib(nomvar_df,attribs,'path')
    # attribs = set_attrib(nomvar_df,attribs,'ensemble_member')
    # attribs = set_attrib(nomvar_df,attribs,'implementation')
    # attribs = set_attrib(nomvar_df,attribs,'run')
    # attribs = set_attrib(nomvar_df,attribs,'label')
    #if not timeseries:
    #    attribs = set_attrib(nomvar_df,attribs,'date_of_validity')

    #attribs = remove_keys(nomvar_df,attribs,['ni','nj','nk','shape','ig1','ig2','ig3','ig4','ip1','ip2','ip3','datyp','dateo','pkind','datev','grid','fstinl_params','d','file_modification_time','ensemble_member','implementation','run','label'])
    
    return attribs


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
                to_stack_df['ip1_kind'] = None
                for i in to_stack_df.index:
                    to_stack_df.at[i,'level'], to_stack_df.at[i,'ip1_kind'], _ = decode_ip1(to_stack_df.at[i,'ip1'])
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
            var['ip1_kind'] = None
            var['key'] = None
            var.at[0,'level'] = to_stack_df['level'].to_numpy()
            var.at[0,'ip1_kind'] = to_stack_df['ip1_kind'].to_numpy()
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


 
def load_data(df):
    import dask.array as da
    from .dataframe import sort_dataframe
    path_groups = df.groupby(df.path)
    res_list = []
    for _,path_df in path_groups:
        unit=rmn.fstopenall(path_df.iloc[0]['path'],rmn.FST_RO)
        path_df.sort_values(by=['key'],inplace=True)
        for i in path_df.index:
            if isinstance(path_df.at[i,'d'],da.core.Array):
                continue
            if isinstance(path_df.at[i,'d'],np.ndarray):
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







