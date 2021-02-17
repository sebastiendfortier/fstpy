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
                'ip2_dec':value of decoded ip2
                'ip2_kind':kind of decoded ip2
                'ip2_pkind':printable kind of decoded ip2
                'ip3_dec':value of decoded ip3
                'ip3_kind':kind of decoded ip3
                'ip3_pkind':printable kind of decoded ip3
        :type decode_meta_data: bool, optional    
        :param load_data: if True, the data will be read, not just the meta data (fstluk vs fstprm)
        :type load_data: bool, optional
        :param subset: parameter to pass to fstinl to select specific records
        :type subset:dict
    """
    meta_data = ["^>", ">>", "^^", "!!", "!!SF", "HY", "P0", "PT", "E1"]
    @initializer
    def __init__(self, filenames, decode_meta_data=False,load_data=False,subset=None,array_container='numpy',stack=False):
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

    def to_xarray(self, timeseries=True):
        from .xarray import get_variable_data_array,get_longitude_data_array,get_latitude_data_array,get_level_data_array,remove_keys,set_attrib
        df = self.to_pandas()
        from .std_io import get_lat_lon
        data_list = []
        grid_groups = df.groupby(df.grid)
        for _,grid_df in grid_groups:
            lat_lon_df = get_lat_lon(grid_df)
            longitudes = get_longitude_data_array(lat_lon_df)
            #print(longitudes.shape)
            latitudes = get_latitude_data_array(lat_lon_df)
            #print(latitudes.shape)
            pdateo_groups = grid_df.groupby(grid_df.pdateo)
            for _,dateo_df in pdateo_groups:
                fhour_groups = dateo_df.groupby(dateo_df.fhour)
                for _,fhour_df in fhour_groups:
                    nomvar_groups = fhour_df.groupby(fhour_df.nomvar)
                    for _,nomvar_df in nomvar_groups:
                        levels = get_level_data_array(nomvar_df)
                        nomvar_df.sort_values(by=['level'],ascending=False,inplace=True)
                        attribs = nomvar_df.iloc[-1].to_dict()
                        attribs = remove_keys(nomvar_df,attribs,['ip1','ip2','pkind','datyp','dateo','datev','grid','fstinl_params','d','path','file_modification_time','ensemble_member','implementation','run','label'])
                        attribs = set_attrib(nomvar_df,attribs,'etiket')
                        attribs = set_attrib(nomvar_df,attribs,'level')
                        attribs = set_attrib(nomvar_df,attribs,'kind')
                        attribs = set_attrib(nomvar_df,attribs,'surface')
                        nomvar = nomvar_df.iloc[-1]['nomvar']
                        data_list.append(get_variable_data_array(nomvar_df, nomvar, attribs, levels, latitudes, longitudes))    

        d = {}                
        for variable in data_list:
            d.update({["level", "lon", "lat"]:variable})

        ds = xr.Dataset(
            data_vars=d,
            coords=dict(
                level = levels,
                lon = longitudes,
                lat = latitudes,
            ),
            attrs=dict(description="Weather related data."),
        )
        return ds

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



















#e_rel_max   E-REL-MOY   VAR-A        C-COR        MOY-A        BIAIS       E-MAX       E-MOY








# def add_metadata_fields(df:pd.DataFrame, latitude_and_longitude=True, pressure=True, vertical_descriptors=True) -> pd.DataFrame:
#     if 'path' not in df:
#         logger.warning('add_metadata_fields - no path to get meta from')
#         return df

#     meta_df = get_meta_data_fields(df, 'add_metadata_fields',StandardFileError,latitude_and_longitude, pressure, vertical_descriptors)

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







