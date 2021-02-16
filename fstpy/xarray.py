# -*- coding: utf-8 -*-
from numpy.lib.function_base import disp
import rpnpy.librmn.all as rmn
import xarray as xr
import numpy as np

def to_xarray(self):
    df = self.to_pandas()
    from .std_reader import get_lat_lon
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
    return disp

def remove_keys(nomvar_df,a_dict,keys):
    for k in keys:
        del a_dict[k]
    return a_dict    

def set_attrib(nomvar_df,attribs,key):
    attribs[key] = np.array(getattr(nomvar_df,key).to_list()) if len(getattr(nomvar_df,key).unique()) > 1 else attribs[key]
    return attribs

def get_level_data_array(df):
    levels = df['level'].to_numpy(dtype='float32')
    level = xr.DataArray(
        levels,
        dims=['level'],
        coords=dict(level = levels),
        name = 'level'
        )
    return level

def get_latitude_data_array(lat_lon_df):
    lati = lat_lon_df.query('nomvar=="^^"').iloc[0]['d'].flatten()
    lat = xr.DataArray(
        lati,
        dims=['lat'],
        coords=dict(lat = lati),
        name='lat'
        )
    return lat
    
def get_longitude_data_array(lat_lon_df):
    loni = lat_lon_df.query('nomvar==">>"').iloc[0]['d'].flatten()
    lon = xr.DataArray(
        loni,
        dims=['lon'],
        coords=dict(lon = loni),
        name='lon'
        )
    return lon

def get_variable_data_array(df, name, attribs, levels, latitudes, longitudes):
    datyps = {0:'float32',
        1:'float64',
        2:'int32',
        4:'int32',
        5:'float64',
        6:'float32',
        8:'float64',
        130:'int32',
        132:'int32',
        133:'float32',
        134:'float32'}
    #datyp = int(df.iloc[0]['datyp'])
    values = np.stack(df['d'].to_list())
    #rmn.dtype_fst2numpy(datyp)
    
    #print(type(type_str))
    arr_da = xr.DataArray(
        values.astype(datyps[df.iloc[0]['datyp']]),
        dims=['level','lon','lat'],
        coords=dict(
            level = levels,
            lon = longitudes,
            lat = latitudes,
        ),
        name=name,
        attrs=attribs
        )    
    return arr_da        

