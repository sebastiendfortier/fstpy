# -*- coding: utf-8 -*-
import xarray as xr
import numpy as np



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

