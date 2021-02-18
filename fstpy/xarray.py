# -*- coding: utf-8 -*-
from os import error
import xarray as xr
import numpy as np



def remove_keys(a_dict,keys):
    for k in keys:
        a_dict.pop(k,None)
    return a_dict    

def set_attrib(nomvar_df,attribs,key):
    attribs[key] = np.array(getattr(nomvar_df,key).to_list()) if len(getattr(nomvar_df,key).unique()) > 1 else attribs[key]
    return attribs

def get_date_of_validity_data_array(df):
    times = df['date_of_validity'].to_numpy()
    time = xr.DataArray(
        times,
        dims=['date_of_validity'],
        coords=dict(time = times),
        name = 'date_of_validity'
        )
    return time

def get_level_data_array(df):
    levels = np.flip(df['level'].to_numpy(dtype='float32'))
    level = xr.DataArray(
        levels,
        dims=['level'],
        coords=dict(level = levels),
        name = 'level'
        )
    return level

def get_latitude_data_array(lat_lon_df):
    lati = lat_lon_df.query('nomvar=="^^"').iloc[0]['d'].flatten()
    attribs = {
        'long_name' : 'latitude in rotated pole grid',
        'standard_name' : 'grid_latitude',
        'units' : 'degrees',
        'axis' : 'Y',
    }
    lat = xr.DataArray(
        lati,
        dims=['lat'],
        coords=dict(lat = lati),
        name='lat',
        attrs=attribs
        )
    return lat
    
def get_longitude_data_array(lat_lon_df):
    loni = np.flip(lat_lon_df.query('nomvar==">>"').iloc[0]['d'].flatten())
    loni = (loni-163.41278)*-1
    attribs = {
        'long_name' : 'longitude in rotated pole grid',
        'standard_name' : 'grid_longitude',
        'units' : 'degrees',
        'axis' : 'X'
    }
    lon = xr.DataArray(
        loni,
        dims=['lon'],
        coords=dict(lon = loni),
        name='lon',
        attrs=attribs
        )
    return lon

def get_variable_data_array(df, name, attribs, dim, latitudes, longitudes,timeseries=False):
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
    df.sort_values(by='level',inplace=True)
    for i in df.index:
        df.at[i,'d'] = df.at[i,'d'].transpose()
    values = np.stack(df['d'].to_list())
    #rmn.dtype_fst2numpy(datyp)
    
    #print(type(type_str))
    if not timeseries:
        dimensions = ['level','lat','lon']
        coordinates = {'level':dim,'lat':latitudes,'lon':longitudes}
    else:
        dimensions = ['time','lat','lon']
        coordinates = {'time':dim,'lat':latitudes,'lon':longitudes}

    arr_da = xr.DataArray(
        values.astype(datyps[df.iloc[0]['datyp']]),
        dims=dimensions,
        coords=coordinates,
        name=name,
        attrs=attribs
        )    
    return arr_da        

   

