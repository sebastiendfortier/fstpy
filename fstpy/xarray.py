# # -*- coding: utf-8 -*-
# import numpy as np
# import pandas as pd

# # import xarray as xr


# def set_data_array_attributes(attribs:dict, nomvar_df:pd.DataFrame) -> dict:
#     """[summary]

#     :param attribs: dictionnary of attribute to attach to data arrays
#     :type attribs: dict
#     :param nomvar_df: dataframe organized by nomvar
#     :type nomvar_df: pd.DataFrame
#     :return: filled dict of atributes
#     :rtype: dict
#     """

#     attribs = nomvar_df.iloc[-1].to_dict()
#     attribs = remove_keys(attribs,['key','nomvar','etiket','ni','nj','nk','shape','ig1','ig2','ig3','ig4','ip1','ip2','ip3','datyp','dateo','pkind','datev','grid','d','file_modification_time'])
#     for k,v in attribs.items():
#         attribs = set_attrib(nomvar_df,attribs,k)
#     # attribs = set_attrib(nomvar_df,attribs,'ip1_kind')
#     # attribs = set_attrib(nomvar_df,attribs,'surface')
#     # attribs = set_attrib(nomvar_df,attribs,'date_of_observation')
#     # attribs = set_attrib(nomvar_df,attribs,'path')
#     # attribs = set_attrib(nomvar_df,attribs,'ensemble_member')
#     # attribs = set_attrib(nomvar_df,attribs,'implementation')
#     # attribs = set_attrib(nomvar_df,attribs,'run')
#     # attribs = set_attrib(nomvar_df,attribs,'label')
#     #if not timeseries:
#     #    attribs = set_attrib(nomvar_df,attribs,'date_of_validity')

#     #attribs = remove_keys(nomvar_df,attribs,['ni','nj','nk','shape','ig1','ig2','ig3','ig4','ip1','ip2','ip3','datyp','dateo','pkind','datev','grid','fstinl_params','d','file_modification_time','ensemble_member','implementation','run','label'])
    
#     return attribs


# def remove_keys(a_dict,keys):
#     for k in keys:
#         a_dict.pop(k,None)
#     return a_dict    

# def set_attrib(nomvar_df,attribs,key):
#     attribs[key] = np.array(getattr(nomvar_df,key).to_list()) if len(getattr(nomvar_df,key).unique()) > 1 else attribs[key]
#     return attribs

# def get_date_of_validity_data_array(df,date_of_validity_name):
#     times = df['date_of_validity'].to_numpy()
#     time = xr.DataArray(
#         times,
#         dims=[date_of_validity_name],
#         coords=dict(time = times),
#         name = date_of_validity_name
#         )
#     return time

# def get_level_data_array(df,level_name):
#     levels = df['level'].to_numpy(dtype='float32')
#     level = xr.DataArray(
#         levels,
#         dims=[level_name],
#         coords={level_name:levels},
#         name = level_name
#         )
#     return level

# def get_latitude_data_array(lat_lon_df,lat_name):
#     lati = lat_lon_df.query('nomvar=="^^"').iloc[0]['d'].flatten()
#     attribs = {
#         'long_name' : 'latitude in rotated pole grid',
#         'standard_name' : 'grid_latitude',
#         'units' : 'degrees',
#         'axis' : 'Y',
#     }
#     lat = xr.DataArray(
#         lati,
#         dims=[lat_name],
#         coords={lat_name:lati},
#         name=lat_name,
#         attrs=attribs
#         )
#     return lat
    
# def get_longitude_data_array(lat_lon_df,lon_name):
#     loni = np.flip(lat_lon_df.query('nomvar==">>"').iloc[0]['d'].flatten())
#     loni = (loni-163.41278)*-1
#     attribs = {
#         'long_name' : 'longitude in rotated pole grid',
#         'standard_name' : 'grid_longitude',
#         'units' : 'degrees',
#         'axis' : 'X'
#     }
#     lon = xr.DataArray(
#         loni,
#         dims=[lon_name],
#         coords={lon_name:loni},
#         name=lon_name,
#         attrs=attribs
#         )
#     return lon

# def get_variable_data_array(df, name, attribs, dim, dim_name, latitudes, lat_name, longitudes, lon_name,timeseries=False):
#     datyps = {0:'float32',
#         1:'float64',
#         2:'int32',
#         4:'int32',
#         5:'float64',
#         6:'float32',
#         8:'float64',
#         130:'int32',
#         132:'int32',
#         133:'float32',
#         134:'float32'}
#     #datyp = int(df.iloc[0]['datyp'])
#     #df.sort_values(by='level',inplace=True)
#     for i in df.index:
#         df.at[i,'d'] = df.at[i,'d'].transpose()
#     values = np.stack(df['d'].to_list())
#     #rmn.dtype_fst2numpy(datyp)
    
#     #print(type(type_str))
#     if not timeseries:
#         dimensions = [dim_name,lat_name,lon_name]
#         coordinates = {dim_name:dim,lat_name:latitudes,lon_name:longitudes}
#     else:
#         dimensions = [dim_name,lat_name,lon_name]
#         coordinates = {dim_name:dim,lat_name:latitudes,lon_name:longitudes}

#     arr_da = xr.DataArray(
#         values.astype(datyps[df.iloc[0]['datyp']]),
#         dims=dimensions,
#         coords=coordinates,
#         name=name,
#         attrs=attribs
#         )    
#     return arr_da        

   

