#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import fstpy
import xarray as xr 
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import metpy.calc as mpcalc


def main():
    vv = get_dataframe_sorted_by_date()

    lat_lon_df = vv.query('nomvar in [">>","^^"]')

    lat = get_latitude_data_array(lat_lon_df)    
    lon = get_longitude_data_array(lat_lon_df)

    time, times = get_time_data_array(vv)
        
    vwnd = get_vwnd_data_array(vv, time, lat, lon)    

    data = get_vwnd_subset(times, vwnd)

    # Compute weights and take weighted average over latitude dimension
    weights = np.cos(np.deg2rad(data.lat.values))

    avg_data = (data * weights[None, None, : ]).sum(dim='lat') / np.sum(weights)

    # Get times and make array of datetime objects
    vtimes = data.time.values.astype('datetime64[ms]').astype('O')

    # Specify longitude values for chosen domain
    lons = data.lon.values

    # Start figure
    create_plot(lons, vtimes, avg_data)

    plt.savefig('toto.png')

def create_plot(lons, vtimes, avg_data):
    # Start figure
    fig = plt.figure(figsize=(10, 13))

    # Use gridspec to help size elements of plot; small top plot and big bottom plot
    gs = gridspec.GridSpec(nrows=2, ncols=1, height_ratios=[1, 6], hspace=0.03)

    # Tick labels
    x_tick_labels = [u'0\N{DEGREE SIGN}E', u'90\N{DEGREE SIGN}E',
                    u'180\N{DEGREE SIGN}E', u'90\N{DEGREE SIGN}W',
                    u'0\N{DEGREE SIGN}E']

    # Top plot for geographic reference (makes small map)
    ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree(central_longitude=180))
    ax1.set_extent([0, 357.5, 35, 65], ccrs.PlateCarree(central_longitude=180))
    ax1.set_yticks([40, 60])
    ax1.set_yticklabels([u'40\N{DEGREE SIGN}N', u'60\N{DEGREE SIGN}N'])
    ax1.set_xticks([-180, -90, 0, 90, 180])
    ax1.set_xticklabels(x_tick_labels)
    ax1.grid(linestyle='dotted', linewidth=2)

    # Add geopolitical boundaries for map reference
    ax1.add_feature(cfeature.COASTLINE.with_scale('50m'))
    ax1.add_feature(cfeature.LAKES.with_scale('50m'), color='black', linewidths=0.5)

    # Set some titles
    plt.title('Hovmoller Diagram', loc='left')
    plt.title('CMC', loc='right')

    # Bottom plot for Hovmoller diagram
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.invert_yaxis()  # Reverse the time order to do oldest first

    # Plot of chosen variable averaged over latitude and slightly smoothed
    clevs = np.arange(-50, 51, 5)
    cf = ax2.contourf(lons, vtimes, mpcalc.smooth_n_point(
        avg_data, 9, 2), clevs, cmap=plt.cm.bwr, extend='both')
    cs = ax2.contour(lons, vtimes, mpcalc.smooth_n_point(
        avg_data, 9, 2), clevs, colors='k', linewidths=1)
    cbar = plt.colorbar(cf, orientation='horizontal', pad=0.04, aspect=50, extendrect=True)
    cbar.set_label('m $s^{-1}$')

    # Make some ticks and tick labels
    ax2.set_xticks([0, 90, 180, 270, 357.5])
    ax2.set_xticklabels(x_tick_labels)
    ax2.set_yticks(vtimes[4::8])
    ax2.set_yticklabels(vtimes[4::8])

    # Set some titles
    plt.title('250-hPa V-wind', loc='left', fontsize=10)
    plt.title('Time Range: {0:%Y%m%d %HZ} - {1:%Y%m%d %HZ}'.format(vtimes[0], vtimes[-1]),
            loc='right', fontsize=10)


def get_vwnd_subset(times, vwnd):
    time_slice = slice(np.min(times), np.max(times))
    lat_slice = slice(40, 60)
    lon_slice = slice(0, 360)

    # Get data, selecting time, level, lat/lon slice
    data = vwnd.sel(time=time_slice,
                        lat=lat_slice,
                        lon=lon_slice)
    return data

def get_vwnd_data_array(vv, time, lat, lon):

    vv_values = get_vv_values(vv)

    vwnd_attribs = {
            'long_name' :'V wind',
            'units' :'knot',
            'precision' :2,
            'GRIB_id' :34,
            'GRIB_name' :'VGRD',
            'var_desc' :'v-wind',
            'level_desc' :'Multiple levels',
            'statistic' :'Individual Obs',
            'parent_stat' :'Other',
            'actual_range' :[np.min(vv_values),np.max(vv_values)],
            'valid_range' :[-125.,160.],
            'dataset' :'CMC Reanalysis'
            }

    vwnd = xr.DataArray(
        vv_values.astype('float32'),
        dims=['time','lon','lat'],
        coords=dict(
            time = time,
            lon = lon,
            lat = lat,
        ),
        name='vwnd',
        attrs=vwnd_attribs
        )    
    return vwnd

def get_latitude_data_array(lat_lon_df):
    lati = lat_lon_df.query('nomvar=="^^"').iloc[0]['d']
    lati = lati.flatten()
    lat_arrtibs = {
            'units' :'degrees_north',
            'actual_range' :[ np.max(lati),np.min(lati)],
            'long_name' :'Latitude',
            'standard_name' :'latitude',
            'axis' :'Y',
            }

    lat = xr.DataArray(
        lati,
        dims=['lat'],
        coords=dict(lat = lati),
        name='lat',
        attrs=lat_arrtibs
        )
    return lat

def get_longitude_data_array(lat_lon_df):
    loni = lat_lon_df.query('nomvar==">>"').iloc[0]['d']
    loni = loni.flatten()
    lon_arrtibs = {
            'units' :'degrees_east',
            'actual_range' :[ np.min(loni),np.max(loni)],    
            'long_name' :'Longitude',
            'standard_name' :'longitude',
            'axis' :'X'
            }

    lon = xr.DataArray(
        loni,
        dims=['lon'],
        coords=dict(lon = loni),
        name = 'lon',
        attrs=lon_arrtibs
        )
    return lon

def get_vv_values(vv):
    df = vv.query('nomvar=="VV"')
    vv_values = np.stack(df['d'].to_list())
    return vv_values


def get_time_data_array(vv):
    df = vv.query('nomvar=="VV"')
    times = np.array(df['date_of_observation'].to_list())
    delta_t = np.max(times)- np.min(times)
    time_arrtibs = {
            'long_name' :'Time',
            'delta_t' :delta_t,
            'standard_name' :'time',
            'axis' :'T',
            'actual_range' :[np.min(times).timestamp(), np.max(times).timestamp()]
            }

    time = xr.DataArray(
        times,
        dims=['time'],
        coords=dict(time = times),
        name='time',
        attrs=time_arrtibs
        )
    return time, times



def get_dataframe_sorted_by_date():
    df = fstpy.StandardFileReader('/fs/homeu1/eccc/cmd/cmda/pbu000/hovmuller/p_levels/reg/VV_250hPa_RU_0.3d',decode_metadata=True,query='nomvar=="VV"').to_pandas()

    vv = df.copy(deep=True)

    vv = vv.sort_values(by=['date_of_observation'])
    
    return vv

if __name__ == "__main__":
    main()
    