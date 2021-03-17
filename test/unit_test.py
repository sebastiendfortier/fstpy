# -*- coding: utf-8 -*-
import pytest
import fstpy.unit as fstuc
import fstpy.constants as fstconst
import fstpy.exceptions as fstexc
import pandas as pd
import numpy as np
import math

pytestmark = [pytest.mark.unit, pytest.mark.unit_tests]


@pytest.fixture
def base_dataframe():
    arr = np.array([-5.0])
    d = [
        {'nomvar':'TT','unit':'celsius','unit_converted':False,'d':arr},
        {'nomvar':'TT','unit':'fahrenheit','unit_converted':False,'d':arr},
        {'nomvar':'TT','unit':'rankine','unit_converted':False,'d':arr},
        ]
    df = pd.DataFrame(d)
    return df


def test_base_conversion(base_dataframe):
    res_df = fstuc.do_unit_conversion(base_dataframe,'kelvin')
    assert res_df['unit'].all() == 'kelvin'
    assert res_df['unit_converted'].all() == True
    assert math.isclose(res_df['d'][0],np.array([268.15]))
    assert math.isclose(res_df['d'][1],np.array([250.3722222222]))
    assert math.isclose(res_df['d'][2],np.array([-2.77777778]))


def test_no_data(base_dataframe):
    base_dataframe['d'] = None
    with pytest.raises(fstexc.UnitConversionError):
        res_df = fstuc.do_unit_conversion(base_dataframe,'kelvin')


def test_wrong_unit_type(base_dataframe):
    with pytest.raises(TypeError):
        res_df = fstuc.do_unit_conversion(base_dataframe,'meter')    

@pytest.fixture
def kelvin():
    return fstconst.get_unit_by_name('kelvin')

@pytest.fixture
def celsius():    
        return fstconst.get_unit_by_name('celsius')

@pytest.fixture
def fahrenheit():    
    return fstconst.get_unit_by_name('fahrenheit')

@pytest.fixture
def rankine():    
    return fstconst.get_unit_by_name('rankine')

@pytest.fixture
def array_to_convert():    
    a = np.arange(1,11)
    a = a.reshape(2,5)
    return a

def convert_kelvin_to_celsius(array_to_convert,kelvin,celsius):
    convert = fstuc.get_converter(kelvin, celsius)
    

def convert_kelvin_to_fahrenheit(array_to_convert,kelvin,fahrenheit):    
    convert = fstuc.get_converter(kelvin, fahrenheit)

def convert_kelvin_to_rankine(array_to_convert,kelvin,rankine):    
    convert = fstuc.get_converter(kelvin, rankine)

def convert_celsius_to_kelvin(celsius,kelvin):
    convert = fstuc.get_converter(celsius, kelvin)

def convert_celsius_to_fahrenheit(array_to_convert,celsius,fahrenheit):    
    convert = fstuc.get_converter(celsius, fahrenheit)

def convert_celsius_to_rankine(array_to_convert,celsius,rankine):    
    convert = fstuc.get_converter(celsius, rankine)

def convert_rankine_to_kelvin(array_to_convert,rankine,kelvin):
    convert = fstuc.get_converter(rankine, kelvin)

def convert_rankine_to_fahrenheit(array_to_convert,rankine,fahrenheit):    
    convert = fstuc.get_converter(rankine, fahrenheit)

def convert_rankine_to_celsius(array_to_convert,rankine,celsius):    
    convert = fstuc.get_converter(rankine, celsius)
    converted_array = np.array([[-272.15,-271.15,-270.15,-269.15,-268.15],[-267.15,-266.15,-265.15,-264.15,-263.15]])
    converted_array = np.array([[-949.54,-947.74,-945.94,-944.14,-942.34],[-940.54,-938.74,-936.94,-935.14,-933.34]])
    converted_array = np.array([[-1709.172,-1705.932,-1702.692,-1699.452,-1696.212],[-1692.972,-1689.732,-1686.492,-1683.252,-1680.012]])
    converted_array = np.array([[-1436.022,-1432.782,-1429.542,-1426.302,-1423.062],[-1419.822,-1416.582,-1413.342,-1410.102,-1406.862]])
    converted_array = np.array([[-1162.872,-1159.632,-1156.392,-1153.152,-1149.912],[-1146.672,-1143.432,-1140.192,-1136.952,-1133.712]])
    converted_array = np.array([[-889.722,-886.482,-883.242,-880.002,-876.762],[-873.522,-870.282,-867.042,-863.802,-860.562]])
    converted_array = np.array([[-494.29,-492.49,-490.69,-488.89,-487.09],[-485.29,-483.49,-481.69,-479.89,-478.09]])
    converted_array = np.array([[-274.60555556,-273.60555556,-272.60555556,-271.60555556,-270.60555556],[-269.60555556,-268.60555556,-267.60555556,-266.60555556,-265.60555556]])
    converted_array = np.array([[-152.55864198,-152.00308642,-151.44753086,-150.89197531,-150.33641975],[-149.7808642,-149.22530864,-148.66975309,-148.11419753,-147.55864198]])
    
