# -*- coding: utf-8 -*-
from fstpy.exceptions import UnitConversionError
import pytest
import pandas as pd
import numpy as np
import math
from fstpy.unit import unit_convert,get_converter
from fstpy import get_unit_by_name

pytestmark = [pytest.mark.unit, pytest.mark.unit_tests]


@pytest.fixture
def base_dataframe():
    arr = np.array([[-5.0]],dtype='float32')
    d = [
        {'nomvar':'TT','unit':'celsius','unit_converted':False,'d':arr, 'path':None,'datev':123456},
        {'nomvar':'TT','unit':'fahrenheit','unit_converted':False,'d':arr, 'path':None,'datev':123456},
        {'nomvar':'TT','unit':'rankine','unit_converted':False,'d':arr, 'path':None,'datev':123456},
        {'nomvar':'TT','unit':'kelvin','unit_converted':False,'d':arr, 'path':None,'datev':123456},
        ]
    df = pd.DataFrame(d)
    return df


def test_kelvin_conversion(base_dataframe):
    res_df = unit_convert(base_dataframe,'kelvin')
    assert res_df['unit'].all() == 'kelvin'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == False
    assert math.isclose(res_df.iloc[0]['d'][0][0],268.15,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],252.594,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-2.77777778,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-5.0,abs_tol=0.001)

def test_celsius_conversion(base_dataframe):
    res_df = unit_convert(base_dataframe,'celsius')
    assert res_df['unit'].all() == 'celsius'
    assert res_df.iloc[0]['unit_converted'] == False
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],-20.5556,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-275.928,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-278.15,abs_tol=0.001)

def test_fahrenheit_conversion(base_dataframe):
    res_df = unit_convert(base_dataframe,'fahrenheit')
    assert res_df['unit'].all() == 'fahrenheit'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == False
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],23,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-464.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-468.67,abs_tol=0.001)

def test_rankine_conversion(base_dataframe):
    res_df = unit_convert(base_dataframe,'rankine')
    assert res_df['unit'].all() == 'rankine'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == False
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],482.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],454.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-9,abs_tol=0.001)

def test_no_data(base_dataframe):
    base_dataframe['d'] = None
    with pytest.raises(TypeError):
        res_df = unit_convert(base_dataframe,'kelvin')


def test_wrong_unit_type(base_dataframe):
    with pytest.raises(UnitConversionError):
        res_df = unit_convert(base_dataframe,'meter')    

@pytest.fixture
def kelvin():
    return get_unit_by_name('kelvin')

@pytest.fixture
def celsius():    
        return get_unit_by_name('celsius')

@pytest.fixture
def fahrenheit():    
    return get_unit_by_name('fahrenheit')

# @pytest.fixture
# def rankine():    
#     return get_unit_by_name('rankine')

@pytest.fixture
def array_to_convert():    
    a = np.arange(1,11).reshape(2,5)
    return a

def convert_kelvin_to_celsius(array_to_convert,kelvin,celsius):
    convert = get_converter(kelvin, celsius)
    converted_array = convert(array_to_convert)
    

def convert_kelvin_to_fahrenheit(array_to_convert,kelvin,fahrenheit):    
    convert = get_converter(kelvin, fahrenheit)
    converted_array = convert(array_to_convert)

# def convert_kelvin_to_rankine(array_to_convert,kelvin,rankine):    
#     convert = get_converter(kelvin, rankine)

def convert_celsius_to_kelvin(array_to_convert,celsius,kelvin):
    convert = get_converter(celsius, kelvin)
    converted_array = convert(array_to_convert)

def convert_celsius_to_fahrenheit(array_to_convert,celsius,fahrenheit):    
    convert = get_converter(celsius, fahrenheit)
    converted_array = convert(array_to_convert)

def convert_fahrenheit_to_kelvin(array_to_convert,fahrenheit,kelvin):
    convert = get_converter(fahrenheit, kelvin)
    converted_array = convert(array_to_convert)

def convert_fahrenheit_to_celsius(array_to_convert,fahrenheit,celsius):    
    convert = get_converter(fahrenheit,celsius)
    converted_array = convert(array_to_convert)

# def convert_celsius_to_rankine(array_to_convert,celsius,rankine):    
#     convert = get_converter(celsius, rankine)

# def convert_rankine_to_kelvin(array_to_convert,rankine,kelvin):
#     convert = get_converter(rankine, kelvin)

# def convert_rankine_to_fahrenheit(array_to_convert,rankine,fahrenheit):    
#     convert = get_converter(rankine, fahrenheit)

# def convert_rankine_to_celsius(array_to_convert,rankine,celsius):    
#     convert = get_converter(rankine, celsius)
#     converted_array = -272.15,-271.15,-270.15,-269.15,-268.15],[-267.15,-266.15,-265.15,-264.15,-263.15
#     converted_array = -949.54,-947.74,-945.94,-944.14,-942.34],[-940.54,-938.74,-936.94,-935.14,-933.34
#     converted_array = -1709.172,-1705.932,-1702.692,-1699.452,-1696.212],[-1692.972,-1689.732,-1686.492,-1683.252,-1680.012
#     converted_array = -1436.022,-1432.782,-1429.542,-1426.302,-1423.062],[-1419.822,-1416.582,-1413.342,-1410.102,-1406.862
#     converted_array = -1162.872,-1159.632,-1156.392,-1153.152,-1149.912],[-1146.672,-1143.432,-1140.192,-1136.952,-1133.712
#     converted_array = -889.722,-886.482,-883.242,-880.002,-876.762],[-873.522,-870.282,-867.042,-863.802,-860.562
#     converted_array = -494.29,-492.49,-490.69,-488.89,-487.09],[-485.29,-483.49,-481.69,-479.89,-478.09
#     converted_array = -274.60555556,-273.60555556,-272.60555556,-271.60555556,-270.60555556],[-269.60555556,-268.60555556,-267.60555556,-266.60555556,-265.60555556
#     converted_array = -152.55864198,-152.00308642,-151.44753086,-150.89197531,-150.33641975],[-149.7808642,-149.22530864,-148.66975309,-148.11419753,-147.55864198
    
