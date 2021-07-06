# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import math
from fstpy.unit import unit_convert




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


def test_kelvin_conversion():
    res_df = unit_convert(base_dataframe(),'kelvin')
    assert res_df['unit'].all() == 'kelvin'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == False
    assert math.isclose(res_df.iloc[0]['d'][0][0],268.15,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],252.594,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-2.77777778,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-5.0,abs_tol=0.001)

def test_celsius_conversion():
    res_df = unit_convert(base_dataframe(),'celsius')
    assert res_df['unit'].all() == 'celsius'
    assert res_df.iloc[0]['unit_converted'] == False
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],-20.5556,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-275.928,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-278.15,abs_tol=0.001)

def test_fahrenheit_conversion():
    res_df = unit_convert(base_dataframe(),'fahrenheit')
    assert res_df['unit'].all() == 'fahrenheit'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == False
    assert res_df.iloc[2]['unit_converted'] == True
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],23,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-464.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-468.67,abs_tol=0.001)

def test_rankine_conversion():
    res_df = unit_convert(base_dataframe(),'rankine')
    assert res_df['unit'].all() == 'rankine'
    assert res_df.iloc[0]['unit_converted'] == True
    assert res_df.iloc[1]['unit_converted'] == True
    assert res_df.iloc[2]['unit_converted'] == False
    assert res_df.iloc[3]['unit_converted'] == True
    assert math.isclose(res_df.iloc[0]['d'][0][0],482.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[1]['d'][0][0],454.67,abs_tol=0.001)
    assert math.isclose(res_df.iloc[2]['d'][0][0],-5.0,abs_tol=0.001)
    assert math.isclose(res_df.iloc[3]['d'][0][0],-9,abs_tol=0.001)
    

def main():
    test_kelvin_conversion()
    test_celsius_conversion()
    test_fahrenheit_conversion()
    test_rankine_conversion()

if __name__ == "__main__":
    main()























