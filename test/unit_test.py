# -*- coding: utf-8 -*-
import pytest
from fstpy.unit import do_unit_conversion, UnitConversionError
import pandas as pd
import numpy as np
import math

@pytest.mark.unit
class TestUnit:

    @pytest.fixture
    def base_dataframe(self):
        arr = np.array([-5.0])
        d = [
            {'nomvar':'TT','unit':'celsius','unit_converted':False,'d':arr},
            {'nomvar':'TT','unit':'fahrenheit','unit_converted':False,'d':arr},
            {'nomvar':'TT','unit':'rankine','unit_converted':False,'d':arr},
            ]
        df = pd.DataFrame(d)
        return df


    def test_base_conversion(self,base_dataframe):
        res_df = do_unit_conversion(base_dataframe,'kelvin')
        assert res_df['unit'].all() == 'kelvin'
        assert res_df['unit_converted'].all() == True
        assert math.isclose(res_df['d'][0],np.array([268.15]))
        assert math.isclose(res_df['d'][1],np.array([250.3722222222]))
        assert math.isclose(res_df['d'][2],np.array([-2.77777778]))


    def test_no_data(self,base_dataframe):
        base_dataframe['d'] = None
        with pytest.raises(UnitConversionError):
            res_df = do_unit_conversion(base_dataframe,'kelvin')


    def test_wrong_unit_type(self,base_dataframe):
        with pytest.raises(TypeError):
            res_df = do_unit_conversion(base_dataframe,'meter')    
