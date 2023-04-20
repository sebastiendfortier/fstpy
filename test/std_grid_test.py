# -*- coding: utf-8 -*-
from pathlib import Path
from test import TEST_PATH

import numpy as np
import pytest

import fstpy

pytestmark = [pytest.mark.std_grid, pytest.mark.unit_tests]

@pytest.fixture(scope="module", params=[str, Path])
def input_file(request):
    return request.param(TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std')



def test_1(input_file):
    """Test getting lat lons from x and  y indexes"""
    std_file = fstpy.StandardFileReader(input_file)
    df = std_file.to_pandas()

    x = [0, 0, 0, 1107]
    y = [0, 1, 2, 1081]

    la = [-8.05976390838623,  -8.001700401306152, -7.94362735748291, 57.716590881347656]
    lo = [231.28515625,  231.21572875976562,  231.1462860107422, 39.63421630859375]

    res_df = fstpy.get_lat_lon_from_index(df.loc[df.nomvar=='TT'], x, y)

    assert(np.allclose(x, res_df.x, rtol=0.005))
    assert(np.allclose(y, res_df.y, rtol=0.005))
    assert(np.allclose(res_df.lat, la, rtol=0.005))
    assert(np.allclose(res_df.lon, lo, rtol=0.005))
          

def test_2(input_file):
    """Test getting indexes from lats and lons"""
    std_file = fstpy.StandardFileReader(input_file)
    df = std_file.to_pandas()

    x = [0, 0, 0, 1107]
    y = [0, 1, 2, 1081]

    la = [-8.05976390838623,  -8.001700401306152, -7.94362735748291, 57.716590881347656]
    lo = [231.28515625,  231.21572875976562,  231.1462860107422, 39.63421630859375]



    res_df = fstpy.get_index_from_lat_lon(df.loc[df.nomvar=='TT'], la, lo)

    assert(np.allclose(x, [round(e) for e in res_df.x], rtol=0.005))
    assert(np.allclose(y, [round(e) for e in res_df.y], rtol=0.005))
    assert(np.allclose(res_df.lat, la, rtol=0.005))
    assert(np.allclose(res_df.lon, lo, rtol=0.005))

    
