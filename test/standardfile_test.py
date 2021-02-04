# -*- coding: utf-8 -*-
from fstpy.standardfile import *
import pytest
import pandas as pd
import numpy as np
import math

@pytest.fixture
def input_file():
    return '/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std'

@pytest.mark.standardfile
def test_open(input_file):
    std_file = StandardFileReader(input)
    assert type(std_file) == StandardFileReader   
    assert std_file.meta_data == StandardFileReader.meta_data


# StandardFileReader(
#     filenames,
#     keep_meta_fields=False,
#     read_meta_fields_only=False,
#     add_extra_columns=True,
#     materialize=False,
#     subset={'datev': -1, 'etiket': ' ', 'ip1': -1, 'ip2': -1, 'ip3': -1, 'typvar': ' ', 'nomvar': ' '},
# )
@pytest.mark.standardfile
def test_params_keep_meta_fields_true(input_file):
    std_file = StandardFileReader(input,keep_meta_fields=True)
    df = std_file.to_pandas()
    assert len(df.index) == 1794
    assert len(df.columns) == 51

def test_params_keep_meta_fields_false(input_file):
    std_file = StandardFileReader(input,keep_meta_fields=False)
    pass