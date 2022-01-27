# -*- coding: utf-8 -*-
import pytest
import pandas as pd
from test import TMP_PATH, TEST_PATH
import datetime
import fstpy.all as fstpy

pytestmark = [pytest.mark.unit_tests]

@pytest.fixture
def input_file():
    return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'
    

def test_1(input_file):
    """Test adding  a localized date of validity in Canada/Eastern"""
    src_df0 = fstpy.StandardFileReader(input_file).to_pandas()

    src_df0 = fstpy.add_columns(src_df0,'datev')

    src_df0 = fstpy.add_timezone_column(src_df0,'date_of_validity','Canada/Eastern')

    assert('date_of_validity_Canada_Eastern' in src_df0.columns)

    assert(not src_df0.loc[src_df0.date_of_validity_Canada_Eastern==datetime.datetime(2020,7,14,8)].empty)