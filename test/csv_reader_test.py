from array import array
import copy
import itertools
from lib2to3.pgen2.pgen import DFAState
import multiprocessing as mp
from mimetypes import init
import re
import pandas as pd
import os
import numpy as np
import csv
import datetime
from fstpy import std_enc
import pytest
import fstpy.csv_reader

pytestmark = [pytest.mark.csv_reader, pytest.mark.unit_tests]


@pytest.fixture
def input_file():
    return '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'


def test_1():
    csv_file = fstpy.csv_reader.CsvFileReader('/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv')
    return csv_file.to_pandas()


