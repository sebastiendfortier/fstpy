# -*- coding: utf-8 -*-
from packages.fstpy.standardfile import reorder_dataframe
from fstpy.standardfile import *
import pytest
import pandas as pd
import numpy as np
from test import TMP_PATH, TEST_PATH


@pytest.mark.std_writer
def a_test():
    pass