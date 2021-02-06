# -*- coding: utf-8 -*-
import pytest
from fstpy.standardfile import *
from test import TMP_PATH, TEST_PATH


@pytest.mark.std_writer
class TestStandardFileReader:

    @pytest.fixture
    def input_file(self):
        return TEST_PATH + '/ReaderStd/testsFiles/source_data_5005.std'

    def a_test():
        pass