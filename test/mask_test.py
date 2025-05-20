# -*- coding: utf-8 -*-

from test import TEST_PATH, TMP_PATH
import fstpy
import pytest
import copy
import numpy as np
from ci_fstcomp import fstcomp
import secrets

pytestmark = [pytest.mark.regressions]


@pytest.fixture
def test_data0():
    return TEST_PATH + "ReaderStd_WriterStd/testsFiles/UUVV5x5_fileSrc.std"


@pytest.fixture
def test_data1():
    return TEST_PATH + "ReaderStd_WriterStd/testsFiles/data_with_mask.std"


@pytest.fixture
def test_data2():
    return TEST_PATH + "MultiplyElementsByPoint/testsFiles/2021071400_024_masked_fields.std"


def test_1(test_data1):
    """Test application d'un fichier avec masque"""
    # open and read source
    source0 = test_data1
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_nb_rows = len(src_df0.index)
    # compute fstpy.ApplyMask
    df = fstpy.ApplyMask(src_df0).compute()

    assert len(df.index) == (src_nb_rows / 2)
    # # write the result
    # results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_1.std"])
    # fstpy.delete_file(results_file)
    # fstpy.StandardFileWriter(results_file, df).to_fst()

    # # open and read comparison file
    # file_to_compare = test_data1

    # # compare results
    # res = fstcomp(results_file, file_to_compare)
    # fstpy.delete_file(results_file)
    # assert(res)


def test_2(test_data2):
    """Test application d'un fichier avec masque et metas donnees"""
    # open and read source
    source0 = test_data2
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_nb_rows = len(src_df0.index) - 4
    # compute fstpy.ApplyMask
    df = fstpy.ApplyMask(src_df0).compute()

    assert len(df.index) == (src_nb_rows / 2) + 4


def test_3(test_data2):
    """Test application d'un fichier avec masque, donnees regulieres et metas donnees"""
    # open and read source
    source0 = test_data2
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    df = fstpy.ApplyMask(src_df0).compute()

    new_df = fstpy.RecoverMask(df).compute()

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_3.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, new_df).to_fst()

    # open and read comparison file
    file_to_compare = test_data2

    # compare results
    cmp_df0 = fstpy.StandardFileReader(file_to_compare).to_pandas()
    cmp_df1 = fstpy.StandardFileReader(results_file).to_pandas()

    cmp_df0 = cmp_df0.sort_values(["nomvar", "typvar", "etiket"]).reset_index(drop=True)
    cmp_df1 = cmp_df1.sort_values(["nomvar", "typvar", "etiket"]).reset_index(drop=True)

    df0_arrays = copy.deepcopy(cmp_df0.d)
    df1_arrays = copy.deepcopy(cmp_df1.d)

    assert cmp_df1.drop("d", axis=1).equals(cmp_df0.drop("d", axis=1))

    for i in range(len(df1_arrays)):
        assert np.array_equal(fstpy.to_numpy(df1_arrays[i]), fstpy.to_numpy(df0_arrays[i]))

    fstpy.delete_file(results_file)


def test_4(test_data0):
    """Test application d'un fichier avec masque, donnees regulieres et metas donnees"""
    # open and read source
    source0 = test_data0
    new_df = src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    df = fstpy.ApplyMask(src_df0).compute()

    new_df = fstpy.RecoverMask(df).compute()

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_4.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, new_df).to_fst()

    # open and read comparison file
    file_to_compare = test_data0

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.001)
    fstpy.delete_file(results_file)
    assert res
