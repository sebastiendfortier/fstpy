# -*- coding: utf-8 -*-
from fstpy.dataframe_utils import select_with_meta
from test import TEST_PATH, TMP_PATH

import fstpy
import pytest
from ci_fstcomp import fstcomp
import secrets

pytestmark = [pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + "Pressure/testsFiles/"


@pytest.fixture
def default_cols():
    DEFAULT_COLUMNS = [
        "nomvar",
        "etiket",
        "typvar",
        "ni",
        "nj",
        "nk",
        "dateo",
        "datev",
        "ip1",
        "ip2",
        "ip3",
        "deet",
        "npas",
        "datyp",
        "nbits",
        "grtyp",
        "ig1",
        "ig2",
        "ig3",
        "ig4",
    ]
    return DEFAULT_COLUMNS


def test_1(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele eta avec l'option --coordinateType ETA_COORDINATE. VCODE 1002"""
    # open and read source
    source0 = plugin_test_dir + "tt_eta_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "R1580V0N"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_1.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_eta_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_2(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele eta avec les options --coordinateType ETA_COORDINATE --standardAtmosphere."""
    # open and read source
    source0 = plugin_test_dir + "tt_eta_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0, standard_atmosphere=True).compute()

    df.loc[df.nomvar != "PXSA", "etiket"] = "R1580V0N"
    df.loc[df.nomvar == "PXSA", "etiket"] = "PRESSR"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_2.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_eta_std_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_3(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele Sigma, avec l'option --coordinateType SIGMA_COORDINATE. VCODE 1001"""
    # open and read source
    source0 = plugin_test_dir + "hu_sig_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["HU"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    # La reduction de colonnes pour que les ip1 soient encodes
    df = fstpy.reduce_columns(df)

    df.loc[df.nomvar != "P0", "etiket"] = "__PRESSRX"
    df.loc[df.nomvar == "P0", "etiket"] = "GA72A16_N"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_3.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_sig_file2cmp.std+PY20240118"

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.1, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_4(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele Sigma, avec les options --coordinateType SIGMA_COORDINATE --standardAtmosphere."""
    # open and read source
    source0 = plugin_test_dir + "hu_sig_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["HU"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0, standard_atmosphere=True).compute()

    # La reduction de colonnes pour que les ip1 soient encodes
    df = fstpy.reduce_columns(df)

    df.loc[df.nomvar != "P0", "etiket"] = "__PRESSRX"
    df.loc[df.nomvar == "P0", "etiket"] = "GA72A16_N"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_4.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_sig_std_file2cmp.std+PY20240118"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_5(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele hybrid, avec l'option --coordinateType HYBRID_COORDINATE."""
    # open and read source
    source0 = plugin_test_dir + "tt_hyb_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "R1580V0N"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_5.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_hyb_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_6(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele Hybrid avec les options --coordinateType HYBRID_COORDINATE --standardAtmosphere."""
    # open and read source
    source0 = plugin_test_dir + "tt_hyb_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0, standard_atmosphere=True).compute()

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_6.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_hyb_std_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_7(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele Hybrid staggered, avec l'option --coordinateType HYBRID_STAGGERED_COORDINATE."""
    # open and read source
    source0 = plugin_test_dir + "px_hyb_stg_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["UU"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[df.nomvar.isin(["!!", ">>", "^^", "P0"]), "etiket"] = "__PRESS_X"
    df.loc[df.nomvar == "PX", "etiket"] = "__PRESSRX"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_7.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_hyb_stg_file2cmp.std+PY20240118"

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_8(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele Hybrid staggered, avec les options --coordinateType HYBRID_STAGGERED_COORDINATE --standardAtmosphere."""
    # open and read source
    source0 = plugin_test_dir + "px_hyb_stg_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["UU"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0, standard_atmosphere=True).compute()

    df.loc[df.nomvar.isin(["!!", ">>", "^^", "P0"]), "etiket"] = "__PRESS_X"
    df.loc[df.nomvar == "PXSA", "etiket"] = "__PRESSRX"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_8.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_hyb_stg_std_file2cmp.std+PY20240118"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_9(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele en pression, avec l'option --coordinateType PRESSURE_COORDINATE."""
    # open and read source
    source0 = plugin_test_dir + "tt_pres_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[df.nomvar.isin([">>", "^^"]), "etiket"] = "R1580V0_N"
    df.loc[df.nomvar == "PX", "etiket"] = "__PRESSRX"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_9.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_pres_file2cmp.std+20240408"

    # compare results
    # On veut comparer datyp et nbits aussi - ils ne font pas partie de la liste par defaut
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_10(plugin_test_dir, default_cols):
    """Test sur un fichier sortie de modele en pression avec les options --coordinateType PRESSURE_COORDINATE --standardAtmosphere."""
    # open and read source
    source0 = plugin_test_dir + "tt_pres_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0, standard_atmosphere=True).compute()

    df.loc[df.nomvar.isin([">>", "^^"]), "etiket"] = "R1580V0_N"
    df.loc[df.nomvar == "PXSA", "etiket"] = "__PRESSRX"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_10.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "px_pres_std_file2cmp.std+20240408"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_11(plugin_test_dir, default_cols):
    """Test avec un fichier contenant differentes heures de prevision."""
    # open and read source
    source0 = plugin_test_dir + "input_vrpcp24_00_fileSrc.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "R110K80N"
    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_11.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df, no_meta=True).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_vrpcp24_00_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_12(plugin_test_dir, default_cols):
    """Test avec un fichier glbpres avec l'option --coordinateType PRESSURE_COORDINATE"""
    # open and read source
    source0 = plugin_test_dir + "glbpres_TT_UU_VV.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    tt_df = fstpy.select_with_meta(src_df0, ["TT"])
    tt_df = tt_df.loc[tt_df.ip1 != 93423264]

    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(tt_df).compute()

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_12.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "glbpres_pressure_coordinate_file2cmp.std+20210517"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_13(plugin_test_dir, default_cols):
    """Test avec un fichier qui genere des artefacts dans les cartes"""
    # open and read source
    source0 = plugin_test_dir + "2019091000_000_input.orig"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])

    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "G1_7_0_0N"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_13.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df, no_meta=True).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "d.compute_pressure_varicelle_rslt.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_14(plugin_test_dir, default_cols):
    """Test avec un fichier 5005 avec l'option --coordinateType HYBRID_5005_COORDINATE thermodynamic"""
    # open and read source
    source0 = plugin_test_dir + "coord_5005_big.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["TT"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "R1_V710_N"
    df = df.loc[~df.nomvar.isin(["^^", ">>", "P0"])]

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_14.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_35_TT.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, exclude_meta=True, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_15(plugin_test_dir, default_cols):
    """Test avec un fichier 5005 avec l'option --coordinateType HYBRID_5005_COORDINATE"""
    # open and read source
    source0 = plugin_test_dir + "coord_5005_big.std"
    src_df0 = fstpy.StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0, ["UU"])
    # compute fstpy.QuickPressure
    df = fstpy.QuickPressure(src_df0).compute()

    df.loc[:, "etiket"] = "R1_V710_N"
    df = df.loc[~df.nomvar.isin(["^^", ">>", "P0"])]
    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_15.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()
    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_36_UU.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, exclude_meta=True, e_max=0.01, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res


def test_16(plugin_test_dir, default_cols):
    """2 groupes de TT avec dates d'origine differentes mais dates de validity indentiques"""

    source = plugin_test_dir + "Regeta_TTHUES_differentDateoSameDatev.std"
    src_df = fstpy.StandardFileReader(source).to_pandas()
    tt_df = fstpy.select_with_meta(src_df, ["TT"])

    df = fstpy.QuickPressure(tt_df).compute()

    #  Pour etre identique au test de Pressure de Spooki
    df.loc[df.nomvar.isin(["PX"]), "etiket"] = "__PRESSRX000"

    # write the result
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_16.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # # open and read comparison file
    file_to_compare = plugin_test_dir + "Regeta_differentDateoSameDatev_file2cmp.std"

    # compare results
    res = fstcomp(results_file, file_to_compare, columns=default_cols)
    fstpy.delete_file(results_file)
    assert res
