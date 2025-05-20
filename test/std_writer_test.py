# -*- coding: utf-8 -*-
import pytest
import warnings
from test import TMP_PATH, TEST_PATH
from pathlib import Path
import tempfile
import fstpy
from fstpy.utils import delete_file
from ci_fstcomp import fstcomp
import secrets
import numpy as np

pytestmark = [pytest.mark.std_writer, pytest.mark.unit_tests]


@pytest.fixture
def input_file():
    return TEST_PATH + "ReaderStd/testsFiles/source_data_5005.std"


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + "WriterStd/testsFiles/"


@pytest.fixture(scope="module", params=[str, Path])
def tmp_file(request):
    temp_name = next(tempfile._get_candidate_names())
    return request.param(TMP_PATH + temp_name)


# filename:str, df:pd.DataFrame, add_meta_fields=True, overwrite=False, load_data=False

# Definition de la liste de colonnes pour fstcomp.  Par defaut, datyp et nbits ne sont pas dans la liste de fstcomp
# et on veut les comparer.
list_of_columns = [
    "nomvar",
    "etiket",
    "typvar",
    "ni",
    "nj",
    "nk",
    "dateo",
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


def test_1(plugin_test_dir):
    """Test conversion des donnees int64 vers int32 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # int32
        df.loc[:, "datyp"] = 2  # Unsigned integer
        df.loc[:, "nbits"] = 32

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 2147483647, dtype=np.int64)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]
        # print(f'\n Apres conversion - {arr.dtype= } \n', df.at[i,'d'])

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_1.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test1_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_2(plugin_test_dir):
    """Test conversion des donnees float64 vers int32 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # int32
        df.loc[:, "datyp"] = 2  # Unsigned integer
        df.loc[:, "nbits"] = 32

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 6.8, dtype=np.float64)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_2.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test2_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_3(plugin_test_dir):
    """Test conversion des donnees int32 vers float32 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # float 32
        df.loc[:, "datyp"] = 5  # IEEE floating point
        df.loc[:, "nbits"] = 32

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 2147483647, dtype=np.int32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_3.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test3-4_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_4(plugin_test_dir):
    """Test conversion des donnees float64 vers float32 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # float 32
        df.loc[:, "datyp"] = 5  # IEEE floating point
        df.loc[:, "nbits"] = 32

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 2147483647, dtype=np.float64)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_4.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test3-4_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_5(plugin_test_dir):
    """Test conversion des donnees float32 vers R24 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        #  R24
        df.loc[:, "datyp"] = 1  # Floating point
        df.loc[:, "nbits"] = 24

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 6.8, dtype=np.float32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_5.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test5_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_6(plugin_test_dir):
    """Test conversion des donnees float32 vers F12 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        #  F12
        df.loc[:, "datyp"] = 6  # Floating point (special format, 16 bit, reserved for use with the compressor)
        df.loc[:, "nbits"] = 12

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 6.8, dtype=np.float32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_6.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test6_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_7(plugin_test_dir):
    """Test conversion des donnees float32 vers f12 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        #  F12
        df.loc[:, "datyp"] = 134  # Compressed floating point
        df.loc[:, "nbits"] = 12

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 6.8, dtype=np.float32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_7.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test7_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_8(plugin_test_dir):
    """Test conversion des donnees float32 positives vers int16 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # int32
        df.loc[:, "datyp"] = 4  # signed integer
        df.loc[:, "nbits"] = 16

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, 6.8, dtype=np.float32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_8.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test8_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_9(plugin_test_dir):
    """Test conversion des donnees float32 negatives vers int16 tel que requis par le datype et nbits"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # int32
        df.loc[:, "datyp"] = 4  # signed integer
        df.loc[:, "nbits"] = 16

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, -6.8, dtype=np.float32)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_9.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test9_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_10(plugin_test_dir):
    """Test conversion des donnees float64 negative(-1.0) vers uint8 tel que requis par le datype et nbits. Le resultat attendu sera max uint8(255)"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)

    # Suppression d'un future warning de pandas; dans notre cas, on veut conserver le meme comportement
    # meme avec le nouveau comportement a venir. On encapsule la suppression du warning pour ce cas seulement.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        # int32
        df.loc[:, "datyp"] = 2  # unsigned integer
        df.loc[:, "nbits"] = 8

    for i in df.index:
        arr = df.at[i, "d"].compute()
        arr_filled = np.full_like(arr, -1.0, dtype=np.float64)
        df.at[i, "d"] = arr_filled
        arr = df.at[i, "d"]

    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_10.std"])
    fstpy.delete_file(results_file)
    fstpy.StandardFileWriter(results_file, df).to_fst()

    # compare results
    file_to_compare = plugin_test_dir + "UUVVTT5x5_test10_conversion_PY_file2cmp.std"
    res = fstcomp(results_file, file_to_compare, columns=list_of_columns)
    fstpy.delete_file(results_file)
    assert res


def test_11():
    """Test case to validate error handling when writing an empty DataFrame."""
    import pandas as pd

    # Create an empty DataFrame
    df = pd.DataFrame()

    # Define the file path to write the DataFrame
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_empty_dataframe.std"])
    fstpy.delete_file(results_file)

    try:
        # Try to write the empty DataFrame, should fail
        fstpy.StandardFileWriter(results_file, df).to_fst()
    except fstpy.StandardFileWriterError as e:
        print(f"The error message is:\n{e}")
        return

    # If no exception was raised, fail the test
    fstpy.delete_file(results_file)
    pytest.fail("Expected a StandardFileWriterError to be raised when writing an empty DataFrame.")


def test_12(plugin_test_dir):
    """Test case to validate error handling when writing a DataFrame that only contains metadata fields."""

    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df = fstpy.StandardFileReader(source0).to_pandas()
    df = src_df.loc[(src_df.nomvar == "TT")].reset_index(drop=True)
    df["nomvar"] = ">>"

    # Define the file path to write the DataFrame
    results_file = "".join([TMP_PATH, secrets.token_hex(16), "test_empty_dataframe.std"])
    fstpy.delete_file(results_file)

    try:
        # Try to write a DataFrame that only contain metadata fields, should fail
        fstpy.StandardFileWriter(results_file, df).to_fst()
    except fstpy.StandardFileWriterError as e:
        print(f"The error message is: \n{e}")
        return

    # If no exception was raised, fail the test
    fstpy.delete_file(results_file)
    pytest.fail(
        "Expected a StandardFileWriterError to be raised when writing a DataFrame that only contain metadata fields"
    )


# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# def test_read_write_noload(input_file,tmp_file):
#     df = fstpy.StandardFileReader(input_file).to_pandas()
#     fstpy.StandardFileWriter(tmp_file,df).to_fst()
#     res = fstpy.fstcomp(input_file,tmp_file)
#     assert(res)

# def test_invalid_path(input_file):
#     std_file = StandardFileReader(input_file)
#     df = std_file.to_pandas()

#     #should crash
#     std_file_writer = StandardFileWriter('/tmp/123456/1',df)
#     with pytest.raises(FileNotFoundError):
#         std_file_writer.to_fst()

# def test_empty_df(tmp_file):
#     df = pd.DataFrame(dtype=object)

#     #should crash
#     with pytest.raises(StandardFileWriterError):
#         std_file_writer = StandardFileWriter(tmp_file,df)
#     #std_file_writer.to_fst()

# def test_default_not_load_datad(input_file,tmp_file):
#     std_file = StandardFileReader(input_file)
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(tmp_file,df)
#     std_file_writer.to_fst()


# def test_default_not_load_datad_same_file(input_file,tmp_file):
#     file = tmp_file

#     std_file = StandardFileReader(input_file,query='nomvar=="TT"')
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df)
#     std_file_writer.to_fst()


#     std_file = StandardFileReader(file)
#     tmp_df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,tmp_df)
#     with pytest.raises(StandardFileError):
#         std_file_writer.to_fst()

#     delete_file(file)


# def test_default_not_load_datad_same_file_overwrite(input_file,tmp_file):
#     file = tmp_file

#     std_file = StandardFileReader(input_file,query='nomvar=="TT"')
#     df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df)
#     std_file_writer.to_fst()

#     std_file = StandardFileReader(file)
#     tmp_df = std_file.to_pandas()

#     std_file_writer = StandardFileWriter(file,df,overwrite=True)
#     std_file_writer.to_fst()


# # def test_default_not_load_datad_same_file_overwrite(input_file,tmp_file):
# #     file = tmp_file

# #     std_file = StandardFileReader(input_file)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df,overwrite=True)
# #     std_file_writer.to_fst()

# #     std_file = StandardFileReader(tmp_file)
# #     tmp_df = std_file.to_pandas()

# #     status = fstcomp_df(df,tmp_df)
# #     assert status

# # def test_default_normal(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_meta_only_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_meta_only(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_default_no_extra(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_default_no_extra_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,load_datad=True,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_no_extra_load_datad(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_default_load_datad_query(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status


# # def test_params_normal_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_meta_only_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,read_meta_fields_only=True)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     #should crash
# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()
# #     assert False

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False)
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_no_extra_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,decode_metadata=False,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status

# # def test_params_query_writer_load_data(input_file,tmp_file):
# #     std_file = StandardFileReader(input_file,query='nomvar=="UU"')
# #     df = std_file.to_pandas()

# #     std_file_writer = StandardFileWriter(tmp_file,df)
# #     std_file_writer.to_fst()

# #     written_file = StandardFileReader(tmp_file)
# #     written_df = written_file.to_pandas()

# #     status = fstcomp_df(df,written_df,exclude_meta=False)
# #     delete_file(tmp_file)
# #     assert status
