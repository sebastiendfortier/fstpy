# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import dask.array as da
import pytest
import psutil
import os
import fstpy
from fstpy.utils import safe_concat
from test import TEST_PATH
from fstpy.std_reader import StandardFileReader

pd.set_option("display.max_columns", None)

pytestmark = [pytest.mark.unit_tests]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH + "ReaderStd/testsFiles/"


def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def mock_get_data(path, key, dtype, shape):
    """Mock function to simulate file reading"""
    return np.zeros(shape, dtype=dtype)


@pytest.fixture
def dask_df(plugin_test_dir):
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    df = StandardFileReader(source0).to_pandas()
    return df


@pytest.fixture
def dask_df_pf(plugin_test_dir):
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PF.std"
    df = StandardFileReader(source0).to_pandas()
    return df


@pytest.fixture
def dask_df_pgsm(plugin_test_dir):
    source0 = plugin_test_dir + "missingData.std"
    df = StandardFileReader(source0).to_pandas()
    return df


@pytest.fixture
def basic_df(dask_df):
    df = fstpy.compute(dask_df)
    return df


@pytest.fixture
def different_shape_df(dask_df_pgsm):
    df = fstpy.compute(dask_df_pgsm)
    return df


@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=["nomvar", "d"])


@pytest.fixture
def none_df():
    return None


@pytest.fixture
def mixed_df(basic_df, dask_df):
    """Create a dataframe with mixed numpy and dask arrays"""
    df = pd.concat([dask_df, basic_df], ignore_index=True)
    return df


def test_1(basic_df):
    """Test concatenating two basic dataframes with numpy arrays"""
    df1 = basic_df.copy()
    df2 = basic_df.copy()
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert all(isinstance(arr, np.ndarray) for arr in result["d"])


def test_2(dask_df):
    """Test concatenating two dataframes with dask arrays"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert all(isinstance(arr, da.core.Array) for arr in result["d"])


def test_3(mixed_df):
    """Test concatenating dataframes with mixed numpy and dask arrays"""
    df1 = mixed_df.copy()
    df2 = mixed_df.copy()
    result = safe_concat([df1, df2])
    assert len(result) == 8
    # Check that dask arrays weren't computed
    assert any(isinstance(arr, da.core.Array) for arr in result["d"])


def test_4(basic_df, different_shape_df):
    """Test concatenating dataframes with arrays of different shapes"""
    result = safe_concat([basic_df, different_shape_df])
    assert len(result) == 7
    assert result["d"].iloc[0].shape != result["d"].iloc[:1].shape


def test_5(empty_df, basic_df):
    """Test concatenating with empty dataframes"""
    result = safe_concat([empty_df, basic_df])
    assert len(result) == 2
    assert isinstance(result["d"].iloc[0], np.ndarray)


def test_6(basic_df):
    """Test concatenating dataframes with different columns"""
    df1 = basic_df.copy()
    df2 = basic_df.copy()
    df2["extra_col"] = "YY"
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert "extra_col" in result.columns


def test_7(basic_df, dask_df):
    """Test concatenating dataframes with None values in 'd' column"""
    df1 = basic_df.copy()
    df2 = dask_df.copy()
    df2["d"] = None
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert result["d"].iloc[2] is None


def test_8(basic_df, dask_df):
    """Test concatenating dataframes with empty arrays"""
    df1 = basic_df.copy()
    df2 = dask_df.copy()
    df2["d"] = [np.array([]), np.array([])]
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert len(result["d"].iloc[2]) == 0


def test_9(basic_df):
    """Test concatenating dataframes with different indices"""
    df1 = basic_df.copy()
    df2 = basic_df.copy()
    # Set different indices with the same length as the dataframe
    df2.index = [0, 10]
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert result.index.tolist() == [0, 1, 2, 3]


def test_10(basic_df, dask_df):
    """Test concatenating dataframes with different dtypes in 'd' column"""
    df1 = basic_df.copy()
    df2 = dask_df.copy()
    df1["d"] = [np.array([[1, 2], [3, 4]], dtype=np.int32), np.array([[1, 2], [3, 4]], dtype=np.int32)]
    result = safe_concat([df1, df2])
    assert len(result) == 4
    assert result["d"].iloc[0].dtype != result["d"].iloc[3].dtype


def test_11(basic_df):
    """Test concatenating many dataframes"""
    dfs = [basic_df.copy() for _ in range(100)]
    result = safe_concat(dfs)
    assert len(result) == 200


def test_12(basic_df, dask_df, different_shape_df, empty_df):
    """Test concatenating dataframes with all possible combinations"""
    result = safe_concat([basic_df, dask_df, different_shape_df, empty_df])
    assert len(result) == 9  # Empty dataframe is filtered out
    # Check that dask arrays weren't computed
    assert any(isinstance(arr, da.core.Array) for arr in result["d"])


def test_13(dask_df):
    """Test that safe_concat preserves the task graph"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()

    # Get the original task graph
    original_graph = df1["d"].iloc[0].dask

    # Using safe_concat should preserve the task graph
    result = safe_concat([df1, df2])
    assert result["d"].iloc[0].dask == original_graph


def test_14(dask_df):
    """Test that safe_concat produces the same result as pd.concat after file reads"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    # Compute the dask arrays
    safe_df = fstpy.compute(safe_df)
    pd_df = fstpy.compute(pd_df)

    assert safe_df.equals(pd_df)


def test_15(dask_df):
    """Test that safe_concat preserves all columns and data types"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()
    df2["extra_col"] = ["YY", "YY"]

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    # Compare column names and dtypes
    assert set(safe_df.columns) == set(pd_df.columns)
    for col in safe_df.columns:
        if col != "d":  # Skip 'd' column as it contains dask arrays
            assert safe_df[col].dtype == pd_df[col].dtype


def test_16(basic_df, dask_df):
    """Test that safe_concat preserves data when mixing with numpy arrays"""
    df1 = basic_df.copy()
    df2 = dask_df.copy()

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    # Compute the dask arrays
    safe_df = fstpy.compute(safe_df)
    pd_df = fstpy.compute(pd_df)

    assert safe_df.equals(pd_df)


def test_17(dask_df, dask_df_pgsm):
    """Test that safe_concat preserves data with different array shapes"""
    df1 = dask_df.copy()
    df2 = dask_df_pgsm.copy()

    # Get results from both methods
    safe_result = safe_concat([df1, df2])
    pd_result = pd.concat([df1, df2], ignore_index=True)

    # Compute the dask arrays
    safe_df = fstpy.compute(safe_result)
    pd_df = fstpy.compute(pd_result)

    assert safe_df.equals(pd_df)
    # Verify shapes are preserved
    assert safe_df["d"].iloc[0].shape != safe_df["d"].iloc[2].shape


def test_18(dask_df):
    """Test that safe_concat preserves data with None values"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()
    df2["d"] = None

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    assert safe_df.equals(pd_df)

    # Verify None values are preserved
    assert safe_df["d"].iloc[2] is None


def test_19(dask_df):
    """Test that safe_concat preserves data with empty arrays"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()
    df2["d"] = [np.array([], dtype=np.float32), np.array([], dtype=np.float32)]

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    # Compute the dask arrays
    safe_df = fstpy.compute(safe_df)
    pd_df = fstpy.compute(pd_df).reset_index(drop=True)

    assert safe_df.equals(pd_df)
    # Verify empty arrays are preserved
    assert len(safe_df["d"].iloc[2]) == 0


def test_20(dask_df):
    """Test that safe_concat preserves data with mixed array types and shapes"""
    df1 = fstpy.compute(dask_df.copy())
    df2 = pd.DataFrame({"nomvar": ["YY"], "d": [np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32)]})
    df3 = pd.DataFrame({"nomvar": ["ZZ"], "d": [None]})
    df4 = pd.DataFrame({"nomvar": ["WW"], "d": [np.array([], dtype=np.float32)]})

    # Get results from both methods
    safe_df = safe_concat([df1, df2, df3, df4])
    pd_df = pd.concat([df1, df2, df3, df4], ignore_index=True)

    assert safe_df.equals(pd_df)
    # Verify all types are preserved
    assert isinstance(safe_df["d"].iloc[0], np.ndarray)
    assert isinstance(safe_df["d"].iloc[2], np.ndarray)
    assert safe_df["d"].iloc[3] is None
    assert len(safe_df["d"].iloc[4]) == 0


def test_21(dask_df):
    """Test that safe_concat preserves data with different column orders"""
    df1 = dask_df.copy()
    df2 = dask_df.copy()
    df2 = df2[df2.columns[::-1]]

    # Get results from both methods
    safe_df = safe_concat([df1, df2])
    pd_df = pd.concat([df1, df2], ignore_index=True)

    assert safe_df.equals(pd_df)
    # Verify column order is preserved
    assert safe_df.columns.tolist() == pd_df.columns.tolist()
