# -*- coding: utf-8 -*-
import pytest
import pandas as pd
import numpy as np
import math
from fstpy.unit_helpers import UnitConversionError, get_unit_converter, unit_convert


pytestmark = [pytest.mark.unit, pytest.mark.unit_tests]


@pytest.fixture
def base_dataframe():
    arr = np.array([[-5.0]], dtype="float32")
    d = [
        {
            "nomvar": "TT",
            "grid": "1",
            "ip1": 0,
            "ip2": 0,
            "ip3": 0,
            "unit": "celsius",
            "unit_converted": False,
            "d": arr,
            "ni": arr.shape[0],
            "nj": arr.shape[0],
            "datev": 123456,
        },
        {
            "nomvar": "TT",
            "grid": "1",
            "ip1": 0,
            "ip2": 0,
            "ip3": 0,
            "unit": "fahrenheit",
            "unit_converted": False,
            "d": arr,
            "ni": arr.shape[0],
            "nj": arr.shape[0],
            "datev": 123456,
        },
        {
            "nomvar": "TT",
            "grid": "1",
            "ip1": 0,
            "ip2": 0,
            "ip3": 0,
            "unit": "kelvin",
            "unit_converted": False,
            "d": arr,
            "ni": arr.shape[0],
            "nj": arr.shape[0],
            "datev": 123456,
        },
    ]
    df = pd.DataFrame(d)
    return df


def test_1(base_dataframe):
    """Test kelvin conversion"""
    res_df = unit_convert(base_dataframe, "kelvin")
    assert res_df["unit"].unique()[0] == "kelvin"
    assert res_df.iloc[0]["unit_converted"] == True
    assert res_df.iloc[1]["unit_converted"] == True
    assert res_df.iloc[2]["unit_converted"] == False
    assert math.isclose(res_df.iloc[0]["d"][0][0], 268.15, abs_tol=0.001)  # -5°C = 268.15K
    assert math.isclose(res_df.iloc[1]["d"][0][0], 252.594, abs_tol=0.001)  # -5°F = 252.594K
    assert math.isclose(res_df.iloc[2]["d"][0][0], -5.0, abs_tol=0.001)  # -5K = -5K (no conversion)


def test_2(base_dataframe):
    """Test celsius conversion"""
    res_df = unit_convert(base_dataframe, "celsius")
    assert res_df["unit"].unique()[0] == "celsius"
    assert res_df.iloc[0]["unit_converted"] == False
    assert res_df.iloc[1]["unit_converted"] == True
    assert res_df.iloc[2]["unit_converted"] == True
    assert math.isclose(res_df.iloc[0]["d"][0][0], -5.0, abs_tol=0.001)  # -5°C = -5°C (no conversion)
    assert math.isclose(res_df.iloc[1]["d"][0][0], -20.5556, abs_tol=0.001)  # -5°F = -20.5556°C
    assert math.isclose(res_df.iloc[2]["d"][0][0], -278.15, abs_tol=0.001)  # -5K = -278.15°C


def test_3(base_dataframe):
    """Test fahrenheit conversion"""
    res_df = unit_convert(base_dataframe, "fahrenheit")
    assert res_df["unit"].unique()[0] == "fahrenheit"
    assert res_df.iloc[0]["unit_converted"] == True
    assert res_df.iloc[1]["unit_converted"] == False
    assert res_df.iloc[2]["unit_converted"] == True
    assert math.isclose(res_df.iloc[0]["d"][0][0], 23.0, abs_tol=0.001)  # -5°C = 23°F
    assert math.isclose(res_df.iloc[1]["d"][0][0], -5.0, abs_tol=0.001)  # -5°F = -5°F (no conversion)
    assert math.isclose(res_df.iloc[2]["d"][0][0], -468.67, abs_tol=0.001)  # -5K = -468.67°F


def test_4(base_dataframe):
    """Test no data"""
    base_dataframe["d"] = None
    with pytest.raises(UnitConversionError):
        _ = unit_convert(base_dataframe, "kelvin")


def test_5(base_dataframe):
    """Test wrong unit type"""
    with pytest.raises(ValueError):
        _ = unit_convert(base_dataframe, "meter")


@pytest.fixture
def array_to_convert():
    a = np.arange(1, 11).reshape(2, 5)
    return a


def test_6():
    """Test temperature unit conversions using get_unit_converter"""
    arr = np.array([0.0])

    # Celsius to Kelvin
    converter = get_unit_converter("celsius", "kelvin")
    assert math.isclose(converter(arr)[0], 273.15, abs_tol=0.001)

    # Kelvin to Celsius
    converter = get_unit_converter("kelvin", "celsius")
    assert math.isclose(converter(arr)[0], -273.15, abs_tol=0.001)

    # Celsius to Fahrenheit
    converter = get_unit_converter("celsius", "fahrenheit")
    assert math.isclose(converter(arr)[0], 32.0, abs_tol=0.001)


def test_7():
    """Test velocity unit conversions using get_unit_converter"""
    arr = np.array([1.0])

    # m/s to km/h
    converter = get_unit_converter("m/s", "km/h")
    assert math.isclose(converter(arr)[0], 3.6, abs_tol=0.001)

    # km/h to knot
    converter = get_unit_converter("km/h", "knot")
    assert math.isclose(converter(arr)[0], 0.539957, abs_tol=0.001)


def test_8():
    """Test dimensionless unit conversions using get_unit_converter"""
    arr = np.array([0.5])

    # fraction to percent
    converter = get_unit_converter("1", "percent")
    assert math.isclose(converter(arr)[0], 50.0, abs_tol=0.001)

    # Same dimensionless units should return None
    assert get_unit_converter("1", "1") is None
    assert get_unit_converter("percent", "percent") is None
