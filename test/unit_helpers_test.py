# -*- coding: utf-8 -*-
import pytest
import numpy as np
import cf_units
from fstpy.unit_helpers import get_cf_unit, get_unit_converter, CMC_TO_CF_UNITS

pytestmark = [pytest.mark.unit, pytest.mark.unit_tests]


def test_1():
    """Test basic unit conversion to cf_units"""
    # Test direct mapping
    assert isinstance(get_cf_unit("celsius"), cf_units.Unit)
    assert isinstance(get_cf_unit("K"), cf_units.Unit)
    assert isinstance(get_cf_unit("m/s"), cf_units.Unit)

    # Test case insensitive
    assert get_cf_unit("celsius") == get_cf_unit("Celsius")
    assert get_cf_unit("m/s") == get_cf_unit("M/S")

    # Test direct cf_units string
    assert isinstance(get_cf_unit("meters"), cf_units.Unit)
    assert isinstance(get_cf_unit("kg m-2"), cf_units.Unit)


def test_2():
    """Test temperature unit conversions"""
    celsius = get_cf_unit("celsius")
    kelvin = get_cf_unit("K")
    fahrenheit = get_cf_unit("fahrenheit")

    assert celsius.convert(0, kelvin) == pytest.approx(273.15)
    assert kelvin.convert(273.15, celsius) == pytest.approx(0)
    assert celsius.convert(0, fahrenheit) == pytest.approx(32)


def test_3():
    """Test dimensionless unit handling"""
    # Test various forms of dimensionless units
    assert get_cf_unit("scalar") == get_cf_unit("1")
    assert get_cf_unit("fraction") == get_cf_unit("1")
    assert get_cf_unit("kg/kg") == get_cf_unit("1")
    assert get_cf_unit("m/m") == get_cf_unit("1")


def test_4():
    """Test invalid unit handling"""
    with pytest.raises(ValueError):
        get_cf_unit("invalid_unit")
    with pytest.raises(ValueError):
        get_cf_unit("not_a_unit")


def test_5():
    """Test basic unit conversion"""
    # Test meter to kilometer conversion
    m_to_km = get_unit_converter("m", "km")
    assert m_to_km(1000) == pytest.approx(1.0)

    # Test km/h to m/s conversion
    kmh_to_ms = get_unit_converter("km/h", "m/s")
    assert kmh_to_ms(3.6) == pytest.approx(1.0)

    # Test no conversion needed
    assert get_unit_converter("m", "m") is None
    assert get_unit_converter("scalar", "scalar") is None


def test_6():
    """Test temperature unit conversion"""
    c_to_k = get_unit_converter("celsius", "K")
    k_to_c = get_unit_converter("K", "celsius")
    c_to_f = get_unit_converter("celsius", "fahrenheit")

    assert c_to_k(0) == pytest.approx(273.15)
    assert k_to_c(273.15) == pytest.approx(0)
    assert c_to_f(0) == pytest.approx(32)


def test_7():
    """Test unit conversion with arrays"""
    m_to_km = get_unit_converter("m", "km")
    arr = np.array([1000, 2000, 3000])
    expected = np.array([1, 2, 3])
    np.testing.assert_array_almost_equal(m_to_km(arr), expected)


def test_8():
    """Test incompatible unit conversion"""
    with pytest.raises(ValueError):
        get_unit_converter("m", "kg")
    with pytest.raises(ValueError):
        get_unit_converter("celsius", "m/s")


def test_9():
    """Test invalid unit conversion"""
    with pytest.raises(ValueError):
        get_unit_converter("invalid_unit", "m")
    with pytest.raises(ValueError):
        get_unit_converter("m", "not_a_unit")


def test_10():
    """Test CMC unit mappings"""
    # Test that all CMC unit mappings are valid cf_units
    for cmc_unit, cf_unit in CMC_TO_CF_UNITS.items():
        try:
            unit = cf_units.Unit(cf_unit)
            assert isinstance(unit, cf_units.Unit)
        except ValueError as e:
            pytest.fail(f"Invalid cf_unit mapping for {cmc_unit}: {cf_unit}")


def test_11():
    """Test common CMC unit conversions"""
    # Temperature
    assert get_unit_converter("°C", "K")(0) == pytest.approx(273.15)
    assert get_unit_converter("degC", "K")(0) == pytest.approx(273.15)

    # Pressure
    assert get_unit_converter("mb", "Pa")(1000) == pytest.approx(100000)
    assert get_unit_converter("hPa", "Pa")(1000) == pytest.approx(100000)

    # Velocity
    assert get_unit_converter("knots", "m/s")(1) == pytest.approx(0.51444444)
    assert get_unit_converter("km/h", "m/s")(3.6) == pytest.approx(1.0)

    # Mass per area
    assert get_unit_converter("kg/m2", "g/cm2")(1) == pytest.approx(0.1)

    # Dimensionless
    assert get_unit_converter("kg/kg", "%")(0.5) == pytest.approx(50)
    assert get_unit_converter("%", "fraction")(50) == pytest.approx(0.5)
