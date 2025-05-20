Unit Conversion
===============

The unit conversion system in fstpy uses the `cf_units` library to provide robust and standardized unit conversions. This ensures compatibility with the CF (Climate and Forecast) metadata conventions widely used in the meteorological community.

Overview
--------

The unit conversion system consists of two main components:

1. A mapping system that converts CMC (Canadian Meteorological Centre) unit names to CF-compliant unit names
2. The `cf_units` library that handles all unit conversions

Key Features
------------

- Support for a wide range of meteorological units
- Automatic conversion between compatible units
- Handling of dimensionless quantities
- Temperature scale conversions (Celsius, Kelvin, Fahrenheit)
- Support for compound units (e.g., kg/(m²·s))

Supported Unit Categories
-------------------------

- Temperature (°C, K, °F)
- Length (m, km, cm, mm, ft)
- Time (s, hr, day)
- Pressure (Pa, hPa, mb)
- Velocity (m/s, km/h, knot)
- Mass and density (kg, g, kg/m³)
- Area and flux (m², kg/m², W/m²)
- Energy and work (J, J/kg)
- Power (W, W/m²)
- Angles (degree, radian)
- Dimensionless quantities (%, fraction, ratio)

Usage
-----

To convert units in your data, you can use the following functions:

1. Direct unit conversion using `cf_units`::

    from fstpy.unit_helpers import get_cf_unit, get_unit_converter
    
    # Get a cf_units Unit object
    celsius_unit = get_cf_unit('celsius')
    kelvin_unit = get_cf_unit('K')
    
    # Convert a value
    temp_c = 0.0
    temp_k = celsius_unit.convert(temp_c, kelvin_unit)  # Returns 273.15

2. Converting arrays with unit conversion::

    import numpy as np
    from fstpy.unit_helpers import get_unit_converter
    
    # Create a converter function
    converter = get_unit_converter('km/h', 'm/s')
    
    # Convert an array
    speeds = np.array([36.0, 72.0, 90.0])  # km/h
    speeds_ms = converter(speeds)  # Converts to m/s

3. Converting units in a DataFrame::

    import fstpy
    
    # Read data
    df = fstpy.StandardFileReader('myfile.std').to_pandas()
    
    # Convert temperature from Celsius to Kelvin
    df = fstpy.unit_convert(df, 'kelvin')

CMC to CF Unit Mapping
----------------------

The system automatically maps CMC unit names to their CF-compliant equivalents. For example:

- 'c' → 'celsius'
- 'kt' → 'knot'
- 'mb' → 'mbar'
- 'dam' → '10 m'

Error Handling
--------------

The unit conversion system includes robust error handling:

- Raises `ValueError` for invalid or unsupported units
- Raises `UnitConversionError` for incompatible unit conversions
- Validates all unit mappings at runtime

For a complete list of supported units and their mappings, refer to the `CMC_TO_CF_UNITS` dictionary in the source code.

.. automodule:: fstpy.unit_helpers
   :members: get_cf_unit, get_unit_converter
