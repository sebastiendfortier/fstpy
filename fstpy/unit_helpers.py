# -*- coding: utf-8 -*-
import logging

import cf_units
import dask.array as da
import pandas as pd

from .utils import safe_concatenate


class UnitConversionError(Exception):
    """Exception raised when unit conversion fails."""

    pass


# Map CMC dictionary unit labels to cf_units compatible names
CMC_TO_CF_UNITS = {
    # Temperature units
    "°C": "celsius",
    "c": "celsius",
    "C": "celsius",
    "deg C": "celsius",
    "degC": "celsius",
    "celsius": "celsius",
    "k": "kelvin",
    "K": "kelvin",
    "degK": "kelvin",
    "kelvin": "kelvin",
    "k/day": "K/day",
    "k/s": "K/s",
    "K/K": "1",
    "K²": "K^2",
    "fahrenheit": "fahrenheit",  # cf_units supports fahrenheit directly
    # Length units
    "meter": "m",
    "m": "m",
    "M": "m",
    "cm": "cm",
    "km": "km",
    "ft": "feet",
    "feet": "feet",
    "pieds": "feet",
    "PIEDS": "feet",
    "100 ft": "100 feet",
    "100*ft": "100 feet",
    "dam": "dam",
    "DAM": "dam",
    "decameter": "dam",
    "mm": "mm",
    "MM": "mm",
    # Time units
    "s": "s",
    "sec": "s",
    "hr": "hour",
    "hrs": "hour",
    "HEURE": "hour",
    "heures": "hour",
    "days": "day",
    # Pressure units
    "Pa": "Pa",
    "PA": "Pa",
    "hpa": "hPa",
    "hPa": "hPa",
    "hPA": "hPa",
    "mb": "mbar",
    "millibar": "mbar",
    "KPa": "kPa",
    "Pa/s": "Pa/s",
    "pa/s": "Pa/s",
    "hectoPascal": "hPa",
    "pascal": "Pa",
    # Ensure all variants of pascal are covered
    "Pascal": "Pa",
    "PASCAL": "Pa",
    # Additional pressure variants for consistency
    "hectopascal": "hPa",
    "HectoPascal": "hPa",
    "HECTOPASCAL": "hPa",
    # Velocity units
    "m s⁻¹": "m/s",
    "m/s": "m/s",
    "M/S": "m/s",
    "M/S/DAY": "m/s/day",
    "m/s/day": "m/s/day",
    "m/s2": "m/s^2",
    "km/h": "km/h",
    "knots/noeuds": "knot",
    "knot": "knot",
    "kts": "knot",
    "noeuds": "knot",
    "kt*h": "nautical_mile",  # knot-hour = nautical mile
    # Mass and density units
    "kg⁻¹": "1/kg",
    "kg/g": "kg/g",
    "kg kg⁻¹": "kg/kg",
    "kg/kg": "kg/kg",
    "KG/KG": "kg/kg",
    "kg m⁻³": "kg/m^3",
    "kg/m³": "kg/m^3",
    "kg/m3": "kg/m^3",
    "KG/M3": "kg/m^3",
    "g": "g",
    "g/g": "g/g",
    "g/kg": "g/kg",
    "g/s": "g/s",
    "µg/kg": "ug/kg",
    "ug/kg": "ug/kg",
    "ug/g": "1e-6",  # dimensionless (ug/g = ppm)
    "µg/m³": "ug/m^3",
    "ug/m3": "ug/m^3",
    "micrograms/kg": "ug/kg",
    "micrograms/M3": "ug/m^3",
    # Area and flux units
    "kg m⁻² s⁻¹": "kg/(m^2.s)",
    "kg m-2 s-1": "kg/(m^2.s)",
    "kg.m-2": "kg/m^2",
    "kg m⁻²": "kg/m^2",
    "kg.m⁻²": "kg/m^2",
    "kg/m²": "kg/m^2",
    "kg/m2": "kg/m^2",
    "Kg/m2": "kg/m^2",
    "KG/M2": "kg/m^2",
    "kg/m²/s": "kg/(m^2.s)",
    "kg/m2/s": "kg/(m^2.s)",
    "m⁻¹": "1/m",
    "m-1": "1/m",
    "m⁻²": "1/m^2",
    "m⁻² s⁻¹": "1/(m^2.s)",
    "m-2/s": "1/(m^2.s)",
    "M-2/S": "1/(m^2.s)",
    "m-3": "1/m^3",
    "m²": "m^2",
    "m2": "m^2",
    "km²": "km^2",
    "m²/m²": "m^2/m^2",
    "m2/m2": "m^2/m^2",
    "m² s⁻¹": "m^2/s",
    "m²/s": "m^2/s",
    "m2/s": "m^2/s",
    "M2/S": "m^2/s",
    "m² s⁻²": "m^2/s^2",
    "m².s⁻²": "m^2/s^2",
    "m²/s²": "m^2/s^2",
    "m²/s2": "m^2/s^2",
    "m2/s2": "m^2/s^2",
    "M2/S2": "m^2/s^2",
    "m²/s³": "m^2/s^3",
    "M2/S3": "m^2/s^3",
    "m³": "m^3",
    "m³/kg": "m^3/kg",
    "m³/s": "m^3/s",
    "m³/m³": "m^3/m^3",
    "m3/m3": "m^3/m^3",
    "M3/M3": "m^3/m^3",
    "m^3/m^3": "m^3/m^3",
    # Energy and work units
    "J kg-1": "J/kg",
    "J kg⁻¹": "J/kg",
    "J/kg": "J/kg",
    "J/KG": "J/kg",
    "Jkg-1": "J/kg",
    "Jkg⁻¹": "J/kg",
    "J.M-2": "J/m^2",
    "J.m⁻²": "J/m^2",
    "J m⁻²": "J/m^2",
    "J/m2": "J/m^2",
    "J/M2": "J/m^2",
    "J/m²": "J/m^2",
    "J m⁻³ K⁻¹": "J/(m^3.K)",
    "J/m³/K": "J/(m^3.K)",
    "J/M3/K": "J/(m^3.K)",
    "K m⁻² J⁻¹": "K.m^2/J",
    "K m-2 J-1": "K.m^2/J",
    # Power units
    "W": "W",
    "W m⁻²": "W/m^2",
    "w/m2": "W/m^2",
    "W/m²": "W/m^2",
    "W/m2": "W/m^2",
    "W/M2": "W/m^2",
    "mW m-2": "mW/m^2",
    "w.m⁻².K⁻¹": "W/(m^2.K)",
    "w.m-2.k-1": "W/(m^2.K)",
    "W m⁻¹ K⁻¹": "W/(m.K)",
    # Angle units
    "deg": "degree",
    "DEG": "degree",
    "deg true": "degree",
    "degree": "degree",
    "DEG|M": "1",  # dimensionless
    "rad": "radian",
    "rad/s": "radian/s",
    # misc units
    "ppb": "ppb",
    "PPB": "ppb",
    "DU": "2.687e20 m^-2",  # Dobson unit = 2.687×10^20 molecules/m^2
    "dobson": "2.687e20 m^-2",  # Dobson unit
    "dbz": "1",  # decibels relative to Z (reflectivity) - treated as dimensionless
    "dbZ": "1",  # decibels relative to Z (reflectivity) - treated as dimensionless
    "dBZ": "1",  # decibels relative to Z (reflectivity) - treated as dimensionless
    "scalar": "1",  # dimensionless
    "nil": "1",  # dimensionless
    "NIL": "1",  # dimensionless
    "no units": "1",  # dimensionless
    "inconnue": "1",  # dimensionless
    "bool": "1",  # dimensionless
    "code": "1",  # dimensionless
    "fraction": "1",  # dimensionless
    "psu": "1",  # dimensionless (practical salinity unit)
    "ln(kg/kg)": "1",  # dimensionless
    "log(m)": "1",  # dimensionless
    "µmoles/m²": "umol/m^2",
    "N m⁻¹": "N/m",
    "N/m": "N/m",
    "Pa s": "Pa.s",
    "PA*S": "Pa.s",
    "Pa*s": "Pa.s",
    "s m⁻¹": "s/m",
    "s/m": "s/m",
    "s m-1": "s/m",
    "s⁻¹": "1/s",
    "s-1": "1/s",
    "S-1": "1/s",
    "1/s": "1/s",
    "1/S": "1/s",
    "s⁻²": "1/s^2",
    "s-2": "1/s^2",
    "1/day": "1/day",
}


def get_cf_unit(unit_name):
    """Convert a unit name to a cf_units Unit object.

    Args:
        unit_name (str): Name of the unit to convert

    Returns:
        cf_units.Unit: The corresponding cf_units Unit object

    Raises:
        ValueError: If the unit name is invalid or not supported
    """
    # First try direct mapping
    cf_unit_str = CMC_TO_CF_UNITS.get(unit_name)

    if cf_unit_str is None:
        # If no direct mapping, try using the name as is
        cf_unit_str = unit_name

    try:
        return cf_units.Unit(cf_unit_str)
    except ValueError as e:
        raise ValueError(f"Invalid or unsupported unit: {unit_name}") from e


def get_unit_converter(from_unit, to_unit):
    """Get a converter function between two units.

    Args:
        from_unit (str): Source unit name
        to_unit (str): Target unit name

    Returns:
        callable: A function that converts values from source to target unit

    Raises:
        ValueError: If units are invalid or incompatible
    """
    if from_unit == to_unit:
        return None

    try:
        source = get_cf_unit(from_unit)
        target = get_cf_unit(to_unit)

        if not source.is_convertible(target):
            raise ValueError(f"Units {from_unit} and {to_unit} are not compatible")

        def converter(values):
            # Handle dask arrays
            if isinstance(values, da.Array):
                # Create a conversion function that will be applied to the computed array
                def convert_computed(x):
                    # x will be a numpy array after computation
                    return source.convert(x, target)

                # Ensure the conversion happens after the array is computed
                return values.map_blocks(convert_computed, meta=values._meta)
            else:
                return source.convert(values, target)

        return converter
    except Exception as e:
        raise ValueError(f"Unit conversion error: {str(e)}")


def unit_convert(df: pd.DataFrame, to_unit_name="1", standard_unit=False) -> pd.DataFrame:
    """Converts the data portion 'd' of all the records of a dataframe to the specified unit
    provided in the to_unit_name parameter. If the standard_unit flag is True, the to_unit_name
    will be ignored and the unit will be based on the standard file variable dictionnary unit
    value instead. This ensures that if a unit conversion was done, the varaible will return
    to the proper standard file unit value. ex. : TT should be in celsius. o.dict can be consulted
    to get the appropriate unit values.

    :param df: dataframe containing records to be converted
    :type df: pd.DataFrame
    :param to_unit_name: unit name to convert to, defaults to '1' (dimensionless)
    :type to_unit_name: str, optional
    :param standard_unit: flag to indicate the use of dictionnary units, defaults to False
    :type standard_unit: bool, optional
    :return: a dataframe containing the converted data
    :rtype: pd.DataFrame
    """
    from fstpy.dataframe import add_columns, add_unit_and_description_columns
    from fstpy.std_dec import get_unit

    # Check for None values in the 'd' column
    if df["d"].isna().any():
        raise UnitConversionError("Cannot convert None values")

    if "unit" not in df.columns:
        df = add_unit_and_description_columns(df)

    meta_df = df.loc[df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])].reset_index(drop=True)
    # remove meta data from DataFrame
    df = df.loc[~df.nomvar.isin(["^^", ">>", "^>", "!!", "!!SF", "HY", "P0", "PT"])].reset_index(drop=True)

    res_df = df.copy(deep=True)
    unit_to = CMC_TO_CF_UNITS.get(to_unit_name, to_unit_name)

    for row in res_df.itertuples():
        current_unit = row.unit

        if current_unit == to_unit_name:
            continue
        elif (not standard_unit) and ((current_unit == "1") or (to_unit_name == "1")):
            continue
        else:
            unit_from = CMC_TO_CF_UNITS.get(current_unit, current_unit)
            if standard_unit:
                to_unit_name = get_unit(row.nomvar, row.ip1, row.ip3)[0]
                unit_to = CMC_TO_CF_UNITS.get(to_unit_name, to_unit_name)
                if current_unit == "1" and not to_unit_name == "1":
                    logging.warning(
                        f"'{row.nomvar}': unit == '1', can't convert unitless to standard unit which is '{to_unit_name}'. No conversion will be done."
                    )
                    continue
                converter = get_unit_converter(unit_from, unit_to)
            else:
                converter = get_unit_converter(unit_from, unit_to)

            if not (converter is None):
                converted_arr = converter(row.d)
                res_df.at[row.Index, "d"] = converted_arr
                res_df.at[row.Index, "unit"] = to_unit_name
                res_df.at[row.Index, "unit_converted"] = True
                res_df["unit_converted"] = res_df["unit_converted"].astype("bool")

    # Necessaire de faire ce check pour eviter warning "object-dtype columns with all-bool values ..." si meta_df est vide
    res_df = safe_concatenate([res_df, meta_df])

    if not standard_unit:
        if "level" not in res_df.columns:
            res_df = add_columns(res_df, columns=["ip_info"])

        res_df = res_df.sort_values(by="level", ascending=res_df.ascending.unique()[0]).reset_index(drop=True)

    return res_df
