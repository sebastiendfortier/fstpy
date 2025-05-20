# -*- coding: utf-8 -*-
from typing import Dict, Any
import datetime
from typing import Final, Dict, List, Tuple, Union, Optional
from .std_io import decode_ip123

import numpy as np
import pandas as pd

from fstpy import DATYP_DICT, INV_DATYP_DICT
from fstpy.unit_helpers import CMC_TO_CF_UNITS
from fstpy.utils import vectorize
from .rmn_interface import RmnInterface

import cmcdict


class Interval:
    def __init__(self, ip, low, high, kind) -> None:
        self.ip = ip
        self.low = low
        self.high = high
        self.kind = kind
        self.pkind = RmnInterface.kind_to_string(kind)
        pass

    def delta(self):
        if self.kind not in [0, 2, 4, 21, 10]:
            return None
        return self.high - self.low

    def __str__(self):
        return f"{self.ip}:{self.low}{self.pkind}@{self.high}{self.pkind}"

    def __eq__(self, other):
        if other is None:
            return False
        return self.ip == other.ip and self.low == other.low and self.high == other.high and self.kind == other.kind

    def __ne__(self, other):
        return not (self == other)


def get_interval(ip1: int, ip2: int, ip3: int, i1: dict, i2: dict, i3: dict) -> Union["Interval", None]:
    """Gets interval if exists from ip values

    :param ip1: ip1 value
    :type ip1: int
    :param ip2: ip2 value
    :type ip2: int
    :param ip3: ip3 value
    :type ip3: int
    :param i1: decoded ip1 values
    :type i1: dict
    :param i2: decoded ip2 values
    :type i2: dict
    :param i3: decoded ip2 values
    :type i3: dict
    :return: Interval
    :rtype: Interval
    """
    if ip3 >= 32768:
        if (ip1 >= 32768) and (i1["kind"] == i3["kind"]):
            return Interval("ip1", i1["v1"], i1["v2"], i1["kind"])
        elif (ip2 >= 32768) and (i2["kind"] == i3["kind"]):
            # Intervalle de temps, borne inf a borne sup (v1 = borne sup, v2 = borne_inf)
            return Interval("ip2", i2["v2"], i2["v1"], i2["kind"])
        else:
            return None
    return None


def get_level_sort_order(kind: int) -> bool:
    """returns the level sort order

    :param kind: level kind
    :type kind: int
    :return: True if the level type is ascending or False otherwise
    :rtype: bool
    """
    # order = {0:'ascending',1:'descending',2:'descending',4:'ascending',5:'descending',21:'ascending'}
    order = {0: True, 3: True, 4: True, 21: True, 100: True, 1: False, 2: False, 5: False, 6: False, 7: False}
    if kind in order.keys():
        return order[kind]

    return False


def get_forecast_hour(deet: int, npas: int) -> datetime.timedelta:
    """creates a timedelta object in seconds from deet * npas

    :param deet: This is the length of a time step used during a model integration, in seconds.
    :type deet: int
    :param npas: This is the time step number at which the field was written during an integration. The number of the initial time step is 0.
    :type npas: int
    :return: time delta in seconds
    :rtype: datetime.timedelta
    """
    if (deet != 0) or (npas != 0):
        return datetime.timedelta(seconds=int(npas * deet))
    return datetime.timedelta(0)


VCREATE_FORECAST_HOUR: Final = vectorize(get_forecast_hour, otypes=["timedelta64[ns]"])  # ,otypes=['timedelta64[ns]']


def get_data_type_str(datyp: int):
    """gets the data type string from the datyp int

    :param datyp: data type int value
    :type datyp: int
    :return: string equivalent of the datyp int value
    :rtype: str
    """
    return DATYP_DICT[datyp]


def get_data_type_value(datyp: str):
    """gets the data type int from the datyp string

    :param datyp: data type str value
    :type datyp: str
    :return: int equivalent of the datyp str value
    :rtype: int
    """
    return INV_DATYP_DICT[datyp]


VCREATE_DATA_TYPE_STR: Final = vectorize(get_data_type_str, otypes=["str"])


def get_ip_info(nomvar: str, ip1: int, ip2: int, ip3: int):
    """gets all relevant level info from the ip1 int value

    :param ip1: encoded value stored in ip1
    :type ip1: int
    :return: level value, kind and kind str obtained from decoding ip1 and bools representing if the level is a surface level, if it follows topography and its sort order.
    :rtype: float,int,str,bool,bool,bool
    """
    # iii1, iii2, iii3 = rmn.DecodeIp(ip1,ip2,ip3)
    i1, i2, i3 = decode_ip123(nomvar, ip1, ip2, ip3)
    # if nomvar not in ['>>','^^','!!','^>']:
    #     print(nomvar,iii1,i1,iii2,i2,iii3,i3)

    #     print(nomvar ,[(iii1.v1,i1['v1']) if (iii1.v1 != i1['v1']) else True, (iii2.v1,i2['v1']) if (iii2.v1 != i2['v1']) else True, (iii3.v1,i3['v1']) if (iii3.v1 != i3['v1']) else True])

    surface = is_surface(i1["kind"], i1["v1"])

    follow_topography = level_type_follows_topography(i1["kind"])

    ascending = get_level_sort_order(i1["kind"])

    # don't search for interval in fields that use IPs for association with IGs
    if nomvar in [">>", "^^", "!!", "^>"]:
        interval = None
    else:
        interval = get_interval(ip1, ip2, ip3, i1, i2, i3)

    return (
        i1["v1"],
        i1["kind"],
        i1["kinds"],
        i2["v1"],
        i2["kind"],
        i2["kinds"],
        i3["v1"],
        i3["kind"],
        i3["kinds"],
        surface,
        follow_topography,
        ascending,
        interval,
    )


VCREATE_IP_INFO: Final = vectorize(
    get_ip_info,
    otypes=[
        "float32",
        "int32",
        "str",
        "float32",
        "int32",
        "str",
        "float32",
        "int32",
        "str",
        "bool",
        "bool",
        "bool",
        "object",
    ],
)


def get_metadata_batch(nomvars, ip1s=None, ip3s=None):
    """Gets metadata for multiple variables at once using cmcdict's batch method

    :param nomvars: list of variable names
    :type nomvars: List[str]
    :param ip1s: list of ip1 values (optional)
    :type ip1s: List[int]
    :param ip3s: list of ip3 values (optional)
    :type ip3s: List[int]
    :return: list of metadata dictionaries
    :rtype: List[Dict]
    """
    if ip1s is None:
        ip1s = [None] * len(nomvars)
    if ip3s is None:
        ip3s = [None] * len(nomvars)

    # Create list of tuples for batch call
    var_tuples = list(zip(nomvars, ip1s, ip3s))

    # Get metadata for each variable
    results = []
    for nomvar, ip1, ip3 in var_tuples:
        try:
            info = cmcdict.get_metvar_metadata(nomvar, ip1=ip1, ip3=ip3)
            results.append(info if info else None)
        except Exception:
            results.append(None)

    return results


def get_unit_and_description(nomvar, ip1=None, ip3=None, existing_units=None, existing_descriptions=None):
    """Reads the Standard file dictionnary and gets the unit and description associated with the variable name

    :param nomvar: name of the variable
    :type nomvar: str or pd.Series
    :param ip1: ip1 value (optional)
    :type ip1: int or pd.Series
    :param ip3: ip3 value (optional)
    :type ip3: int or pd.Series
    :param existing_units: existing unit values to preserve (optional)
    :type existing_units: pd.Series
    :param existing_descriptions: existing description values to preserve (optional)
    :type existing_descriptions: pd.Series
    :return: unit name and description
    :rtype: Tuple[np.ndarray, np.ndarray]

    >>> get_unit_and_description('TT')
    array(['celsius']), array(['Air Temperature'])
    """
    if isinstance(nomvar, (pd.Series, list, np.ndarray)):
        # Convert to list for batch processing
        nomvars = list(nomvar)
        ip1s = list(ip1) if ip1 is not None else None
        ip3s = list(ip3) if ip3 is not None else None

        # Batch processing
        var_infos = get_metadata_batch(nomvars, ip1s, ip3s)
        units = []
        descriptions = []

        for i, info in enumerate(var_infos):
            # Use existing values if available
            if existing_units is not None and pd.notna(existing_units.iloc[i]):
                units.append(existing_units.iloc[i])
            elif not info:
                units.append("1")
            else:
                var_unit = info["units"]
                cf_unit = CMC_TO_CF_UNITS.get(var_unit)
                units.append(cf_unit if cf_unit is not None else "1")

            if existing_descriptions is not None and pd.notna(existing_descriptions.iloc[i]):
                descriptions.append(existing_descriptions.iloc[i])
            elif not info:
                descriptions.append("N/A")
            else:
                descriptions.append(info["description_short_en"])

        return np.array(units), np.array(descriptions)
    else:
        # Single variable processing
        var_infos = cmcdict.get_metvar_metadata(nomvar, ip1=ip1, ip3=ip3)
        if not var_infos:
            return np.array(["1"]), np.array(["N/A"])

        var_unit = var_infos["units"]
        cf_unit = CMC_TO_CF_UNITS.get(var_unit)
        return (np.array([cf_unit if cf_unit is not None else "1"]), np.array([var_infos["description_short_en"]]))


def get_description(nomvar, ip1=None, ip3=None):
    """Reads the Standard file dictionnary and gets the description associated with the variable name

    :param nomvar: name of the variable
    :type nomvar: str or pd.Series
    :param ip1: ip1 value (optional)
    :type ip1: int or pd.Series
    :param ip3: ip3 value (optional)
    :type ip3: int or pd.Series
    :return: description
    :rtype: np.ndarray

    >>> get_description('TT')
    array(['Air Temperature'])
    """
    if isinstance(nomvar, (pd.Series, list, np.ndarray)):
        # Convert to list for batch processing
        nomvars = list(nomvar)
        ip1s = list(ip1) if ip1 is not None else None
        ip3s = list(ip3) if ip3 is not None else None

        # Batch processing
        var_infos = get_metadata_batch(nomvars, ip1s, ip3s)
        return np.array([info["description_short_en"] if info else "N/A" for info in var_infos])
    else:
        # Single variable processing
        var_infos = cmcdict.get_metvar_metadata(nomvar, ip1=ip1, ip3=ip3)
        if not var_infos:
            print(f"nomvar: '{nomvar}' not found in operational dictionary. Description set to 'N/A'")
            return np.array(["N/A"])
        return np.array([var_infos["description_short_en"]])


def _all_same_units(var_infos: Dict[str, Dict[str, Any]]) -> bool:
    units = {subdict["units"] for subdict in var_infos.values()}
    return len(units) == 1


def get_unit(nomvar, ip1=None, ip3=None):
    """Reads the Standard file dictionnary and gets the unit associated with the variable name

    :param nomvar: name of the variable
    :type nomvar: str or pd.Series
    :param ip1: ip1 value (optional)
    :type ip1: int or pd.Series
    :param ip3: ip3 value (optional)
    :type ip3: int or pd.Series
    :return: unit name
    :rtype: np.ndarray

    >>> get_unit('TT')
    array(['celsius'])
    """
    if isinstance(nomvar, (pd.Series, list, np.ndarray)):
        # Convert to list for batch processing
        nomvars = list(nomvar)
        ip1s = list(ip1) if ip1 is not None else None
        ip3s = list(ip3) if ip3 is not None else None

        # Batch processing
        var_infos = get_metadata_batch(nomvars, ip1s, ip3s)
        units = []
        for info in var_infos:
            if not info:
                units.append("1")
                continue

            var_unit = info["units"]
            cf_unit = CMC_TO_CF_UNITS.get(var_unit)
            units.append(cf_unit if cf_unit is not None else "1")

        return np.array(units)
    else:
        # Single variable processing
        var_infos = cmcdict.get_metvar_metadata(nomvar, ip1=ip1, ip3=ip3)
        if not var_infos:
            print(f"nomvar: '{nomvar}' not found in operational dictionary. Unit set to '1'")
            return np.array(["1"])
        # TODO handle when ip1 is required but not provided. Problem with AL when the writer is called and is making sure the variable have the right unit.
        # ./apps/spooki_run.py "[ReaderStd --input /home/spst900/dataV/humidex/v13.1.x/inputfiles/2016041212_024_004_ens.regpres] >> [Select --fieldName AL,TT --plugin_language CPP] >> [WriterStd --output test.std --plugin_language PYTHON]"
        if not "nomvar" in var_infos:
            # we have multiple entry for same nomvar (probably never happening)
            # use first one to get unit
            _, first_var_infos = next(iter(var_infos.items()))
            var_unit = first_var_infos["units"]
            if not _all_same_units(var_infos):
                print(
                    f"problem when getting unit for {nomvar=}, multiple entry with different units for the same nomvar\n{var_infos=}"
                )
                print(f"try first entry")
                print(f"{var_unit=}")
        else:
            var_unit = var_infos["units"]

        cf_unit = CMC_TO_CF_UNITS.get(var_unit)
        if cf_unit is None:
            print(f"No fstpy internal unit found for '{var_unit}', unit set to '1'")
            return np.array(["1"])
        return np.array([cf_unit])


VGET_UNIT_AND_DESCRIPTION = get_unit_and_description
VGET_UNIT = get_unit
VGET_DESCRIPTION = get_description


# written by Micheal Neish creator of fstd2nc
def convert_rmndate_to_datetime(date: int) -> Optional[datetime.datetime]:
    """returns a datetime object of the decoded RMNDate int

    :param date: RMNDate int value
    :type date: int
    :return: datetime object of the decoded date
    :rtype: datetime.datetime

    >>> convert_rmndate_to_datetime(442998800)
    datetime.datetime(2020, 7, 14, 12, 0)
    """
    dummy_stamps = (0, 10101011, 101010101)
    if date not in dummy_stamps:
        return RmnInterface.decode_rpn_date(int(date)).replace(tzinfo=None)
    else:
        return np.datetime64("NaT")


def is_surface(ip1_kind: int, level: float) -> bool:
    """Return a bool that tell us if the level is a surface level

    :param ip1_kind: kind of level
    :type ip1_kind: int
    :param level: value of the level
    :type level: float
    :return: True if the level is a surface level else False
    :rtype: bool

    >>> is_surface(5,0.36116)
    False
    """
    meter_levels = np.arange(0.0, 10.5, 0.5).tolist()
    if (ip1_kind == 5) and (level == 1):
        return True
    elif (ip1_kind == 4) and (level in meter_levels):
        return True
    elif (ip1_kind == 1) and (level == 1):
        return True
    else:
        return False


def level_type_follows_topography(ip1_kind: int) -> bool:
    """Returns True if the kind of level is a kind that follows topography

    :param ip1_kind: level type
    :type ip1_kind: int
    :return: True if the kind of level is a kind that follows topography else False
    :rtype: bool

    >>> level_type_follows_topography(5)
    True
    """
    if ip1_kind == 1:
        return True
    elif ip1_kind == 4:
        return True
    elif ip1_kind == 5:
        return True
    else:
        return False


def get_grid_identifier(nomvar: str, ip1: int, ip2: int, ig1: int, ig2: int) -> str:
    """Create a grid identifer from ip2,ip2 or ig1,ig2 depending of the varibale.
    Meta information like >> have their grid identifiers strored in ip1,and ip2,
    while regular viables have them strored in ig1 and ig2

    :param nomvar: name of the variable
    :type nomvar: str
    :param ip1: ip1 value
    :type ip1: int
    :param ip2: ip2 value
    :type ip2: int
    :param ig1: ig1 value
    :type ig1: int
    :param ig2: ig2 value
    :type ig2: int
    :return: concatenation of ig1,ig2 or ip1,ip2 depending on variable name
    :rtype: str

    >>> get_grid_identifier('TT',94733000,6,33792,77761)
    '3379277761'
    """
    nomvar = nomvar.strip()
    if nomvar in ["^>", ">>", "^^", "!!", "!!SF"]:
        grid = "".join([str(ip1), str(ip2)])
    elif nomvar == "HY":
        grid = "None"
    else:
        grid = "".join([str(ig1), str(ig2)])
    return grid


VCREATE_GRID_IDENTIFIER: Final = vectorize(get_grid_identifier, otypes=["str"])


def get_parsed_etiket(raw_etiket: str, etiket_format: str = ""):
    """parses the etiket of a standard file to get label, run, implementation and ensemble member if available

    :param raw_etiket: raw etiket before parsing
    :type raw_etiket: str
    :param etiket_format: flag with number of character in run, label, implementation and ensemble_member
    :type etiket_format: str
    :return: the parsed etiket, run, implementation, ensemble member and etiket_format
    :rtype: str

    >>> get_parsed_etiket('')
    ('', '', '', '')
    >>> get_parsed_etiket('R1_V710_N')
    ('_V710_', 'R1', 'N', '')
    """
    import re

    label = ""
    run = None
    implementation = None
    ensemble_member = None

    if etiket_format != "":
        idx = etiket_format.split(",")
        idx_run = int(idx[0])
        idx_label = int(idx[0]) + int(idx[1])
        idx_implementation = int(idx[0]) + int(idx[1]) + int(idx[2])
        idx_ensemble = int(idx[0]) + int(idx[1]) + int(idx[2]) + int(idx[3])

        run = raw_etiket[:idx_run]
        label = raw_etiket[idx_run:idx_label]
        implementation = raw_etiket[idx_label:idx_implementation]
        ensemble_member = raw_etiket[idx_implementation:idx_ensemble]
        return label, run, implementation, ensemble_member, etiket_format

    # match_run = "[RGPEAIMWNC_][\\dRLHMEA_]"
    match_run = "\\w{2}"
    match_main_cmc = "\\S{5}"
    match_main_spooki = "\\S{6}"
    match_implementation = "[NPX]"
    # match_ensemble_member = "\\w{3}"
    match_ensemble_number3 = "\\d{3}"
    match_ensemble_number4 = "\\d{4}"
    match_ensemble_letter3 = "[a-zA-Z]{3}"
    match_ensemble_letter4 = "[a-zA-Z]{4}"
    match_ensemble_all = "ALL"
    match_ensemble_iic = "\\d{2}[a-zA-Z]"
    match_end = "$"

    re_match_run_only = match_run + match_end
    re_match_cmc_no_ensemble = match_run + match_main_cmc + match_implementation + match_end
    re_match_cmc_ensemble_number3 = (
        match_run + match_main_cmc + match_implementation + match_ensemble_number3 + match_end
    )
    re_match_cmc_ensemble_number4 = (
        match_run + match_main_cmc + match_implementation + match_ensemble_number4 + match_end
    )
    re_match_cmc_ensemble_letter3 = (
        match_run + match_main_cmc + match_implementation + match_ensemble_letter3 + match_end
    )
    re_match_cmc_ensemble_letter4 = (
        match_run + match_main_cmc + match_implementation + match_ensemble_letter4 + match_end
    )
    re_match_spooki_no_ensemble = match_run + match_main_spooki + match_implementation + match_end
    re_match_spooki_ensemble = match_run + match_main_spooki + match_implementation + match_ensemble_number3 + match_end
    re_match_spooki_ensemble_all = match_run + match_main_spooki + match_implementation + match_ensemble_all + match_end
    re_match_spooki_ensemble_iic = match_run + match_main_spooki + match_implementation + match_ensemble_iic + match_end

    if re.match(re_match_cmc_no_ensemble, raw_etiket):
        etiket_format = "2,5,1,0,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
    elif re.match(re_match_cmc_ensemble_number3, raw_etiket):
        etiket_format = "2,5,1,3,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:11]
    elif re.match(re_match_cmc_ensemble_number4, raw_etiket):
        etiket_format = "2,5,1,4,K"
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:12]
    elif re.match(re_match_cmc_ensemble_letter3, raw_etiket):
        etiket_format = "2,5,1,3,K"
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:11]
    elif re.match(re_match_cmc_ensemble_letter4, raw_etiket):
        etiket_format = "2,5,1,4,K"
        run = raw_etiket[:2]
        label = raw_etiket[2:7]
        implementation = raw_etiket[7]
        ensemble_member = raw_etiket[8:12]
    elif re.match(re_match_spooki_no_ensemble, raw_etiket):
        etiket_format = "2,6,1,0,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
    elif re.match(re_match_spooki_ensemble, raw_etiket):
        etiket_format = "2,6,1,3,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
        ensemble_member = raw_etiket[9:12]
    elif re.match(re_match_spooki_ensemble_all, raw_etiket):
        etiket_format = "2,6,1,3,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
        ensemble_member = raw_etiket[9:12]
    elif re.match(re_match_run_only, raw_etiket):
        etiket_format = "2,0,0,0,K"
        run = raw_etiket[:2]
    elif re.match(re_match_spooki_ensemble_iic, raw_etiket):
        etiket_format = "2,6,1,3,D"
        run = raw_etiket[:2]
        label = raw_etiket[2:8]
        implementation = raw_etiket[8]
        ensemble_member = raw_etiket[9:12]
    else:
        if len(raw_etiket) >= 2:
            label_len = len(raw_etiket) - 2
            etiket_format = "2," + str(label_len) + ",0,0,D"
            run = raw_etiket[:2]
            label = raw_etiket[2:]
        else:
            label = raw_etiket
    return label, run, implementation, ensemble_member, etiket_format


VPARSE_ETIKET: Final = vectorize(get_parsed_etiket, otypes=["str", "str", "str", "str", "str"])
