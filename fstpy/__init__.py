# -*- coding: utf-8 -*-
import os
import sys
if sys.version_info < (3, 9):
    import importlib_resources as resources
else:
    import importlib.resources as resources
from pathlib import Path
from threading import RLock, stack_size
from typing import Final, Union, List

import pandas as pd
from .rmn_interface import RmnInterface

error = 0
if sys.version_info[:2] < (3, 8):
    sys.exit("Wrong python version, python>=3.8")


__version__ = "2025.03.00"


_LOCK = RLock()
stack_size(100000000)

# https://wiki.cmc.ec.gc.ca/wiki/Python-RPN/2.1/rpnpy/librmn/fstd98#fstopt


def fstpy_log_level_debug():
    """sets log level to debug"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_DEBUG)


def fstpy_log_level_info():
    """sets log level to info"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_INFO)


def fstpy_log_level_warning():
    """sets log level to warning"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_WARNING)


def fstpy_log_level_error():
    """sets log level to error"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_ERROR)


def fstpy_log_level_fatal():
    """sets log level to fatal"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_FATAL)


def fstpy_log_level_catast():
    """sets log level to catast"""
    RmnInterface.set_log_level(RmnInterface.FSTOPI_MSG_CATAST)


FSTPY_LOG_LEVEL = os.environ.get("FSTPY_LOG_LEVEL")
fstpy_progress = os.environ.get("FSTPY_PROGRESS")
FSTPY_PROGRESS = False
if (not (fstpy_progress is None)) and (fstpy_progress == "True"):
    FSTPY_PROGRESS = True


FSTPY_LOG_VALUES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if FSTPY_LOG_LEVEL is None:
#     fstpy_log_level_info()
# elif FSTPY_LOG_LEVEL not in FSTPY_LOG_VALUES:
#     logging.error(
#         f'Acceptable FSTPY_LOG_LEVEL environment variable values are {FSTPY_LOG_VALUES}')
if not (FSTPY_LOG_LEVEL is None):
    if FSTPY_LOG_LEVEL == "DEBUG":
        fstpy_log_level_debug()
    elif FSTPY_LOG_LEVEL == "INFO":
        fstpy_log_level_info()
    elif FSTPY_LOG_LEVEL == "WARNING":
        fstpy_log_level_warning()
    elif FSTPY_LOG_LEVEL == "ERROR":
        fstpy_log_level_error()
    elif FSTPY_LOG_LEVEL == "CRITICAL":
        fstpy_log_level_catast()


DATYP_DICT = {
    0: "X",
    1: "R",
    2: "I",
    3: "C",
    4: "S",
    5: "E",
    6: "F",
    7: "A",
    8: "Z",
    129: "r",
    130: "i",
    132: "s",
    133: "e",
    134: "f",
}  # : :meta hide-value:

INV_DATYP_DICT = {v: k for k, v in DATYP_DICT.items()}

"""data type aliases constant

:return: correspondance betweeen datyp and str version of datyp
:rtype: dict
:meta hide-value:
"""

KIND_DICT = {
    -1: "_",
    0: "m",  # [metres] (height with respect to sea level)
    1: "sg",  # [sigma] (0.0->1.0)
    2: "mb",  # [mbars] (pressure in millibars)
    3: "   ",  # [others] (arbitrary code)
    4: "M",  # [metres] (height with respect to ground level)
    5: "hy",  # [hybrid] (0.0->1.0)
    6: "th",  # [theta]
    10: "H",  # [hours]
    15: "  ",  # [reserved, integer]
    17: " ",  # [index X of conversion matrix]
    21: "mp",  # [pressure in metres]
}  # : :meta hide-value:
"""kind aliases constant

:return: correspondance betweeen kind and str version of kind
:rtype: dict
:meta hide-value:
"""


def _get_csv_path(filename):
    try:
        csv_dir = resources.files("fstpy.csv")
        with resources.as_file(csv_dir / filename) as csv_path:
            return csv_path
    except KeyError:
        csv_path = None
    return csv_path


_csv_path = _get_csv_path


_stationsfb = pd.read_csv(_csv_path("stationsfb.csv"))
_vctypes = pd.read_csv(_csv_path("verticalcoordinatetypes.csv"))

_leveltypes = pd.read_csv(_csv_path("leveltype.csv"))
_thermoconstants = pd.read_csv(_csv_path("thermo_constants.csv"))

STATIONSFB = _stationsfb  # : :meta hide-value:
"""FB stations table

:return: FB stations table
:rtype: pd.DataFrame
:meta hide-value:

>>> fstpy.STATIONSFB
    StationIntlId StationAlphaId  CanRegCode                 StationName   Latitude   Longitude  StationElevation  TerrainElevation  FictiveStationFlag      SpookiStationKey ProductName
0            71000           CYGW         2.0        'KUUJJUARAPIK A  QC'  55.453333  -78.200000              10.0              10.0                   0   71000CYGW5517N7745W        [FB]
1            71000           CYAB         4.0         'ARCTIC BAY  NU CA'  73.000000  -85.053333              22.0              22.0                   0   71000CYAB7300N8502W        [FB]
2            71000           CYSC         2.0         'SHERBROOKE  QC CA'  45.693333  -72.093333             241.0             241.0                   0   71000CYSC4526N7141W        [FB]
3            71000           CYDL         6.0  'DEASE LAKE LWIS BC (AU5)'  58.666667 -130.053333               NaN             793.0                   0  71000CYDL5825N13002W        [FB]
4            71000           CYLT         5.0      'ALERT AIRPORT  NT CA'  82.826667  -62.453333              31.0              31.0                   0   71000CYLT8231N6217W        [FB]
...

"""

VCTYPES = _vctypes  # : :meta hide-value:
"""vertical coordinate type information table

:return: vertical coordinate type information table
:rtype: pd.DataFrame
:meta hide-value:

>>> fstpy.VCTYPES
    ip1_kind  toctoc     P0     E1     PT     HY     SF  vcode                vctype
0          5    True   True  False  False  False  False   5002           HYBRID_5002
1          5    True   True  False  False  False  False   5001           HYBRID_5001
2          5    True   True  False  False  False  False   5005           HYBRID_5005
...

"""
LEVELTYPES = _leveltypes  # : :meta hide-value:
"""Level type table

:return: dataframe
:rtype: pd.DataFrame
:meta hide-value:

>>> fstpy.LEVELTYPES
                label  kind  follow_topography  surface
0     METER_SEA_LEVEL     0                  0   np.nan
1               SIGMA     1                  1      1.0
2           MILLIBARS     2                  0   np.nan
3      ARBITRARY_CODE     3                  0   np.nan
4  METER_GROUND_LEVEL     4                  1  0.0@5.0
5              HYBRID     5                  1      1.0
6               THETA     6                  0   np.nan
7       MILLIBARS_NEW    12                  0   np.nan
8               INDEX    13                  0   np.nan
9              NUMBER   100                  0   np.nan

"""


THERMO_CONSTANTS = _thermoconstants
"""Thermodynamic constants table

:return: dataframe
:rtype: pd.DataFrame
:meta hide-value:

>>> fstpy.THERMO_CONSTANTS
        name      value
0     'AEw1'    6.10940
1     'AEw2'   17.62500
2     'AEw3'  243.04000
3     'AEi1'    6.11210
4     'AEi2'   22.58700
5     'AEi3'  273.86000
6  'epsilon'    0.62198

"""


def get_constant_row_by_name(df: pd.DataFrame, col_name: str, index_name: str, name: str) -> pd.Series:
    """Get a row from a constant dataframe by name

    :param df: constant dataframe
    :type df: pd.DataFrame
    :param col_name: column name
    :type col_name: str
    :param index_name: index name
    :type index_name: str
    :param name: name to search for
    :type name: str
    :return: row from constant dataframe
    :rtype: pd.Series
    """
    df = df.copy(deep=True)
    df.set_index(index_name, inplace=True)
    if name in df.index:
        return df.loc[name, col_name]
    else:
        return pd.Series(dtype=object)


def get_constant_by_name(name: str) -> pd.Series:
    return get_constant_row_by_name(THERMO_CONSTANTS, "value", "name", name)


def get_column_value_from_row(row, column):
    return row[column].values[0]


# def init_dask(num_cpus:int=None):
#     """Create a dask client that works on one machine and that has
#         processes=True. This is important because of rpnpy and its non parallel
#         compatibility

#     :param num_cpus: number of dask cpus, defaults to None
#     :type num_cpus: int, optional
#     """
#     if num_cpus is None:
#         num_cpus = multiprocessing.cpu_count()
#     # Start a LocalCluster that has 1 thread per process
#     # processes=True very important
#     cluster = dask.distributed.LocalCluster(processes=True, n_workers=num_cpus, threads_per_worker=1)

#     # Connect to the local cluster.
#     # We store the connection in the client variable, but we shouldn't need that variable anymore.
#     _ = dask.distributed.Client(cluster)

BASE_COLUMNS = [
    "nomvar",
    "typvar",
    "etiket",
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
    "datev",
    "grid",
    "d",
]


from . import *
from .apply_mask import *
from .csv_reader import *
from .csv_writer import *
from .dataframe import *
from .dataframe_utils import *
from .log import *
from .quick_pressure import *
from .recover_mask import *
from .std_dec import *
from .std_enc import *
from .std_grid import *
from .std_io import *
from .std_reader import *
from .std_vgrid import *
from .std_writer import *
from .unit_helpers import *
from .utils import *
