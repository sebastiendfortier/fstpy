# -*- coding: utf-8 -*-
import sys
import pandas as pd
from .fstcompstats import fstcompstats
# __version__ = '1.0.1'

error = 0
if sys.version_info[:2] < (3, 6):
    sys.exit("Wrong python version, python>=3.6")

try:
    import rpnpy.librmn.all as rmn
except ImportError:
    sys.stdout.write('rpnpy.librmn.all is required')
    error=1

# try:
#     import pandas as pd
# except ImportError:
#     sys.stdout.write('pandas is required')
#     error=1

# if pd.__version__ < '1.1.5':
#     sys.stdout.write('pandas>=1.1.5 is required')
#     error=1

if error == 1:
    sys.exit('Requirements not met')

# https://wiki.cmc.ec.gc.ca/wiki/Python-RPN/2.1/rpnpy/librmn/fstd98#fstopt
rmn.fstopt(rmn.FSTOP_MSGLVL, rmn.FSTOPI_MSG_CATAST, setOget=0)

# KIND_ABOVE_SEA = 0
# KIND_SIGMA     = 1
# KIND_PRESSURE  = 2
# KIND_ARBITRARY = 3
# KIND_ABOVE_GND = 4
# KIND_HYBRID    = 5
# KIND_THETA     = 6
# KIND_HOURS     = 10
# KIND_SAMPLES   = 15
# KIND_MTX_IND   = 17
# KIND_M_PRES    = 21
# kind    : level_type
#  0       : m  [metres] (height with respect to sea level)
#   [pressure in metres]

#'/fs/site3/eccc/ops/cmod/prod/hubs/suites/ops/rdps_20191231/r1/gridpt/prog/hyb/2021021012_*'




DATYP_DICT = {
                    0:'X',
                    1:'R',
                    2:'I',
                    4:'S',
                    5:'E',
                    6:'F',
                    7:'A',
                    8:'Z',
                    130:'i',
                    132:'s',
                    133:'e',
                    134:'f'
                } #: :meta hide-value:
"""data type aliases constant

:return: correspondance betweeen datyp and str version of datyp
:rtype: dict
:meta hide-value:
"""

KIND_DICT = {
                    -1:'_',
                    0: 'm',   #[metres] (height with respect to sea level)
                    1: 'sg',  #[sigma] (0.0->1.0)
                    2: 'mb',  #[mbars] (pressure in millibars)
                    3: '   ', #[others] (arbitrary code)
                    4: 'M',   #[metres] (height with respect to ground level)
                    5: 'hy',  #[hybrid] (0.0->1.0)
                    6: 'th',  #[theta]
                    10: 'H',  #[hours]
                    15: '  ', #[reserved, integer]
                    17: ' ',  #[index X of conversion matrix]
                    21: 'mp'  #[pressure in metres]
                } #: :meta hide-value:
"""kind aliases constant

:return: correspondance betweeen kind and str version of kind
:rtype: dict
:meta hide-value:
"""

_const_prefix='/'.join(__file__.split('/')[0:-1])
_csv_path = _const_prefix + '/csv/'
_stationsfb = pd.read_csv(_csv_path + 'stationsfb.csv')
_vctypes = pd.read_csv(_csv_path + 'verticalcoordinatetypes.csv')
_stdvar = pd.read_csv(_csv_path + 'stdvar.csv')
_units = pd.read_csv(_csv_path + 'units.csv'
,dtype=
{
    'name':str,
    'symbol':str,
    'expression':str,
    'bias':'float32',
    'factor':'float32',
    'mass':'int32',
    'length':'int32',
    'time':'int32',
    'electricCurrent':'int32',
    'temperature':'int32',
    'amountOfSubstance':'int32',
    'luminousIntensity':'int32'
    })
_etikets = pd.read_csv(_csv_path + 'etiket.csv')
_leveltypes = pd.read_csv(_csv_path + 'leveltype.csv')
_thermoconstants = pd.read_csv(_csv_path + 'thermo_constants.csv')

STATIONSFB = _stationsfb #: :meta hide-value:
"""FB stations table

:return: correspondance betweeen datyp and str version of datyp
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

VCTYPES = _vctypes #: :meta hide-value:
"""virtical coordinate type information table

:return: correspondance betweeen datyp and str version of datyp
:rtype: pd.DataFrame
:meta hide-value:
>>> fstpy.VCTYPES
    ip1_kind  toctoc     P0     E1     PT     HY     SF  vcode                vctype
0          5    True   True  False  False  False  False   5002      HYBRID_STAGGERED
1          5    True   True  False  False  False  False   5001                HYBRID
2          5    True   True  False  False  False  False   5005           HYBRID_5005
...
"""
STDVAR = _stdvar #: :meta hide-value:
"""Like the o.dict standard file dictionnary table

:return: dataframe
:rtype: pd.DataFrame
:meta hide-value:
>>> fstpy.STDVAR
    nomvar                                     description_fr                        description_en              unit
0       !!                 Descripteur de coordonnée vericale        Vertical coordinate descriptor            scalar
1       ++                           Réservé pour usage futur               Reserved for Future Use            scalar
2       1A                                     Albedo visible                        Visible Albedo           percent
3       1P                           Pression à la tropopause                   Tropopause pressure       hectoPascal
4       1T                       Température de la tropopause         Temperature at the tropopause           celsius
..     ...                                                ...                                   ...               ...
923    ZVC  hauteur du NCL utilisant le temperature virtuelle  LFC height using virtual temperature             meter
924    ZVE   hauteur du NE utilisant le temperature virtuelle   EL height using virtual temperature             meter
925     ZZ                 Mouvement vertical en coordonnée Z        Vertical Motion (Z Coordinate)  meter_per_second
926     ^^          Position verticale dans une Grille Y ou Z    Vertical position in a Y or Z grid            scalar
927     ^>        Position horizontale dans une grille Y ou Z  Horizontal position in a Y or Z grid            scalar

[928 rows x 4 columns]
"""
UNITS = _units #: :meta hide-value:
"""Units table for conversions

:return: dataframe
:rtype: pd.DataFrame
:meta hide-value:
>>> fstpy.UNITS
                       name      symbol               expression  bias        factor  mass  length  time  electricCurrent  temperature  amountOfSubstance  luminousIntensity
0                  kilogram          kg                       kg   0.0  1.000000e+00     1       0     0                0            0                  0                  0
1     kilogram_per_kilogram       kg/kg                    kg/kg   0.0  1.000000e+00     0       0     0                0            0                  0                  0
2             gram_per_gram         g/g                    kg/kg   0.0  1.000000e+00     0       0     0                0            0                  0                  0
3         gram_per_kilogram        g/kg                    kg/kg   0.0  1.000000e-03     0       0     0                0            0                  0                  0
4         kilogram_per_gram        kg/g                    kg/kg   0.0  1.000000e+03     0       0     0                0            0                  0                  0
..                      ...         ...                      ...   ...           ...   ...     ...   ...              ...          ...                ...                ...
153                radiance   W/(m2·sr)         W·m^(-2)·sr^(-1)   0.0  1.000000e+00     1       0    -3                0            0                  0                  0
154  catalyticConcentration      kat/m3               kat·m^(-3)   0.0  1.000000e+00     0      -3    -1                0            0                  1                  0
155       pascal_per_second        Pa/s          N·m^(-2)·s^(-1)   0.0  1.000000e+00     1      -1    -3                0            0                  0                  0
156     millimeter_per_hour        mm/h                 m·s^(-1)   0.0  2.777778e-07     0       1    -1                0            0                  0                  0
157  potentialVorticityUnit  Km2/(kg·s)  K·m^(2)·(kg^(-1)·s(-1))   0.0  1.000000e+00    -1       2    -1                0            1                  0                  0

[158 rows x 12 columns]
"""
# ETIKETS = _etikets #: :meta hide-value:
# """Etikets table

# :return: dataframe
# :rtype: pd.DataFrame
# :meta hide-value:
# >>> fstpy.ETIKETS
#                 plugin_name   etiket
# 0             AbsoluteValue   ABSVAL
# 1        AddElementsByPoint   ADDEPT
# 2     AddElementsVertically   ADDEVY
# 3             AddToElements   ADDTOE
# 4                AirDensity   AIRDEN
# ..                      ...      ...
# 81          WindDeformation  WINDDEF
# 82              WindModulus   WNDMOD
# 83  WindModulusAndDirection   WNDMAD
# 84       WindTurbulenceDVSI   WNDVSI
# 85        WindVerticalShear   WNDVSH

# [86 rows x 2 columns]
# """
LEVELTYPES = _leveltypes #: :meta hide-value:
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

def get_constant_row_by_name(df:pd.DataFrame, df_name:str, index:str, name:str) -> pd.Series:
    row = df.loc[df[index] == name]
    if len(row.index):
        return row
    #else:
    #    logger.warning('get_constant_row_by_name - %s - no %s found by that name'%(name,df_name))
        #logger.error('get_constant_row_by_name - available %s are:'%df_name)
        #logger.error(pprint.pformat(sorted(df[index].to_list())))
    return pd.Series(dtype=object)

def get_unit_by_name(name:str) -> pd.Series:
    unit = get_constant_row_by_name(UNITS, 'unit', 'name', name)
    if len(unit.index):
        return unit
    else:
        return get_constant_row_by_name(UNITS, 'unit', 'name', 'scalar')

def get_etikey_by_name(name:str) -> pd.Series:
    return get_constant_row_by_name(ETIKETS, 'etiket', 'plugin_name', name)

def get_constant_by_name(name:str) -> pd.Series:
    return get_constant_row_by_name(THERMO_CONSTANTS, 'value', 'name', name)

def get_column_value_from_row(row, column):
    return row[column].values[0]
