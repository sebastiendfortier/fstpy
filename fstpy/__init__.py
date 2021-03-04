import sys
import pandas as pd
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
                    133:'e',
                    134:'f'
                }

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
                }

_const_prefix='/'.join(__file__.split('/')[0:-1])
_csv_path = _const_prefix + '/csv/'
_vctypes = pd.read_csv(_csv_path + 'verticalcoordinatetypes.csv')
_stdvar = pd.read_csv(_csv_path + 'stdvar.csv')
_units = pd.read_csv(_csv_path + 'units.csv')
_etikets = pd.read_csv(_csv_path + 'etiket.csv')
_leveltypes = pd.read_csv(_csv_path + 'leveltype.csv')
_thermoconstants = pd.read_csv(_csv_path + 'thermo_constants.csv')
VCTYPES = _vctypes
STDVAR = _stdvar
UNITS = _units
ETIKETS = _etikets
LEVELTYPES = _leveltypes
THERMO_CONSTANTS = _thermoconstants

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
    
# #expose some functions and classes
# from .std_reader import StandardFileReader,load_data
# from .std_writer import StandardFileWriter
# from .dataframe_utils import fstcomp,voir,fststat,zap,select
# from .utils import delete_file,get_groups,flatten_data_series,create_1row_df_from_model
# from .std_io import get_grid_metadata_fields,get_lat_lon
# from .std_dec import convert_rmndate_to_datetime
# from .unit import do_unit_conversion
# from .constants import STDVAR,DATYP_DICT

