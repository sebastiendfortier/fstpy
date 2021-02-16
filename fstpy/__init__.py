import sys

__version__ = '1.0.1'

error = 0
if sys.version_info[:2] < (3, 6):
    sys.exit("Wrong python version, python>=3.6")

try:
    import rpnpy.librmn.all as rmn
except ImportError:
    sys.stderr.write('rpnpy.librmn.all is required')
    error=1    

try:
    import pandas as pd
except ImportError:
    sys.stderr.write('pandas is required')
    error=1

if pd.__version__ < '1.1.5':
    sys.stderr.write('pandas>=1.1.5 is required')  
    error=1

if error == 1:
    sys.exit('Requirements not met')
    
# fstopt(optName, optValue, setOget=0):
#    '''
#    Set or print FST option.

#    fstopt(optName, optValue)
#    fstopt(optName, optValue, setOget)

#    Args:
#        optName  : name of option to be set or printed
#                   or one of these constants:
#                   FSTOP_MSGLVL, FSTOP_TOLRNC, FSTOP_PRINTOPT, FSTOP_TURBOCOMP
#                   FSTOP_FASTIO, FSTOP_IMAGE, FSTOP_REDUCTION32
#        optValue : value to be set (int or string)
#                   or one of these constants:
#                   for optName=FSTOP_MSGLVL:
#                      FSTOPI_MSG_DEBUG,   FSTOPI_MSG_INFO,  FSTOPI_MSG_WARNING,
#                      FSTOPI_MSG_ERROR,   FSTOPI_MSG_FATAL, FSTOPI_MSG_SYSTEM,
#                      FSTOPI_MSG_CATAST
#                   for optName=FSTOP_TOLRNC:
#                      FSTOPI_TOL_NONE,    FSTOPI_TOL_DEBUG, FSTOPI_TOL_INFO,
#                      FSTOPI_TOL_WARNING, FSTOPI_TOL_ERROR, FSTOPI_TOL_FATAL
#                   for optName=FSTOP_TURBOCOMP:
#                      FSTOPS_TURBO_FAST, FSTOPS_TURBO_BEST
#                   for optName=FSTOP_FASTIO, FSTOP_IMAGE, FSTOP_REDUCTION32:
#                      FSTOPL_TRUE, FSTOPL_FALSE
#        setOget  : define mode, set or print/get
#                   one of these constants: FSTOP_SET, FSTOP_GET
#                   default: set mode
#    Returns:
#        None
#    Raises:
#        TypeError  on wrong input arg types
#        ValueError on invalid input arg value
#        FSTDError  on any other error
#    '''
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
def get_file_list(pattern):
    import glob
    files = glob.glob(pattern)

def ip1_from_level_and_kind(level:float,kind:str):
    d = {
        'm':0,
        'sg':1,
        'mb':2,
        'M':4,
        'hy':5,
        'th':6,
        'H':10,
        'mp':21
    }
    pk =  rmn.listToFLOATIP((level, level, d[kind.strip()]))
    (ip1, _, _) = rmn.convertPKtoIP(pk,pk,pk)
    return ip1

#expose some functions and classes
from .std_reader import StandardFileReader,load_data,stack_arrays
from .std_io import get_meta_data_fields,get_lat_lon
from .utils import create_1row_df_from_model
from .std_dec import decode_ip1
from .dataframe_utils import compute_stats