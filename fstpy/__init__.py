import sys

__version__ = '1.0.1'

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
def get_file_list(pattern):
    import glob
    files = glob.glob(pattern)
    return files

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

def column_descriptions():
    import sys
    sys.stdout.write('nomvar: variable name')
    sys.stdout.write('typvar: type of field ([F]orecast, [A]nalysis, [C]limatology)')
    sys.stdout.write('etiket: concatenation of label, run, implementation and ensemble_member')
    sys.stdout.write('ni: first dimension of the data field - relates to shape')
    sys.stdout.write('nj: second dimension of the data field - relates to shape')
    sys.stdout.write('nk: third dimension of the data field - relates to shape')
    sys.stdout.write('dateo: date of observation time stamp')
    sys.stdout.write('ip1: encoded vertical level')
    sys.stdout.write('ip2: encoded forecast hour, but can be used in other ways by encoding an ip value')
    sys.stdout.write('ip3: user defined identifier')
    sys.stdout.write('deet: length of a time step in seconds - usually invariable - relates to model ouput times')
    sys.stdout.write('npas: time step number')
    sys.stdout.write('datyp: data type of the elements (int,float,str,etc)')
    sys.stdout.write('nbits: number of bits kept for the elements of the field (16,32,etc)')
    sys.stdout.write('ig1: first grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig2: second grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig3: third grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('ig4: fourth grid descriptor, helps to associate >>, ^^, !!, HY, etc with variables')
    sys.stdout.write('grtyp: type of geographical projection identifier (Z, X, Y, etc)')
    sys.stdout.write('datev: date of validity (dateo + deet * npas) Will be set to -1 if dateo invalid')
    sys.stdout.write('d: data associated to record, empty until data is loaded - either a numpy array or a daks array for one level of data')
    sys.stdout.write('key: key/handle of the record - used by rpnpy to locate records in a file')
    sys.stdout.write('shape: (ni, nj, nk) dimensions of the data field - an attribute of the numpy/dask array (array.shape)')
    
    

#expose some functions and classes
from .std_reader import StandardFileReader,load_data
from .std_writer import StandardFileWriter
from .dataframe_utils import fstcomp,voir,fststat,zap,select
from .utils import delete_file
from .std_io import get_grid_metadata_fields,get_lat_lon
