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

# try:
#     import pandas as pd
# except ImportError:
#     sys.stderr.write('pandas is required')
#     error=1

# if pd.__version__ < '1.1.5':
#     sys.stderr.write('pandas>=1.1.5 is required')  
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

#expose some functions and classes
from .std_reader import StandardFileReader,load_data
from .std_io import get_meta_data_fields,get_lat_lon
