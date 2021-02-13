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

rmn.fstopt(rmn.FSTOP_MSGLVL, rmn.FSTOPI_MSG_CATAST, setOget=0)

#expose some functions and classes
from .standardfile import StandardFileReader