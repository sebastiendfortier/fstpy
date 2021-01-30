import rpnpy.librmn.all as rmn
import logging
from log.log import setup_custom_logger

logger = setup_custom_logger('root')
logger = logging.getLogger('root')

rmn.fstopt(rmn.FSTOP_MSGLVL, rmn.FSTOPI_MSG_CATAST, setOget=0)