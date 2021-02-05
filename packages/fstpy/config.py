# -*- coding: utf-8 -*-

import logging
from .log import setup_custom_logger


#logger = setup_custom_logger('root')
# start logging with this function
#logger = logging.getLogger('root')
logger = logging.getLogger('dummy')
logger.setLevel(logging.CRITICAL)
#disable logger
logger.propagate = False