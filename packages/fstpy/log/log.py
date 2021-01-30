# -*- coding: utf-8 -*-
import logging
from logging.config import dictConfig

logging_config = dict(
    version = 1,
    formatters = {
        'f': 
        {
            'format': '[%(asctime)s.%(msecs)03d] [%(levelname)-7s] %(message)s',
            'datefmt' :'%H:%M:%S'
        },
        
        
    },
    handlers = {
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.DEBUG}
        },
    root = {
        'handlers': ['h'],
        'level': logging.DEBUG,
        },
)
dictConfig(logging_config)

def setup_custom_logger(name):
    logger = logging.getLogger(name)
    return logger
    