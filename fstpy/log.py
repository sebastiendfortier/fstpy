# -*- coding: utf-8 -*-
import logging
# from logging.config import dictConfig
import sys

# logging_config = dict(
#     version = 1,
#     formatters = {
#         'f':
#         {
#             'format': '[%(asctime)s.%(msecs)03d] [%(levelname)-7s] %(message)s',
#             'datefmt' :'%H:%M:%S'
#         },
#     },
#     handlers = {
#         'stream': {'class': 'logging.StreamHandler',
#             'formatter': 'f',
#             'level': logging.DEBUG},
#         },
#     root = {
#         'handlers': ['stream'],
#         'level': logging.DEBUG,
#         },
# )
# dictConfig(logging_config)




def setup_custom_logger():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format='[%(asctime)s.%(msecs)03d] [%(levelname)-7s] %(message)s',datefmt='%H:%M:%S')
    logger = logging.getLogger('root')
    return logger
