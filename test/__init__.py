# -*- coding: utf-8 -*-
from fstpy.log import setup_fstpy_logger
import os

logger = setup_fstpy_logger()

DEFAULT_HOST_NUM = 3
HOST_NUM = os.getenv("FSTPY_HOST_NUM", None)

if HOST_NUM is not None:
    HOST_NUM = int(HOST_NUM)
else:
    HOST_NUM = os.getenv("TRUE_HOST")[-1]
    try:
        HOST_NUM = int(HOST_NUM)
    except ValueError as ve:
        HOST_NUM = DEFAULT_HOST_NUM

USER = os.getenv("USER")
SPOOKI_TMPDIR = os.getenv("SPOOKI_TMPDIR")
TMPDIR = os.getenv("TMPDIR")
TEST_PATH = "/fs/site%d/eccc/cmd/w/spst900/spooki/spooki_dir/pluginsRelatedStuff/" % HOST_NUM
TMP_PATH = SPOOKI_TMPDIR if SPOOKI_TMPDIR else TMPDIR
