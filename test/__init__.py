# -*- coding: utf-8 -*-
import os

HOST_NUM = os.getenv("TRUE_HOST")[-1]
USER = os.getenv("USER")
SPOOKI_TMPDIR = os.getenv("SPOOKI_TMPDIR")
TEST_PATH = "/fs/site%s/eccc/cmd/w/spst900/spooki/spooki_dir/pluginsRelatedStuff/"%HOST_NUM
TMP_PATH = SPOOKI_TMPDIR if SPOOKI_TMPDIR else "/home/spst900/spooki/spooki_tmpdir_ppp%s/%s"%(HOST_NUM,USER)
