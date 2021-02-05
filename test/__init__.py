# -*- coding: utf-8 -*-
import os, sys
#sys.path.append("/".join(os.getcwd().split("/")[0:-1]))
HOST_NUM = os.getenv("TRUE_HOST")[-1]
USER = os.getenv("USER")
TEST_PATH = "/fs/site%s/eccc/cmd/w/spst900/spooki/spooki_dir/pluginsRelatedStuff/"%HOST_NUM
TMP_PATH = "/fs/site%s/eccc/cmd/w/%s/spooki_tmpdir/"%(HOST_NUM,USER)