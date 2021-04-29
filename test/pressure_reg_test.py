from test import TEST_PATH, TMP_PATH

import pytest
from fstpy.dataframe_utils import fstcomp, select, select_zap, zap
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.utils import delete_file
from rpnpy.librmn.all import FSTDError

pytestmark = [pytest.mark.std_reader_regtests, pytest.mark.regressions]


