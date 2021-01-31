import sys

__version__ = '1.0.1'

if sys.version_info[:2] < (3,6):
    sys.exit("Wrong python version, python>=3.6")
