# -*- coding: utf-8 -*-
import copy
import itertools
import multiprocessing as mp
from mimetypes import init
import pandas as pd
import os
import numpy as np
from .utils import initializer


#BASE_COLUMNS = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas','datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'datev', 'grid', 'd']
#key – Positioning information to the record. Obtained with fstinf or fstinl.
#dateo – date of origin (date time stamp) Cannot change dateo and datev.
#datev – date of validity (date time stamp) Cannot change dateo and datev.
#deet – Length of a time step in seconds datev constant unless keep_dateo
#npas – time step number datev constant unless keep_dateo
#ni – first dimension of the data field
#nj – second dimension of the data field
#nk – third dimension of the data field
#nbits – number of bits kept for the elements of the field
#datyp – data type of the elements
#ip1 – vertical level
#ip2 – forecast hour
#ip3 – user defined identifier
#typvar – type of field (forecast, analysis, climatology)
#nomvar – variable name
#etiket – label
#grtyp – type of geographical projection
#ig1 – first grid descriptor
#ig2 – second grid descriptor
#ig3 – third grid descriptor
#ig4 – fourth grid descriptor/

class CsvFileReaderError(Exception):
    pass


class CsvFileReader :
    
    @initializer
    def __init__(self,path):
        if os.path.exists(path):
        #path = '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'
            self.df = pd.read_csv(path)
        else:
            raise CsvFileReaderError('Path must not exist\n')
        pass

    def to_pandas(self) -> pd.DataFrame:
        pass  

    def reader_csv(self):
        pass

