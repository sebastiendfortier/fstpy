# -*- coding: utf-8 -*-
from array import array
import copy
import itertools
import multiprocessing as mp
from mimetypes import init
import re
import pandas as pd
import os
import numpy as np
from .utils import initializer
import csv
import datetime


BASE_COLUMNS = ['nomvar','level','typvar', 'etiket', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'grid', 'd']
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
class ArrayIsNotNumpyStrError(Exception):
    pass
class ArrayIs3dError(Exception):
    pass
class NoHeaderError(Exception):
    pass
class MinimumHeadersError(Exception):
    pass
class HeadersAreNotValidError(Exception):
    pass
class NoHeaderInFileError(Exception):
    pass


class CsvFileReader :   
    @initializer
    def __init__(self,path):
        if os.path.exists(path):
        #path = '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'
            self.df = pd.read_csv(path,comment="#")
        else:
            raise CsvFileReaderError('Path must not exist\n')
        pass

    def to_pandas(self) -> pd.DataFrame:
        pass  

class CsvArray:
    def __init__(self,array):
        self.array=array
        if isinstance(self.array,str):
            self.to_array(self)
            pass

        elif isinstance(self.array,np.array):
            self.from_array(self)
            pass

        else:
            raise ArrayIsNotNumpyStrError('The array you provided does not contains strings or numpy arrays')

    def to_array(self):
        for row in self.df.itertuples():
            #print(row)
            self.array= row.d
            a = np.array([[float(j) for j in i.split(',')] for i in array.split(';')],dtype=np.float32, order='F')
            # print("array = " + f'{a}')
            if(a.ndim == 1):
                ni = np.shape(a)[0]
                nj=0
                nk=1
            if(a.ndim == 2):
                ni = np.shape(a)[0] 
                nj = np.shape(a)[1]
                nk = 1

            if(a.ndim == 3):
                raise ArrayIs3dError('The numpy array you created from the string array is 3D and it should not be 3d')
                pass

            self.df.at[row.Index,"ni"] = ni
            self.df.at[row.Index,"nj"] = nj
            self.df.at[row.Index,"nk"] = nk
            pass

    def from_array(self):
        pass

    def hasHeader(self):
            self.sniffer = csv.Sniffer()
            self.sample_bytes = 32
            return self.sniffer.has_header(
                open(self.path).read(self.sample_bytes))

    def hasMinimumHeaders(self):
        if(self.hasHeader):
            list_of_hdr_names = self.df.columns.tolist()
            if 'nomvar' and 'd' and 'level' in list_of_hdr_names:
                return True
            else:
                raise MinimumHeadersError('Your csv file doesnt have the necessary columns to proceed! Check that you '
                                                    + 'have at least nomvar,d and level as columns in your csv file')
        else:
            raise NoHeaderInFileError('Your file does not have a csv file with headers')

    def checkHeadersAllValid(self):
        all_the_cols = BASE_COLUMNS
        all_the_cols.sort()

        list_of_hdr_names = self.df.columns.tolist()
        list_of_hdr_names.sort()

        set1 = set(list_of_hdr_names)
        set2 = set(BASE_COLUMNS)
        if(self.hasMinimumHeaders):
            if(len(list_of_hdr_names) < 19 and self.hasMinimumHeaders):
                is_subset = set1.issubset(set2)
                return is_subset

            if(len(list_of_hdr_names) == 19 and self.hasMinimumHeaders):
                return all_the_cols == list_of_hdr_names

            if(len(list_of_hdr_names) > 19):
                raise HeadersAreNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS or you have too many columns')
        else:
            print("Should not print!")



    def colExists(self,col):
        if col in self.df.columns:
            return True
        else:
            return False


    # DEFAULT VALUES
    # Probablement va etre ds writer
    # Remplace toutes les valeurs nbits nulles par 24 en int
    def checkNbits(self):
        if(self.colExists("nbits")):
            self.df.nbits.replace(np.nan,24).apply(np.int64)

    def checkDatyp(self):
        if(self.colExists("datyp")):
            self.df.datyp.replace(np.nan,1).apply(np.int64)

    def checkGrTyp(self):
        if(self.colExists("grtyp")):
            self.df.grtyp.replace(np.nan,"X")

    def checkDateO(self):
        if(self.colExists("dateo")):
            self.df.dateo.replace(np.nan,datetime.utcnow())

    def checkIp1(self):
        if(self.colExists("ip1")):
            self.df.ip1.replace(np.nan,3).apply(np.int64)

    def checkIp2EtIp3(self):
        if(self.colExists("ip2")):
            self.df.ip2.replace(np.nan,0).apply(np.int64)
        if(self.colExists("ip3")):
            self.df.ip3.replace(np.nan,0).apply(np.int64)

    def checkIg(self):
        if(self.colExists("ig1")):
            self.df.ig1.replace(np.nan,0).apply(np.int64)

        if(self.colExists("ig2")):
            self.df.ig2.replace(np.nan,0).apply(np.int64)

        if(self.colExists("ig3")):
            self.df.ig3.replace(np.nan,0).apply(np.int64)

        if(self.colExists("ig4")):
            self.df.ig4.replace(np.nan,0).apply(np.int64)
    
    def checkEticket(self):
        if(self.colExists("etiket")):
            self.df.eticket.replace(np.nan,"CSVREADER")

    
    

    
    






        


        
             







