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
import std_enc


BASE_COLUMNS = ['nomvar','level','typvar', 'etiket', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'ig1', 'ig2', 'ig3', 'ig4', 'd']
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

class NoHeaderError(Exception):
    pass
class MinimumHeadersError(Exception):
    pass
class HeadersAreNotValidError(Exception):
    pass
class NoHeaderInFileError(Exception):
    pass
class ip1HasAmissingValueError(Exception):
    pass
class ip1andLevelExistsError(Exception):
    pass

class CsvFileReader :   
    @initializer
    def __init__(self,path):
        if os.path.exists(path):
        #path = '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'
            self.df = pd.read_csv(path,comment="#")
        else:
            raise CsvFileReaderError('Path does not exist\n')
        pass

    def to_pandas(self) -> pd.DataFrame:
        pass  

    def verificationHeaders(self):
        return self.hasHeader() and self.hasMinimumHeaders() and self.checkHeadersAllValid()
    
    def checkColumns(self):
        return self.checkNbits() and self.checkDatyp() and self.checkDateO() and self.checkEticket() and self.checkGrTyp() and self.checkIg() and self.checkIp2EtIp3() and self.checkLevel()

    def hasHeader(self):
            self.sniffer = csv.Sniffer()
            self.sample_bytes = 32
            if(self.sniffer.has_header(open(self.path).read(self.sample_bytes))):
                return True
            else:
                raise NoHeaderInFileError('Your file does not have a csv file with headers')

    def hasMinimumHeaders(self):
        list_of_hdr_names = self.df.columns.tolist()
        if set(['nomvar', 'd','level']).issubset(list_of_hdr_names) or set(['nomvar', 'd','ip1']).issubset(list_of_hdr_names):
                return True
        else:
            raise MinimumHeadersError('Your csv file doesnt have the necessary columns to proceed! Check that you '
                                        + 'have at least nomvar,d and level or ip1 as columns in your csv file')

    def checkHeadersAllValid(self):
        all_the_cols = BASE_COLUMNS
        all_the_cols.sort()

        list_of_hdr_names = self.df.columns.tolist()
        list_of_hdr_names.sort()

        set1 = set(list_of_hdr_names)
        set2 = set(BASE_COLUMNS)

        if(len(list_of_hdr_names) < len(BASE_COLUMNS)):
            is_subset = set1.issubset(set2)
            return is_subset

        if(len(list_of_hdr_names) == len(BASE_COLUMNS)):
            return all_the_cols == list_of_hdr_names

        else:
            raise HeadersAreNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS or you have too many columns')

    def colExists(self,col):
        if col in self.df.columns:
            return True
        else:
            return False

    # DEFAULT VALUES
    # Remplace toutes les valeurs nbits nulles par 24 en int
    def checkNbits(self):
        if(self.colExists("nbits")):
            self.df.nbits.replace(np.nan,24).apply(np.int64)
        else:
            self.df["nbits"] = 24
        

    def checkDatyp(self):
        if(self.colExists("datyp")):
            self.df.datyp.replace(np.nan,1).apply(np.int64)
        else:
            self.df["datyp"] = 1

    def checkGrTyp(self):
        if(self.colExists("grtyp")):
            self.df.grtyp.replace(np.nan,"X")
        else:
            self.df["grtyp"] = "X"

    def checkDateO(self):
        dateo_encoded = std_enc.create_encoded_dateo(datetime.utcnow())
        if(self.colExists("dateo")):
            self.df.dateo.replace(np.nan,dateo_encoded)
        else:
            self.df["dateo"] = dateo_encoded

    #def checkIp1(self):
     #   if(self.colExists("ip1")):
       #     self.df.ip1.replace(np.nan,3).apply(np.int64)
      #  else:
      #      self.df["ip1"] = 3

    def checkIp2EtIp3(self):
        if(self.colExists("ip2")):
            self.df.ip2.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ip2"] = 0

        if(self.colExists("ip3")):
            self.df.ip3.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ip3"] = 0

    def checkIg(self):
        if(self.colExists("ig1")):
            self.df.ig1.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ig1"] = 0

        if(self.colExists("ig2")):
            self.df.ig2.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ig2"] = 0

        if(self.colExists("ig3")):
            self.df.ig3.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ig3"] = 0

        if(self.colExists("ig4")):
            self.df.ig4.replace(np.nan,0).apply(np.int64)
        else:
            self.df["ig4"] = 0
    
    def checkEticket(self):
        if(self.colExists("etiket")):
            self.df.eticket.replace(np.nan, "CSVREADER") 
        else:
            self.df["eticket"] = "CSVREADER"

    def checkLevel(self):
        if(self.colExists("level") and not self.colExists("ip1")):
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = std_enc.create_encoded_ip1(level=level,ip1_kind=3)
                self.df.at[row.Index,"ip1"] = ip1
        # Remove level after we added ip1 column
            self.df.drop(["level"],axis = 1,inplace = True)
        else:
            raise ip1andLevelExistsError("IP1 AND LEVEL EXISTS IN THE CSV FILE")
    
            
        

class ArrayIsNotNumpyStrError(Exception):
    pass

class ArrayIs3dError(Exception):
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







        


        
             







