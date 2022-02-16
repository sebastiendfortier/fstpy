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
from fstpy import std_enc


BASE_COLUMNS = ['nomvar','level','typvar','etiket','dateo','ip1','ip2','ip3','deet','npas','datyp','nbits','ig1','ig2','ig3','ig4','d']
IP1_KIND = 3
#key–Positioning information to the record. Obtained with fstinf or fstinl.
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

    def __init__(self,path):
        if os.path.exists(path):
        #path = '/home/zak000/src/notebooks/readerCsv_notebook/test2_src.csv'
            self.path = path
        else:
            raise CsvFileReaderError('Path does not exist\n')

    def to_pandas(self)-> pd.DataFrame:
        self.df = pd.read_csv(self.path,comment="#")
        if(self.verifyHeaders()):
            self.checkColumns()
            return self.df
            
    def to_pandas_no_condition(self):
        self.df = pd.read_csv(self.path,comment="#")
        return self.df

    def to_pandas_no_hdr(self):
        self.df = pd.read_csv(self.path,comment="#",header=None)
        if(self.verifyHeaders()):
            return self.df
            


    def verifyHeaders(self):
        return self.hasHeader() and self.hasMinimumHeaders() and self.checkHeadersAllValid()
    
    def checkColumns(self):
        self.checkNbits()
        self.checkDatyp()
        self.checkTypVar()
        self.checkIp2EtIp3()
        self.checkIg()
        self.checkEticket()
        self.checkLevel()
    
    
    """Verify that the csv file has a header
    """
    def hasHeader(self):
            if(self.df.columns.dtype == object):
                return True
            else:
                raise NoHeaderInFileError('Your file does not have a csv file with headers')

    def hasMinimumHeaders(self):
        """_summary_

        :raises MinimumHeadersError: _description_
        :return: _description_
        :rtype: _type_

        """

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
        sorted(set1)
        sorted(set2)

        if(len(list_of_hdr_names) < len(BASE_COLUMNS)):
            is_subset = set1.issubset(set2)
            if(is_subset):
                return True
            else:
                raise HeadersAreNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS')
        if(len(list_of_hdr_names) == len(BASE_COLUMNS)):
            if(all_the_cols == list_of_hdr_names):
                return True
            else:
                raise HeadersAreNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS')
        else:
            raise HeadersAreNotValidError('The headers in the csv file are not valid you have too many columns')

    def colExists(self,col):
        if col in self.df.columns:
            return True
        else:
            return False



    # DEFAULT VALUES
    
    def checkDataColumn(self):
        pass

    # Remplace toutes les valeurs nbits nulles par 24 en int
    def checkNbits(self):
        if(not self.colExists("nbits")):
            self.df["nbits"] = 24 # le int est 64 bits non 32
        return self.df
        

    def checkDatyp(self):
        if(not self.colExists("datyp")):
            self.df["datyp"] = 1
        return self.df

           

    def checkTypVar(self):
        if(not self.colExists("typvar")):
            self.df["typvar"] = "X"
        return self.df

            

    def checkDateO(self):
        dateo_encoded = std_enc.create_encoded_dateo(datetime.utcnow())
        if(not self.colExists("dateo")):
            self.df["dateo"] = dateo_encoded
        return self.df
            

    #def checkIp1(self):
     #   if(self.colExists("ip1")):
       #     self.df.ip1.replace(np.nan,3).apply(np.int32)
      #  else:
      #      self.df["ip1"] = 3

    def checkIp2EtIp3(self):
        if(not self.colExists("ip2")):
            self.df["ip2"] = 0
        if(not self.colExists("ip3")):
            self.df["ip3"] = 0
        else:
            return self.df
        return self.df

    def checkIg(self):
        if(not self.colExists("ig1")):
            self.df["ig1"] = 0        

        if(not self.colExists("ig2")):
            self.df["ig2"] = 0

        if(not self.colExists("ig3")):
            self.df["ig3"] = 0

        if(not self.colExists("ig4")):
            self.df["ig4"] = 0

        return self.df
    
    def checkEticket(self):
        if(not self.colExists("etiket")):
            self.df["eticket"] = "CSVREADER"
        return self.df

    def checkLevel(self):
        if(self.colExists("level") and not self.colExists("ip1")):
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = std_enc.create_encoded_ip1(level=level,ip1_kind=IP1_KIND)
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
            self.to_numpy_array(self)
            pass

        elif isinstance(self.array,np.array):
            self.to_str_array(self)
            pass

        else:
            raise ArrayIsNotNumpyStrError('The array you provided does not contains strings or numpy arrays')

    def to_numpy_array(self):
        array_list = []

        for row in self.df.itertuples():
            #print(row)
            self.array= row.d
            a = np.array([[float(j) for j in i.split(',')] for i in array.split(';')],dtype=np.float32, order='F')
            array_list.append(a)
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

            self.df.at[row.Index,"ni"] = ni
            self.df.at[row.Index,"nj"] = nj
            self.df.at[row.Index,"nk"] = nk
        self.df["d"] = array_list

    def to_str_array(self):
        for row in self.df.itertuples(): 
            self.array = row.d 
            a = array.tostring
        pass







        


        
             







