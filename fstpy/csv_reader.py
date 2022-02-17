# -*- coding: utf-8 -*-
import copy
import itertools
import multiprocessing as mp
from mimetypes import init
import re
from this import d
import pandas as pd
import os
import numpy as np
from .utils import initializer
import csv
import datetime
from fstpy import std_enc
import rpnpy.librmn.all as rmn



BASE_COLUMNS = ['nomvar','level','typvar','etiket','dateo','ip1','ip2','ip3','deet','npas','datyp','nbits','ig1','ig2','ig3','ig4','d','datev']
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

class DimensionError(Exception):
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
        return self.hasHeader() and self.hasMinimumHeaders() and self.addHeadersAllValid()
    
    def checkColumns(self):
        self.add_n_bits()
        self.add_datyp()
        self.add_typ_var()
        self.add_ip2_ip3()
        self.add_ig()
        self.add_eticket()
        self.add_level()
        self.add_n_dimensions()
        self.add_deet()
        self.add_npas()
        self.add_dateo()
        self.add_datev()
        self.to_numpy_array()
        self.change_columns_type()
        self.check_dimension_meme_etiket()
    
    
    
    def hasHeader(self):
        """Verify that the csv file has a header"""
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

    def addHeadersAllValid(self):
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

    def col_exists(self,col):
        if col in self.df.columns:
            return True
        else:
            return False

    # DEFAULT VALUES
    def add_n_dimensions(self):
        for row in self.df.itertuples():
            array= row.d
            a = np.array([[float(j) for j in i.split(',')] for i in array.split(';')],dtype=np.float32, order='F')
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
        return self.df


    # Remplace toutes les valeurs nbits nulles par 24 en int
    def add_n_bits(self):
        if(not self.col_exists("nbits")):
            self.df["nbits"] = 24
    # le int est 64 bits non 32
        

    def add_datyp(self):
        if(not self.col_exists("datyp")):
            self.df["datyp"] = 1
     

    def add_typ_var(self):
        if(not self.col_exists("typvar")):
            self.df["typvar"] = "X"
     

    def add_dateo(self):
        dateo_encoded = std_enc.create_encoded_dateo(datetime.datetime.utcnow())
        if(not self.col_exists("dateo")):
            self.df["dateo"] = dateo_encoded

    def add_ip2_ip3(self):
        if(not self.col_exists("ip2")):
            self.df["ip2"] = 0
        if(not self.col_exists("ip3")):
            self.df["ip3"] = 0

    def add_ig(self):
        if(not self.col_exists("ig1")):
            self.df["ig1"] = 0        

        if(not self.col_exists("ig2")):
            self.df["ig2"] = 0

        if(not self.col_exists("ig3")):
            self.df["ig3"] = 0

        if(not self.col_exists("ig4")):
            self.df["ig4"] = 0
    
    def add_eticket(self):
        if(not self.col_exists("etiket")):
            self.df["eticket"] = "CSVREADER"
    
    def add_level(self):
        if(self.col_exists("level") and not self.col_exists("ip1")):
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = std_enc.create_encoded_ip1(level=level,ip1_kind=IP1_KIND,mode =rmn.CONVIP_ENCODE)
                self.df.at[row.Index,"ip1"] = ip1
            self.df["ip1"] = self.df["ip1"].astype("int32")
        # Remove level after we added ip1 column
            self.df.drop(["level"],axis = 1,inplace = True)
        else:
            raise ip1andLevelExistsError("IP1 AND LEVEL EXISTS IN THE CSV FILE")
    
    def add_deet(self):
        if(not self.col_exists("deet")):
            self.df["deet"] = 0

    def add_npas(self):
        if(not self.col_exists("npas")):
            self.df["npas"] = 0


    def add_datev(self):
        if(not self.col_exists("datev")):
            self.df["datev"] = None

    def check_dimension_meme_etiket(self):
        # Check if etiket is the same as the previous row to compare dimension if its the same etiket
        groups = self.df.groupby(['nomvar','typvar','etiket','dateo','ip1','ip2','ip3','deet','npas','datyp','nbits','ig1','ig2','ig3','ig4'])
        for _,df in groups:
            if df.ni.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")
            if df.nj.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")


    def to_numpy_array(self):
        array_list = []
        for i in self.df.index:
            a =CsvArray(self.df.at[i,"d"]).to_numpy()
            print(a)
            array_list.append(a)
        self.df["d"] = array_list


    def change_columns_type(self):
        self.df = self.df.astype({'ni':'int32','nj':'int32','nk':'int32','nomvar':"str",'typvar':'str','etiket':'str','dateo':'int32','ip1':'int32','ip2':'int32','ip3':'int32','datyp':'int32','nbits':'int32','ig1':'int32','ig2':'int32','ig3':'int32','ig4':'int32','deet':'int32','npas':'int32'})
    
    
class ArrayIsNotNumpyStrError(Exception):
    pass

class ArrayIs3dError(Exception):
    pass

class ArrayIsNotStringOrNp(Exception):
    pass


# 1- ta classe cvsarry ne doit rien faire a part initialiser ses variables d'instances dans le init
# dans tes methode to... tu verifie le type de self.array et tu retourne celui qui a rapport avec la methode
# to_str ... devrais faire un check sur le type de self.array, si c'est un numpy alors converti en str et 
# retourne la string, sinon retourne self.array
# dans ton csvReader
# for i in df.index:
#   df.at[i,'d'] = CsVArray(df.at[i,'d']).to_numpy...
class CsvArray:
    def __init__(self,array):
        self.array=array
        if(self.validate_array()):
            pass
        else:
            raise ArrayIsNotStringOrNp("The array is not a string or a numpy aray")

    def validate_array(self):
        #Verifier que larray est une string ou np.ndarray
        if(type(self.array) == np.ndarray or type(self.array) == str):
            return True
        else:
            return False
            

    def to_numpy(self):
        if isinstance(self.array,str):
            b = self.array
            a = np.array([[float(j) for j in i.split(',')] for i in b.split(';')],dtype=np.float32, order='F')
            if(a.ndim == 3):
                raise ArrayIs3dError('The numpy array you created from the string array is 3D and it should not be 3d')
            return a
        else:
            return self.array


    def to_str(self):
        if isinstance(self.array,np.ndarray):
            b=self.array
            #Transformer en string
            pass
        else:
            return self.array
        pass







        


        
             







