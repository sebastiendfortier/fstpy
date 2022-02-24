# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
import datetime
from .std_enc import create_encoded_dateo, create_encoded_ip1
from fstpy import std_enc
from .dataframe import add_grid_column
import rpnpy.librmn.all as rmn



BASE_COLUMNS = ['nomvar', 'typvar', 'etiket', 'level', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'd']
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
    """Read a csv file and convert it to a readable csv file.
    :param path: path of the csv file i want to read
    :type path:str
    """
    def __init__(self,path,encode_ip1=True):
        self.path = path
        self.encode_ip1 = encode_ip1
        if not os.path.exists(self.path):
            raise CsvFileReaderError('Path does not exist\n')

    def to_pandas(self)-> pd.DataFrame:
        """Read the csv file and verify that I have headers and add the missing columns
        :return: df
        :rtype: pd.DataFrame
        """
        self.df = pd.read_csv(self.path,comment="#")
        if(self.verify_headers()):
            self.check_columns()
            self.df = add_grid_column(self.df)
            return self.df

    def to_pandas_no_condition(self):
        self.df = pd.read_csv(self.path,comment="#")
        return self.df

    def to_pandas_no_hdr(self):
        self.df = pd.read_csv(self.path,comment="#",header=None)
        if(self.verify_headers()):
            return self.df
            


    def verify_headers(self):
        """Verify Headers with the 3 functions that does it
        :return: self.has_header() and self.has_minimum_headers() and self.has_headers_all_valid()
        :rtype: Boolean
        """
        return self.has_header() and self.has_minimum_headers() and self.has_headers_all_valid()
    
    def check_columns(self):
        """Add the missings columns in the dataframe 
        """
        self.add_n_bits()
        self.add_datyp()
        self.add_grtyp()
        self.add_typ_var()
        self.add_ip2_ip3()
        self.add_ig()
        self.add_eticket()
        self.add_ip1()
        self.add_n_dimensions()
        self.add_deet()
        self.add_npas()
        self.add_date()
        self.to_numpy_array()
        self.change_columns_type()
        self.check_dimension_meme_etiket()
    
    
    
    def has_header(self):
        """Verify that the csv file has a single header"""
        if(self.df.columns.dtype == object):
            return True
        else:
            raise NoHeaderInFileError('Your file does not have a csv file with headers')

    def has_minimum_headers(self):
        """Verify that I have the minimum amount of headers 
        :raises MinimumHeadersError: I dont have the necessary headers to change the dataframe of the csv file.
        :return: True
        :rtype: Boolean
        """

        list_of_hdr_names = self.df.columns.tolist()
        if set(['nomvar', 'd','level']).issubset(list_of_hdr_names) or set(['nomvar', 'd','ip1']).issubset(list_of_hdr_names):
                return True
        else:
            raise MinimumHeadersError('Your csv file doesnt have the necessary columns to proceed! Check that you '
                                        + 'have at least nomvar,d and level or ip1 as columns in your csv file')

    def has_headers_all_valid(self):
        """Check that all the headers I have are valid and are present in the BASE_COLUMN list of headers availables
        :raises HeadersAreNotValidError: Raise an error when the columns names are not valid
        :return: True
        :rtype: Boolean
        """
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
        """Check if the column exists in a dataframe
        :param col: The column I want to check
        :type col: dataframe column
        :return: return true if the column exists
        :rtype: Boolean
        """
        if col in self.df.columns:
            return True
        else:
            return False

    
    def add_n_dimensions(self):
        """add ni,nj and nk column with the help of the d column in the dataframe 
        :raises ArrayIs3dError: the array present in the d column is 3D and we do not work on 3d arrays for now
        :return: df
        :rtype: dataframe
        """
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


    def add_n_bits(self):
        """Add a colomn nbits in the dataframe with a default value of 24
        """
        if(not self.col_exists("nbits")):
            self.df["nbits"] = 24
    
        

    def add_datyp(self):
        """Add a colomn datyp in the dataframe with a default value of 1
        """
        if(not self.col_exists("datyp")):
            self.df["datyp"] = 1
     
    def add_grtyp(self):
        """Add a colomn grtyp in the dataframe with a default value of X
        """
        if(not self.col_exists("grtyp")):
            self.df["grtyp"] = "X"

    def add_typ_var(self):
        """Add a colomn typvar in the dataframe with a default value of X
        """
        if(not self.col_exists("typvar")):
            self.df["typvar"] = "X"
     

    def add_date(self):
        """Add a colomn dateo and datev in the dataframe with a default value of a encoded value for both column
        """
        dateo_encoded = std_enc.create_encoded_dateo(datetime.datetime.utcnow())
        if(not self.col_exists("dateo") and not self.col_exists("datev")):
            self.df["dateo"] = dateo_encoded
            self.df["datev"] = self.df["dateo"]

    def add_ip2_ip3(self):
        """Add a colomn ip2 and ip3 in the dataframe with a default value of 0
        """
        if(not self.col_exists("ip2")):
            self.df["ip2"] = 0
        if(not self.col_exists("ip3")):
            self.df["ip3"] = 0

    def add_ig(self):
        """Add a colomn ig1,ig2,ig3,ig4 in the dataframe with a default value of 0
        """
        if(not self.col_exists("ig1")):
            self.df["ig1"] = 0        

        if(not self.col_exists("ig2")):
            self.df["ig2"] = 0

        if(not self.col_exists("ig3")):
            self.df["ig3"] = 0

        if(not self.col_exists("ig4")):
            self.df["ig4"] = 0
    
    def add_eticket(self):
        """Add a colomn etiket in the dataframe with a default value of CSVREADER
        """
        if(not self.col_exists("etiket")):
            self.df["eticket"] = "CSVREADER"
    
    def add_ip1(self):
        """Add a colomn ip1 found with the help of the level column in the dataframe which is encoded and put in the ip1 column.
        The level column is deleted after the data been encoded and put on the ip1 column
        :raises ip1andLevelExistsError: ip1 and level column exists in the given dataframe
        """
        print(self.encode_ip1)
        if self.col_exists("level") and (not self.col_exists("ip1")) and self.encode_ip1:
            print("encode == true")
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = std_enc.create_encoded_ip1(level=level,ip1_kind=IP1_KIND,mode =rmn.CONVIP_ENCODE)
                self.df.at[row.Index,"ip1"] = ip1
        elif self.col_exists("level") and (not self.col_exists("ip1")) and (not self.encode_ip1):
            print("encode == false")
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = level
                self.df.at[row.Index,"ip1"] = ip1

        elif (not self.col_exists("ip1")) and (not self.col_exists("level")):
            raise ip1andLevelExistsError("IP1 AND LEVEL EXISTS IN THE CSV FILE")
        # Remove level after we added ip1 column
        self.df.drop(["level"],axis = 1,inplace = True,errors="ignore")
    
    def add_deet(self):
        """Add a colomn grtyp in the dataframe with a default value of X
        """
        if(not self.col_exists("deet")):
            self.df["deet"] = 0

    def add_npas(self):
        if(not self.col_exists("npas")):
            self.df["npas"] = 0

    def check_dimension_meme_etiket(self):
        # Check if etiket is the same as the previous row to compare dimension if its the same etiket
        groups = self.df.groupby(['nomvar','typvar','etiket','dateo','ip1','ip2','ip3','deet','npas','datyp',
                                    'nbits','ig1','ig2','ig3','ig4'])
        for _,df in groups:
            if df.ni.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")
            if df.nj.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")


    def to_numpy_array(self):
        array_list = []
        for i in self.df.index:
            a =CsvArray(self.df.at[i,"d"])
            a= a.to_numpy()
            print(a)
            array_list.append(a)
        self.df["d"] = array_list

    def change_columns_type(self):
        self.df = self.df.astype({'ni':'int32','nj':'int32','nk':'int32','nomvar':"str",'typvar':'str','etiket':'str',
        'dateo':'int32','ip1':'int32','ip2':'int32','ip3':'int32','datyp':'int32','nbits':'int32','ig1':'int32','ig2'
        :'int32','ig3':'int32','ig4':'int32','deet':'int32','npas':'int32','grtyp':'str','datev':'int32'})
    
    
class ArrayIsNotNumpyStrError(Exception):
    pass

class ArrayIs3dError(Exception):
    pass

class ArrayIsNotStringOrNp(Exception):
    pass

class CsvArray:
    def __init__(self,array):
        self.array=array
        if(self.validate_array()):
            print("good")
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







        


        
             







