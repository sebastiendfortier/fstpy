# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
import datetime
from .std_enc import create_encoded_dateo, create_encoded_ip1
from fstpy import std_enc
from .dataframe import add_grid_column
import rpnpy.librmn.all as rmn



BASE_COLUMNS = ['nomvar', 'typvar', 'etiket', 'level', 'dateo', 'ip1', 'ip2', 'ip3',
                'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'd']
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


class MinimalColumnsError(Exception):
    pass


class ColumnsNotValidError(Exception):
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
            self.add_missing_columns()
            self.df = add_grid_column(self.df)
            return self.df

    def to_pandas_no_condition(self):
        """Read the csv file for testing purposes

        :return: df
        :rtype: pd.DataFrame
        """
        self.df = pd.read_csv(self.path,comment="#")
        return self.df

    def to_pandas_no_hdr(self):
        """Read the csv file for testing purposes

        :return: df
        :rtype: pd.DataFrame
        """
        self.df = pd.read_csv(self.path,comment="#",header=None)
        if(self.verify_headers()):
            return self.df
            


    def verify_headers(self):
        """Verify the file header

        :return: self.has_minimal_columns() and self.valid_columns()
        :rtype: Boolean
        """
        return self.has_minimal_columns() and self.valid_columns()
    
    def add_missing_columns(self):
        """Add the missings columns to the dataframe 
        """
        self.add_n_bits()
        self.add_datyp()
        self.add_grtyp()
        self.add_typ_var()
        self.add_ip2_ip3()
        self.add_ig()
        self.add_eticket()
        self.add_ip1()
        self.add_array_dimensions()
        self.add_deet()
        self.add_npas()
        self.add_date()
        self.to_numpy_array()
        self.change_column_dtypes()
        self.check_array_dimensions()
    
    

    def has_minimal_columns(self):
        """Verify that I have the minimum amount of headers 
        :raises MinimalColumnsError: I dont have the necessary headers to change the dataframe of the csv file.
        :return: True
        :rtype: bool
        """

        list_of_hdr_names = self.df.columns.tolist()

        if set(['nomvar', 'd','level']).issubset(list_of_hdr_names) or set(['nomvar', 'd','ip1']).issubset(list_of_hdr_names):
                return True
        else:
            raise MinimalColumnsError('Your csv file doesnt have the necessary columns to proceed! Check that you '
                                        + 'have at least nomvar,d and level or ip1 as columns in your csv file')

    def valid_columns(self):
        """Check that all the provided columns are valid and are present in BASE_COLUMN list
        :raises ColumnsNotValidError: Raise an error when the column names are not valid
        :return: True
        :rtype: bool
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
                raise ColumnsNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS')
        if(len(list_of_hdr_names) == len(BASE_COLUMNS)):
            if(all_the_cols == list_of_hdr_names):
                return True
            else:
                raise ColumnsNotValidError('The headers in the csv file are not valid. Makes sure that the columns names'
                                                            + 'are present in BASE_COLUMNS')
        else:
            raise ColumnsNotValidError('The headers in the csv file are not valid you have too many columns')

    def column_exists(self,col):
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

    
    def add_array_dimensions(self):
        """add ni, nj and nk columns with the help of the d column in the dataframe 
        :raises ArrayIs3dError: the array present in the d column is 3D and we do not work on 3d arrays for now
        :return: df
        :rtype: pd.DataFrame
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
        """Add the nbits column in the dataframe with a default value of 24
        """
        if(not self.column_exists("nbits")):
            self.df["nbits"] = 24
    
        

    def add_datyp(self):
        """Add the datyp column in the dataframe with a default value of 1
        """
        if(not self.column_exists("datyp")):
            self.df["datyp"] = 1
     
    def add_grtyp(self):
        """Add the grtyp column in the dataframe with a default value of X
        """
        if(not self.column_exists("grtyp")):
            self.df["grtyp"] = "X"

    def add_typ_var(self):
        """Add the typvar column in the dataframe with a default value of X
        """
        if(not self.column_exists("typvar")):
            self.df["typvar"] = "X"
     

    def add_date(self):
        """Add dateo and datev columns in the dataframe with default values of encoded utcnow
        """
        dateo_encoded = std_enc.create_encoded_dateo(datetime.datetime.utcnow())
        if(not self.column_exists("dateo") and not self.column_exists("datev")):
            self.df["dateo"] = dateo_encoded
            self.df["datev"] = self.df["dateo"]

    def add_ip2_ip3(self):
        """Add ip2 and ip3 columns in the dataframe with a default value of 0
        """
        if(not self.column_exists("ip2")):
            self.df["ip2"] = 0
        if(not self.column_exists("ip3")):
            self.df["ip3"] = 0

    def add_ig(self):
        """Add ig1, ig2, ig3, ig4 columns in the dataframe with a default value of 0
        """
        if(not self.column_exists("ig1")):
            self.df["ig1"] = 0        

        if(not self.column_exists("ig2")):
            self.df["ig2"] = 0

        if(not self.column_exists("ig3")):
            self.df["ig3"] = 0

        if(not self.column_exists("ig4")):
            self.df["ig4"] = 0
    
    def add_eticket(self):
        """Add the etiket column in the dataframe with a default value of CSVREADER
        """
        if(not self.column_exists("etiket")):
            self.df["eticket"] = "CSVREADER"
    
    def add_ip1(self):
        """Add the ip1 column with the help of the level column. 
        The level column is deleted after the data been encoded and put on the ip1 column

        :raises Ip1andLevelExistsError: ip1 and level column exists in the given dataframe
        """
        if self.column_exists("level") and (not self.column_exists("ip1")) and self.encode_ip1:
            # print("encode == true")
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = std_enc.create_encoded_ip1(level=level,ip1_kind=IP1_KIND,mode =rmn.CONVIP_ENCODE)
                self.df.at[row.Index,"ip1"] = ip1
        elif self.column_exists("level") and (not self.column_exists("ip1")) and (not self.encode_ip1):
            # print("encode == false")
            for row in self.df.itertuples():
                level = float (row.level)
                ip1 = level
                self.df.at[row.Index,"ip1"] = ip1

        elif (not self.column_exists("ip1")) and (not self.column_exists("level")):
            raise ip1andLevelExistsError("IP1 AND LEVEL EXISTS IN THE CSV FILE")
        # Remove level after we added ip1 column
        self.df.drop(columns=["level"],inplace = True,errors="ignore")
    
    def add_deet(self):
        """Add a colomn grtyp in the dataframe with a default value of X
        """
        if(not self.column_exists("deet")):
            self.df["deet"] = 0

    def add_npas(self):
        if(not self.column_exists("npas")):
            self.df["npas"] = 0

    def check_array_dimensions(self):
        # Check if etiket is the same as the previous row to compare dimension if its the same etiket
        groups = self.df.groupby(['nomvar','typvar','etiket','dateo','ip2','ip3','deet','npas','datyp',
                                    'nbits','ig1','ig2','ig3','ig4'])
        # print(self.df.to_string())
        # print(groups)

        for _,df in groups:
            # print(df.drop(columns=["d"]))
            # print(df.ni.unique())
            # print(df.nj.unique())
            if df.ni.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")
            if df.nj.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")


    def to_numpy_array(self):
        array_list = []
        for i in self.df.index:
            a =CsvArray(self.df.at[i,"d"])
            a= a.to_numpy()
            # print(a)
            array_list.append(a)
        self.df["d"] = array_list

    def change_column_dtypes(self):
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
    """_summary_

    :param array: _description_
    :type array: _type_
    :raises ArrayIsNotStringOrNp: _description_
    """
    def __init__(self,array):
        """_summary_

        :return: _description_
        :rtype: _type_
        """
        self.array=array
        if(self.validate_array()):
            pass
        else:
            raise ArrayIsNotStringOrNp("The array is not a string or a numpy aray")

    def validate_array(self):
        """_summary_

        :raises ArrayIs3dError: _description_
        :return: _description_
        :rtype: _type_
        """
        #Verifier que larray est une string ou np.ndarray
        if(type(self.array) == np.ndarray or type(self.array) == str):
            return True
        else:
            return False
            

    def to_numpy(self):
        """_summary_

        :raises ArrayIs3dError: _description_
        :return: _description_
        :rtype: _type_
        """
        if isinstance(self.array,str):
            b = self.array
            a = np.array([[float(j) for j in i.split(',')] for i in b.split(';')],dtype=np.float32, order='F')
            if(a.ndim == 3):
                raise ArrayIs3dError('The numpy array you created from the string array is 3D and it should not be 3d')
            return a
        else:
            return self.array


    def to_str(self):
        """_summary_

        :raises ArrayIs3dError: _description_
        :return: _description_
        :rtype: _type_
        """
        if isinstance(self.array,np.ndarray):
            b=self.array
            # check that array is 2d only
            # case for 2d array
            # dim0 = []
            # ndim0 = self.array.shape[0]
            # for i in range(ndim0):
            #     dim0.append([self.array[i,j] for j in range(self.array.shape[1])])
            # transform all arrays to strings and concat with ';' ex ';'.join(list of strings)
            # Transformer en string
            pass
        else:
            return self.array
        pass







        


        
             







