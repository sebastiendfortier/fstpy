# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
import datetime
from typing import Final
from .std_enc import create_encoded_dateo,create_encoded_ip1
from .dataframe import add_grid_column
import rpnpy.librmn.all as rmn


BASE_COLUMNS = ['nomvar', 'typvar', 'etiket', 'level', 'dateo', 'ip1', 'ip2', 'ip3',
                'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'd']
IP1_KIND : Final[int] = 3
NOMVAR_MIN_LEN : Final[int] = 2
NOMVAR_MAX_LEN : Final[int] = 4
TYPVAR_MIN_LEN: Final[int] = 1
TYPVAR_MAX_LEN: Final[int] = 2
ETIKET_MIN_LEN: Final[int] = 1
ETIKET_MAX_LEN: Final[int] = 12
NBITS_DEFAULT: Final[int] = 24
DATYP_DEFAULT: Final[int] = 1
GRTYP_DEFAULT: Final[str] = "X"
TYPVAR_DEFAULT: Final[str] = "X"
IP2_DEFAULT: Final[int] = 0
IP3_DEFAULT: Final[int] = 0
IG1_DEFAULT: Final[int] = 0
IG2_DEFAULT: Final[int] = 0
IG3_DEFAULT: Final[int] = 0
IG4_DEFAULT: Final[int] = 0
ETIKET_DEFAULT: Final[str] = "CSVREADER"
DEET_DEFAULT: Final[int] = 0
NPAS_DEFAULT: Final[int] = 0


class CsvFileReaderError(Exception):
    pass


class MinimalColumnsError(Exception):
    pass


class ColumnsNotValidError(Exception):
    pass


class Ip1andLevelExistsError(Exception):
    pass


class DimensionError(Exception):
    pass


class NomVarLengthError(Exception):
    pass


class TypVarLengthError(Exception):
    pass


class EtiketVarLengthError(Exception):
    pass


class CsvFileReader:
    """Read a csv file and convert it to a pandas dataframe.
    :param path: path of the csv file i want to read
    :type path:str
    Algorithm:
        Reading a file that must have the followed form:
        COLUMNS/HEADERS NAMES: nomvar,etiket,level,d
        VALUES:                CSV,CSVREADER,1.0,"11.1,22.2;33.3,44.4;55.5,66.6"
        VALUES:                CSV,CSVREADER,0.0,"77.7,88.8;99.9,100.10;110.11,120.12"

    - The d column is composed of floats and the ";" means one of the line of the level is done
    - You can't provide an ip1 column and a level column at the same time in your dataframe
    - One line of a single level represent the x axis (row)
    - The values inside a single line are the y axis (column)
    - The number of rows of a matrix d determines ni.
    - The number of columns of a matrix section determines nj
    - If 2 arrays of the column d have the same dimension they can have the same variable name and etiket
    - If you wish to comment a csv file, dont do it in the same line as the values in the csv files. You need to have lines with only comments.
    - nomvar:string, typvar:string, etiket:string, level:int32, dateo:, ip1, ip2, ip3,deet, npas, datyp, nbits, grtyp, ig1, ig2, ig3, ig4, d are the admissibles columns in the dataframe
    - The admissible columns in the dataframe given before being used by the csv reader:
            'nomvar': "str", variable name 
            'typvar': 'str', type of field (forecast, analysis, climatology)
            'etiket': 'str', label
            'level': 'int32', value that helps get ip1
            'ip1': 'int32', vertical level
            'ip2': 'int32', forecast hour
            'ip3': 'int32', user defined identifier
            'datyp': 'int32', data type 
            'nbits': 'int32', number of bits kept for the elements of the field
            'grtyp': 'str', type of geographical projection

    - The columns in the dataframe created with the csv reader:
            'ni': 'int32', first dimension of the data field
            'nj': 'int32', second dimension of the data field
            'nk': 'int32', third dimension of the data field
            'nomvar': "str", variable name 
            'typvar': 'str', type of field (forecast, analysis, climatology)
            'etiket': 'str', label
            'dateo': 'int32', date of observation  
            'ip1': 'int32', vertical level
            'ip2': 'int32', forecast hour
            'ip3': 'int32', user defined identifier
            'datyp': 'int32', data type 
            'nbits': 'int32', number of bits kept for the elements of the field
            'ig1': 'int32', first grid descriptor
            'ig2': 'int32', second grid descriptor
            'ig3': 'int32', third grid descriptor
            'ig4': 'int32', fourth grid descriptor
            'deet': 'int32', Length of a time step in seconds datev constant unless keep_dateo 
            'npas': 'int32', time step number datev constant unless keep_dateo
            'grtyp': 'str', type of geographical projection 
            'datev': 'int32', date of validation
    """


    def __init__(self, path, encode_ip1=True):
        self.path = path
        self.encode_ip1 = encode_ip1
        if not os.path.exists(self.path):
            raise CsvFileReaderError('Path does not exist\n')


    def to_pandas(self) -> pd.DataFrame:
        """Read the csv file and verify the existence of headers and add the missing columns if they are missing
        :return: df
        :rtype: pd.DataFrame
        """
        self.df = pd.read_csv(self.path, comment="#")
        self.df.columns = self.df.columns.str.replace(' ', '')
        if(self.verify_headers()):
            self.add_missing_columns()
            self.check_columns()
            self.df = add_grid_column(self.df)
            return self.df


    def count_char(self, s):
        """Count the number of characters in a string 
        :param s: the name of the column
        :return: list of the count of the number of characters of the strings of every row of a column
        """
        array_list = []
        for i in self.df.index:
            a = len(self.df.at[i, s])
            array_list.append(a)
        return array_list


    def check_nomvar_char_length(self):
        """Check that the length of the column nomvar is always between 2 and 4 characters for the whole dataframe 
        """

        a = self.count_char(s="nomvar")
        for i in a:
            if (i < NOMVAR_MIN_LEN or i > NOMVAR_MAX_LEN):
                raise NomVarLengthError("the variable nomvar should have between 2 and 4 characters")


    def check_typvar_char_length(self):
        """Check that the length of the column typvar is always between 1 and 2 characters for the whole dataframe 
        """
        a = self.count_char(s="typvar")
        for i in a:
            if (i < TYPVAR_MIN_LEN or i > TYPVAR_MAX_LEN):
                raise TypVarLengthError("the variable typvar should have between 1 and 2 characters")


    def check_etiket_char_length(self):
        """Check that the length of the column etiket is always between 1 and 12 characters for the whole dataframe 
        """

        a = self.count_char(s="etiket")
        for i in a:
            if (i < ETIKET_MIN_LEN or i > ETIKET_MAX_LEN):
                raise EtiketVarLengthError("the variable etiket should have between 1 and 12 characters")


    def verify_headers(self):
        """Verify the file header

        :return: self.has_minimal_columns() and self.valid_columns()
        :rtype: Boolean
        """
        return self.has_minimal_columns() and self.valid_columns()


    def add_missing_columns(self):
        """Add the missings columns to the dataframe 
        """
        self.add_nbits()
        self.add_datyp()
        self.add_grtyp()
        self.add_typvar()
        self.add_ip2_ip3()
        self.add_ig()
        self.add_etiket()
        self.add_ip1()
        self.add_array_dimensions()
        self.add_deet()
        self.add_npas()
        self.add_date()
        self.to_numpy_array()


    def check_columns(self):
        """Check the types of the columns, the dimensions of the differents d arrays and the length of the nomvar,etiket
        and typvar of the dataframe"""

        self.change_column_dtypes()
        self.check_array_dimensions()
        self.check_nomvar_char_length()
        self.check_typvar_char_length()
        self.check_etiket_char_length()


    def has_minimal_columns(self):
        """Verify that I have the minimum amount of headers 
        :raises MinimalColumnsError: raises this error if the necessary headers are not present in the dataframe
        :return: True
        :rtype: bool
        """

        list_of_hdr_names = self.df.columns.tolist()

        if set(['nomvar', 'd', 'level']).issubset(list_of_hdr_names) or set(['nomvar', 'd', 'ip1']).issubset(list_of_hdr_names):
            return True
        else:
            raise MinimalColumnsError('Your csv file doesnt have the necessary columns to proceed! Check that you '
                                      + 'have at least nomvar,d and level or ip1 as columns in your csv file')


    def valid_columns(self):
        """Check that all the provided columns are valid and are present in BASE_COLUMN list
        :raises ColumnsNotValidError: Raise an error when the column names are not valid
        :return: True
        :rtype: Boolean
        """
        all_the_cols = BASE_COLUMNS
        all_the_cols.sort()
        list_of_hdr_names = self.df.columns.tolist()
        list_of_hdr_names.sort()

        set1 = set(list_of_hdr_names)
        set2 = set(BASE_COLUMNS)

        if(len(list_of_hdr_names) < len(BASE_COLUMNS)):
            is_subset = set1.issubset(set2)
            if(is_subset):
                return True
            else:
                raise ColumnsNotValidError(f'The headers in the csv file are not valid. Make sure that the columns names'
                                           + 'are present in {BASE_COLUMNS}')

        if(len(list_of_hdr_names) == len(BASE_COLUMNS)):
            if all_the_cols == list_of_hdr_names:
                return True
            else:
                raise ColumnsNotValidError(f'The headers in the csv file are not valid. Make sure that the columns names'
                                           + 'are present in {BASE_COLUMNS}')
        else:
            raise ColumnsNotValidError('The headers in the csv file are not valid you have too many columns')


    def column_exists(self, col):
        """Check if the column exists in the dataframe
        :param col: The column to check
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
        :raises ArrayIs3dError: The error is raised if the array present in the d column is 3D
        :return: df
        :rtype: pd.DataFrame
        """
        for row in self.df.itertuples():
            array = row.d
            a = np.array([[float(j) for j in i.split(',')] for i in array.split(';')], dtype=np.float32, order='F')
            if(a.ndim == 1):
                ni = np.shape(a)[0]
                nj = 0
                nk = 1

            if(a.ndim == 2):
                ni = np.shape(a)[0]
                nj = np.shape(a)[1]
                nk = 1

            if(a.ndim == 3):
                raise ArrayIs3dError('The numpy array you created from the string array is 3D and it should not be 3d')
            self.df.at[row.Index, "ni"] = ni
            self.df.at[row.Index, "nj"] = nj
            self.df.at[row.Index, "nk"] = nk
        return self.df


    def add_nbits(self):
        """Add the nbits column in the dataframe with a default value of 24
        """
        if(not self.column_exists("nbits")):
            self.df["nbits"] = NBITS_DEFAULT


    def add_datyp(self):
        """Add the datyp column in the dataframe with a default value of 1
        """
        if(not self.column_exists("datyp")):
            self.df["datyp"] = DATYP_DEFAULT


    def add_grtyp(self):
        """Add the grtyp column in the dataframe with a default value of X
        """
        if(not self.column_exists("grtyp")):
            self.df["grtyp"] = GRTYP_DEFAULT


    def add_typvar(self):
        """Add the typvar column in the dataframe with a default value of X
        """
        if(not self.column_exists("typvar")):
            self.df["typvar"] = TYPVAR_DEFAULT


    def add_date(self):
        """Add dateo and datev columns in the dataframe with default values of encoded utcnow
        """
        dateo_encoded = create_encoded_dateo(datetime.datetime.utcnow())
        if(not self.column_exists("dateo") and not self.column_exists("datev")):
            self.df["dateo"] = dateo_encoded
            self.df["datev"] = self.df["dateo"]


    def add_ip2_ip3(self):
        """Add ip2 and ip3 columns in the dataframe with a default value of 0
        """
        if(not self.column_exists("ip2")):
            self.df["ip2"] = IP2_DEFAULT
        if(not self.column_exists("ip3")):
            self.df["ip3"] = IP3_DEFAULT


    def add_ig(self):
        """Add ig1, ig2, ig3, ig4 columns in the dataframe with a default value of 0
        """
        if(not self.column_exists("ig1")):
            self.df["ig1"] = IG1_DEFAULT

        if(not self.column_exists("ig2")):
            self.df["ig2"] = IG2_DEFAULT

        if(not self.column_exists("ig3")):
            self.df["ig3"] = IG3_DEFAULT

        if(not self.column_exists("ig4")):
            self.df["ig4"] = IG4_DEFAULT


    def add_etiket(self):
        """Add the etiket column in the dataframe with a default value of CSVREADER
        """
        if(not self.column_exists("etiket")):
            self.df["eticket"] = ETIKET_DEFAULT


    def add_ip1(self):
        """Add the ip1 column with the help of the level column. 
        The level column is deleted after the data been encoded and put on the ip1 column

        :raises Ip1andLevelExistsError: raised when ip1 and level column exists in the given dataframe
        """
        if self.column_exists("level") and (not self.column_exists("ip1")) and self.encode_ip1:
            for row in self.df.itertuples():
                level = float(row.level)
                ip1 = create_encoded_ip1(level=level, ip1_kind=IP1_KIND, mode=rmn.CONVIP_ENCODE)
                self.df.at[row.Index, "ip1"] = ip1

        elif self.column_exists("level") and (not self.column_exists("ip1")) and (not self.encode_ip1):
            for row in self.df.itertuples():
                level = float(row.level)
                ip1 = level
                self.df.at[row.Index, "ip1"] = ip1

        elif (self.column_exists("ip1")) and (self.column_exists("level")):
            raise Ip1andLevelExistsError("IP1 AND LEVEL EXISTS IN THE CSV FILE")

        # Remove level after we added ip1 column
        self.df.drop(columns=["level"], inplace=True, errors="ignore")


    def add_deet(self):
        """Add a colomn deet in the dataframe with a default value of 0
        """
        if(not self.column_exists("deet")):
            self.df["deet"] = DEET_DEFAULT


    def add_npas(self):
        """Add a colomn npas in the dataframe with a default value of 0
        """
        if(not self.column_exists("npas")):
            self.df["npas"] = NPAS_DEFAULT


    def check_array_dimensions(self):
        """Check if etiket is the same as the previous row to compare dimension if its the same etiket and variable name

        :raises DimensionError: raise an error when the array with the same var and etiket dont have the same dimension
        """
        groups = self.df.groupby(['nomvar', 'typvar', 'etiket', 'dateo', 'ip2', 'ip3', 'deet', 'npas', 'datyp',
                                  'nbits', 'ig1', 'ig2', 'ig3', 'ig4'])

        for _, df in groups:
            if df.ni.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")
            if df.nj.unique().size != 1:
                raise DimensionError("Array with the same var and etiket dont have the same dimension ")


    def to_numpy_array(self):
        """Takes a line of an array string and transform it to a numpy array

        :return: return an array list of numpy arrays
        """
        array_list = []
        for i in self.df.index:
            a = CsvArray(self.df.at[i, "d"])
            a = a.to_numpy()
            array_list.append(a)
        self.df["d"] = array_list

    def change_column_dtypes(self):
        """Change the columns types to the correct types in the dataframe
        """
        self.df = self.df.astype({'ni': 'int32', 'nj': 'int32', 'nk': 'int32', 'nomvar': "str", 'typvar': 'str', 'etiket': 'str',
                                  'dateo': 'int32', 'ip1': 'int32', 'ip2': 'int32', 'ip3': 'int32', 'datyp': 'int32', 'nbits': 'int32',
                                  'ig1': 'int32', 'ig2': 'int32', 'ig3': 'int32', 'ig4': 'int32', 'deet': 'int32', 'npas': 'int32',
                                  'grtyp': 'str', 'datev': 'int32'})


class ArrayIsNotNumpyStrError(Exception):
    pass


class ArrayIs3dError(Exception):
    pass


class ArrayIsNotStringOrNp(Exception):
    pass


class CsvArray:
    """A class that represents an array with the data in the csv file

    :param array: An array with the data
    :type array: string or numpy array
    :raises ArrayIsNotStringOrNp: the error is raised when the array is not formed from strings or numpy array
    """

    def __init__(self, array):
        self.array = array
        if(self.validate_array()):
            pass
        else:
            raise ArrayIsNotStringOrNp("The array is not a string or a numpy aray")

    def validate_array(self):
        """validate that the array is either an array string or a numpy string

        :raises ArrayIs3dError: raises an error when the array provided is 3D
        :rtype: Boolean
        """

        if(type(self.array) == np.ndarray or type(self.array) == str):
            return True
        else:
            return False

    def to_numpy(self):
        """the string array is transformed to a numpy array

        :raises ArrayIs3dError: raises an error when the array provided is 3D
        :return: numpy array
        """
        if isinstance(self.array, str):
            b = self.array
            a = np.array([[float(j) for j in i.split(',')] for i in b.split(';')], dtype=np.float32, order='F')
            if(a.ndim == 3):
                raise ArrayIs3dError('The numpy array you created from the string array is 3D and it should not be 3d')
            return a
        else:
            return self.array

    def to_str(self):
        """the numpy array is transformed to a string array

        :raises ArrayIs3dError: raises an error when the array provided is 3D
        :return: string array
        """
        if isinstance(self.array, np.ndarray):
            b = self.array
            dim0 = []
            ndim0 = self.array.shape[0]

            for i in range(ndim0):
                dim0.append([self.array[i, j] for j in range(self.array.shape[1])])

            dim0 = []
            ndim0 = self.array.shape[0]

            for i in range(ndim0):
                dim0.append([self.array[i, j] for j in range(self.array.shape[1])])

            s = ""

            for i in range(ndim0):
                s1 = str(dim0[i]).replace("[", "")
                s1 = s1.replace("]", ";")
                s += s1
            s = s.replace(" ", "")
            s = s.rstrip(s[-1])
            return s
        else:
            return self.array
