from fileinput import filename
from pathlib import Path
from xmlrpc.client import Boolean
from fstpy.csv_reader import CsvArray
from fstpy.std_reader import compute
import os.path
import pandas as pd
BASE_COLUMNS = ['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4', 'datev','d']

class CsvFileWriterError(Exception):
    pass

class CsvFileWriter:
    def __init__(self,path: str,df: pd.DataFrame,overwrite=False): 
        self.path = path
        self.df = df
        self.overwrite = overwrite

    def to_csv(self):
        if not os.path.isfile(self.path) or self.overwrite == True:
            self.df = compute(self.df)
            self.convert_d_column()
            self.remove_grid_column()
            self.check_columns()
            self.change_column_dtypes()
            self.df.to_csv(self.path,index=False)
        else:
            raise CsvFileWriterError("The file created already exists in the path specified. Use overwrite flag to avoid this error")

    def remove_columns(self):
        """Remove columns that are not part of the columns in the csv file
        """
        if len(self.df.columns) > len(BASE_COLUMNS):
            diff = list(set(BASE_COLUMNS) ^ set(self.df.columns))
            for i in diff:
                self.df.drop(str(i), inplace=True, axis=1)
            

    def change_column_dtypes(self):
        """Change the columns types to the correct types in the dataframe
        """
        self.df = self.df.astype({'ni': 'int32', 'nj': 'int32', 'nk': 'int32', 'nomvar': "str", 'typvar': 'str', 'etiket': 'str',
                                  'dateo': 'int32', 'ip1': 'int32', 'ip2': 'int32', 'ip3': 'int32', 'datyp': 'int32', 'nbits': 'int32',
                                  'ig1': 'int32', 'ig2': 'int32', 'ig3': 'int32', 'ig4': 'int32', 'deet': 'int32', 'npas': 'int32',
                                  'grtyp': 'str', 'datev': 'int32'})

    def remove_grid_column(self):
        """Remove grid column ,because it's added in the csv reader
        """
        self.df.drop('grid', inplace=True, axis=1)

    def check_columns(self):
        """Check that all the columns in the dataframe are the correct ones. If any of the columns are not supposed to be there they are deleted

        :raises ColumnsError: The columns in the dataframe are not valid
        :return: True
        :rtype: Boolean
        """
        all_the_cols = BASE_COLUMNS
        all_the_cols.sort()
        list_of_hdr_names = self.df.columns.tolist()
        list_of_hdr_names.sort()
        if all_the_cols == list_of_hdr_names:
            return True
        else:
            self.remove_columns()
            

    def convert_d_column(self):
        """Convert the d column to a string in the right format
        """
        for i in self.df.index:
            self.df.at[i,'d'] = CsvArray(self.df.at[i,'d']).to_str()
        


    


        