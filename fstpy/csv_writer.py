from fileinput import filename
from pathlib import Path
from .utils import initializer
import pandas as pd
import os

class PathDoesNotExists(Exception):
    pass

class CsvFileWriter:
    @initializer
    def __init__(self,path: str,df: pd.DataFrame):
        """_summary_

        :param path: _description_
        :type path: strorPath
        :param df: _description_
        :type df: pd.DataFrame
        """
        self.path = path
        self.df = df
        self.validate_imput() 
        self.write()

    def validate_imput(self):
        self.path = os.path.abspath(str(self.path))
        self.file_exists = os.path.exists(self.path)
        if self.file_exists == False:
            raise PathDoesNotExists('Path does not exist')

    def write(self):
        self.df.to_csv(self.path)
        


    


        