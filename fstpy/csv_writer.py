from fileinput import filename
from pathlib import Path
from fstpy.csv_reader import CsvArray

from fstpy.std_reader import compute
from .utils import initializer
import pandas as pd

class PathDoesNotExists(Exception):
    pass

class CsvFileWriter:
    @initializer
    def __init__(self,path: str,df: pd.DataFrame):
        
        # self.validate_imput() 
        self.path = path
        self.df = df

    # def validate_imput(self):
    #     self.path = os.path.abspath(str(self.path))
    #     self.file_exists = os.path.exists(self.path)
    #     if self.file_exists == False:
    #         raise PathDoesNotExists('Path does not exist')

    def to_csv(self):
        # self.df = compute(self.df)
        for i in self.df.index:
            self.df.at[i,'d'] = CsvArray(self.df.at[i,'d']).to_str()

        self.df.to_csv(self.path,index=False)
        


    


        