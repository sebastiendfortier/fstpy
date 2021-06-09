# -*- coding: utf-8 -*-
import abc
import pandas as pd

class Plugin(abc.ABC):
    def __init__(self,df:pd.DataFrame) -> None:
        pass

    @abc.abstractclassmethod
    def compute(self) -> pd.DataFrame:
        pass