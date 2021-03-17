# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from fstpy import get_column_value_from_row, get_unit_by_name
from fstpy.std_dec import get_unit_and_description

from .dataframe import add_unit_column
from .exceptions import UnitConversionError
from .std_reader import load_data


class no_conversion:
   def __init__(self, bias = 0.0,   factor = 1.0):
      pass
   def __call__(self,v):
      return v

class kelvin_to_kelvin:
   def __init__(self, bias = 0.0,   factor = 1.0 ):
      pass
   def __call__(self,v):
      return v

class kelvin_to_celsius:
   def __init__(self, bias = 0.0, factor = 1.0):
      self.bias = bias
   def __call__(self,v):
      return v - self.bias

class kelvin_to_fahrenheit:
   def __init__(self, bias = 0.0,   factor = 1.0):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      return v * 1/self.factor - self.bias

class kelvin_to_rankine:
   def __init__(self, bias = 0.0, factor = 1.0):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      return v * 1/self.factor

class celsius_to_kelvin:
   def __init__(self,  bias = 0.0, factor = 1.0):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      return v +  self.bias

class celsius_to_celsius:
   def __init__(self,  bias = 0.0,   factor = 1.0):
      pass
   def __call__(self,v):
      return v

class celsius_to_fahrenheit:
   def __init__(self, cbias = 0.0,   cfactor = 1.0,   fbias = 0.0,   ffactor = 1.0):
      self.cbias=cbias
      self.cfactor=cfactor
      self.fbias=fbias
      self.ffactor=ffactor
   def __call__(self,v):
      a = kelvin_to_fahrenheit(self.fbias, self.ffactor)
      b = celsius_to_kelvin(self.cbias, self.cfactor)
      return a(b(v))

class celsius_to_rankine:
   def __init__(self, cbias = 0.0,   cfactor = 1.0,   rbias = 0.0,   rfactor = 1.0):
      self.cbias=cbias
      self.cfactor=cfactor
      self.rbias=rbias
      self.rfactor=rfactor
   def __call__(self,v):
      a = kelvin_to_rankine(self.rbias, self.rfactor)
      b = celsius_to_kelvin(self.cbias, self.cfactor)
      return a(b(v))

class fahrenheit_to_kelvin:
   def __init__(self, bias,   factor):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      return v +  self.bias * self.factor

class fahrenheit_to_celsius:
   def __init__(self, fbias = 0.0,   ffactor = 1.0,   cbias = 0.0,   cfactor = 1.0):
      self.fbias=fbias
      self.ffactor=ffactor
      self.cbias=cbias
      self.cfactor=cfactor
   def __call__(self,v):
      a = kelvin_to_celsius(self.cbias, self.cfactor)
      b = fahrenheit_to_kelvin(self.fbias, self.ffactor)
      return a(b(v))

class fahrenheit_to_fahrenheit:
   def __init__(self, bias,   factor):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
         return v
         
class fahrenheit_to_rankine:
   def __init__(self,  bias, factor):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      a = kelvin_to_rankine(self.bias, self.factor)
      b = fahrenheit_to_kelvin(self.bias, self.factor)
      return a(b(v))

class rankine_to_kelvin:
   def __init__(self,  bias, factor):
      self.bias=bias
      self.factor=factor
   def __call__(self,v):
      return v * self.factor
         
class rankine_to_celsius:
   def __init__(self,  rbias = 0.0,   rfactor = 1.0,   cbias = 0.0,   cfactor = 1.0):
      self.rbias=rbias
      self.rfactor=rfactor
      self.cbias=cbias
      self.cfactor=cfactor
   def __call__(self,v):
      a = kelvin_to_celsius(self.cbias, self.cfactor)
      b = rankine_to_kelvin(self.rbias, self.rfactor)
      return a(b(v))

class rankine_to_fahrenheit:
   def __init__(self, rbias= 0.0, rfactor=1.0, fbias = 0.0,   ffactor = 1.0):
      self.rbias=rbias
      self.rfactor=rfactor
      self.fbias=fbias
      self.ffactor=ffactor
   def __call__(self,v):
      a = kelvin_to_fahrenheit(self.fbias, self.ffactor)
      b = rankine_to_kelvin(self.rbias, self.rfactor)
      return a(b(v))

class rankine_to_rankine:
   def __init__(self, bias, factor):
      self.bias = bias
      self.factor = factor
   def __call__(self,v):
      return v

class factor_conversion:
   def __init__(self, from_factor, to_factor):
      self.from_factor = from_factor
      self.to_factor = to_factor
   def __call__(self,v):
      return v * self.from_factor / self.to_factor


def get_temperature_converter(unit_from, unit_to):
   from_expression = get_column_value_from_row(unit_from,'expression')
   to_expression = get_column_value_from_row(unit_to,'expression')
   from_factor = float(get_column_value_from_row(unit_from,'factor'))
   to_factor = float(get_column_value_from_row(unit_to,'factor'))
   from_bias = float(get_column_value_from_row(unit_from,'bias'))
   to_bias = float(get_column_value_from_row(unit_to,'bias'))
   from_name = get_column_value_from_row(unit_from,'name')
   to_name = get_column_value_from_row(unit_to,'name')

   if from_expression != to_expression:
      raise UnitConversionError('different unit family')
   if (from_name == "kelvin") and (to_name == "kelvin"):
      converter = kelvin_to_kelvin(to_bias, to_factor)
      return converter 
   if (from_name == "kelvin") and (to_name == "celsius"):
      converter = kelvin_to_celsius(to_bias, to_factor)            
      return converter 
   if (from_name == "kelvin") and (to_name == "fahrenheit"):
      converter = kelvin_to_fahrenheit(to_bias, to_factor)         
      return converter 
   if (from_name == "kelvin") and (to_name == "rankine"):
      converter = kelvin_to_rankine(to_bias, to_factor)            
      return converter 
   if (from_name == "celsius") and (to_name == "kelvin"):
      converter = celsius_to_kelvin(from_bias, from_factor)        
      return converter 
   if (from_name == "celsius") and (to_name == "celsius"):
      converter = celsius_to_celsius(from_bias, from_factor)       
      return converter 
   if (from_name == "celsius") and (to_name == "fahrenheit"):
      converter = celsius_to_fahrenheit(from_bias, from_factor)    
      return converter 
   if (from_name == "celsius") and (to_name == "rankine"):
      converter = celsius_to_rankine(from_bias, from_factor)       
      return converter 
   if (from_name == "fahrenheit") and (to_name == "kelvin"):
      converter = fahrenheit_to_kelvin(from_bias, from_factor)     
      return converter 
   if (from_name == "fahrenheit") and (to_name == "celsius"):
      converter = fahrenheit_to_celsius(from_bias, from_factor)    
      return converter 
   if (from_name == "fahrenheit") and (to_name == "fahrenheit"):
      converter = fahrenheit_to_fahrenheit(from_bias, from_factor) 
      return converter 
   if (from_name == "fahrenheit") and (to_name == "rankine"):
      converter = fahrenheit_to_rankine(from_bias, from_factor)    
      return converter 
   if (from_name == "rankine")    and (to_name == "kelvin"):
      converter = rankine_to_kelvin(from_bias, from_factor)        
      return converter 
   if (from_name == "rankine")    and (to_name == "celsius"):
      converter = rankine_to_celsius(from_bias, from_factor)       
      return converter 
   if (from_name == "rankine")    and (to_name == "fahrenheit"):
      converter = rankine_to_fahrenheit(from_bias, from_factor)    
      return converter 
   if (from_name == "rankine")    and (to_name == "rankine"):
      converter = rankine_to_rankine(from_bias, from_factor)
      return converter 
   return no_conversion()

def get_converter(unit_from:str, unit_to:str):
   """Based on unit names contained in fstpy.UNITS database (dataframe), 
   attemps to provide the appropriate unit conversion function 
   based on unit name and family. The returned function takes a value
   and returns a value value_to = f(value_from). 

   :param unit_from: unit name to convert from
   :type unit_from: str
   :param unit_to: unit name to convert to
   :type unit_to: str
   :raises UnitConversionError: Exception
   :return: returns the unit conversion function
   :rtype: function
   """
   from_expression = get_column_value_from_row(unit_from,'expression')
   to_expression = get_column_value_from_row(unit_to,'expression')
   from_factor = float(get_column_value_from_row(unit_from,'factor'))
   to_factor = float(get_column_value_from_row(unit_to,'factor'))
   if from_expression != to_expression:
      raise UnitConversionError('different unit family')
   if from_expression == 'K':
      converter = get_temperature_converter(unit_from, unit_to)
      return converter
   #compare units equality - compares to rows of a dataframe
   if (unit_from.values[0] == unit_to.values[0]).all():
      converter = no_conversion()
      return converter
   converter = factor_conversion(from_factor,to_factor)
   return np.vectorize(converter)

def do_unit_conversion(df:pd.DataFrame, to_unit_name='scalar',standard_unit=False) -> pd.DataFrame:
   """Converts the data portion 'd' of all the records of a dataframe to the specified unit
   provided in the to_unit_name parameter. If the standard_unit flag is True, the to_unit_name 
   will be ignored and the unit will be based on the standard file variable dictionnary unit
   value instead. This ensures that if a unit conversion was done, the varaible will return
   to the proper standard file unit value. ex. : TT should be in celsius. o.dict can be consulted
   to get the appropriate unit values.

   :param df: dataframe containing records to be converted
   :type df: pd.DataFrame
   :param to_unit_name: unit name to convert to, defaults to 'scalar'
   :type to_unit_name: str, optional
   :param standard_unit: flag to indicate the use of dictionnary units, defaults to False
   :type standard_unit: bool, optional
   :return: a dataframe containing the converted data
   :rtype: pd.DataFrame
   """
   df = load_data(df)
   if 'unit' not in df.columns:
      df = add_unit_column(df)
   unit_to = get_unit_by_name(to_unit_name)
   #unit_groups = df.groupby(df.unit)
   #converted_dfs = [] 
   for i in df.index:
      current_unit = df.at[i,'unit']
      if current_unit == to_unit_name:
         continue
      else:
         if standard_unit:
            to_unit_name,_ = get_unit_and_description(df.at[i,'nomvar'])
            unit_to = get_unit_by_name(to_unit_name)
         unit_from = get_unit_by_name(current_unit)
         converter = get_converter(unit_from, unit_to)
         df.at[i,'d'] = converter(df.at[i,'d'])
         df.at[i,'unit'] = to_unit_name
         df.at[i,'unit_converted'] = True
        
   return df
