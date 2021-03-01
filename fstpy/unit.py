# -*- coding: utf-8 -*-
import pandas as pd
from .dataframe import sort_dataframe


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
   from .constants import get_column_value_from_row
   from_expression = get_column_value_from_row(unit_from,'expression')
   to_expression = get_column_value_from_row(unit_to,'expression')
   from_factor = float(get_column_value_from_row(unit_from,'factor'))
   to_factor = float(get_column_value_from_row(unit_to,'factor'))
   from_bias = float(get_column_value_from_row(unit_from,'bias'))
   to_bias = float(get_column_value_from_row(unit_to,'bias'))
   from_name = get_column_value_from_row(unit_from,'name')
   to_name = get_column_value_from_row(unit_to,'name')

   if from_expression != to_expression:
      raise TypeError
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

def get_converter(unit_from, unit_to):
   from .constants import get_column_value_from_row
   from_expression = get_column_value_from_row(unit_from,'expression')
   to_expression = get_column_value_from_row(unit_to,'expression')
   from_factor = float(get_column_value_from_row(unit_from,'factor'))
   to_factor = float(get_column_value_from_row(unit_to,'factor'))
   if from_expression != to_expression:
      raise TypeError
   if from_expression == 'K':
      converter = get_temperature_converter(unit_from, unit_to)
      return converter
   #compare units equality - compares to rows of a dataframe
   if (unit_from.values[0] == unit_to.values[0]).all():
      converter = no_conversion()
      return converter
   converter = factor_conversion(from_factor,to_factor)
   return converter

def do_unit_conversion(df:pd.DataFrame, to_unit_name:str):
   from .constants import get_unit_by_name
   from .exceptions import UnitConversionError
   unit_to = get_unit_by_name(to_unit_name)
   unit_groups = df.groupby(df.unit)
   converted_dfs = [] 
   for _, unit_group in unit_groups:
      current_unit = unit_group.iloc[0]['unit']
      if current_unit == to_unit_name:
         converted_dfs.append(unit_group)
         continue
      else:
         if (df['d'].isna() != False).all():
            raise UnitConversionError('DataFrame must be load_datad to do a unit conversion!')
         unit_from = get_unit_by_name(current_unit)
         converter = get_converter(unit_from, unit_to)
         unit_group['d'] = unit_group['d'].apply(converter)
         unit_group['unit'] = to_unit_name
         unit_group['unit_converted'] = True
         converted_dfs.append(unit_group)

   converted_df = pd.concat(converted_dfs)
   converted_df = sort_dataframe(converted_df)
   return converted_df
