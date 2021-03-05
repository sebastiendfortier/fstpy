# Utils

## Python utility

**initializer decorator, used to initialize automatically parameters of __init__ method of a class**   

 ```python 
 class AClass():

    @initializer
    def __init__(self, param1, param2, param3):
        # no need to specify self.param1 = param1 etc...
        pass
```

## Dataframe utilities

**Gets a list of DataFrames grouped by specified options, results are always groupepd by grid**   

```python 
get_groups(df:pd.DataFrame, group_by_forecast_hour:bool=False,group_by_level=True) -> list:   
``` 

**When working with 3D data, this function flattens all the 2d arrays for each level**  

```python 
flatten_data_series(df) -> pd.DataFrame:  
``` 

**Function to create a one row DataFrame for holding results**  

```python
create_1row_df_from_model(df:pd.DataFrame) -> pd.DataFrame:
``` 

**Function to check that a DataFrame is not empty**  

```python 
validate_df_not_empty(df, caller_class, error_class):
``` 

## Validators

**Funtion used to make sure the nomvar is 4 character long for standard files**   

```python 
validate_nomvar(nomvar, caller_class, error_class)  
``` 

