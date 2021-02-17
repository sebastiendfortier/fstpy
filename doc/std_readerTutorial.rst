fstpy std_reader Tutorial
=========================


.. toctree::
   :maxdepth: 3

   fstpy std_readerTutorial


Examples
--------
'''Examples are available as a jupyter notebook and script in the following directory /fs/site4/eccc/cmd/w/sbf000/fstpy/doc'''

Getting and manipulating the meta data
--------------------------------------

Get a dataframe from a standard file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    >>> import fstpy
    >>> # method 1 - to_pandas with explicit instance
    >>> std_file = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std')
    >>> df = std_file.to_pandas()
    >>> 
    >>> # method 2 - to_pandas without explicit instance
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()


Get a dataframe from multiple standard files
--------------------------------------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader(['/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',etc...]).to_pandas()


See the contents of the dataframe
---------------------------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> # show a summary of the dataframe contentvs
    >>> print(df)
    >>> #show the first lines of the dataframe
    >>> print(df.head())
    >>> # show the last rows of the dataframe
    >>> print(df.tail())
    >>> # show column names of the dataframe
    >>> print(df.columns)
    >>> # show the levels contained in the dataframe
    >>> print(df.level)
    >>> # show the unique levels contained in the dataframe
    >>> print(df.level.unique())
    >>> # show a subset of columns of the dataframe
    >>> print(df[['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3']])
    >>> # show a voir like output of the dataframe
    >>> fstpy.voir(df)


select sub-sets of data
-----------------------------

'''Note''': fstpy.select is a wrapper for pandas.DataFrame.query method 

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> # select TT
    >>> sel_tt_df = fstpy.select(df,'nomvar=="TT"')
    >>> # select UU and VV
    >>> sel_uuvv_df = fstpy.select(df,'nomvar in ["UU","VV"]')
    >>> # select UU and VV with ip2 of 6
    >>> sel_uuvv6_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (ip2==6)')

selecting by date range
-----------------------

    >>> from datetime import date,datetime
    >>> start_date = datetime(2020, 2, 1, 0, 0)
    >>> end_date = datetime(2020, 2, 4, 12, 0)
    >>> print(start_date,end_date)
    >>> mask = df['pdateo'].between(start_date, end_date, inclusive=True)
    >>> sub_df = df[mask]
    >>> print(sub_df.sort_values(by=['pdateo']))


Modify meta data
----------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> # select TT
    >>> sel_tt_df = fstpy.select(df,'nomvar=="TT"')
    >>> # change nomvar from TT to TTI
    >>> fstpy.zapped_df = fstpy.zap(sel_tt_df,mark=False,nomvar='TTI')
    

Reformatting meta data for other types or structures
----------------------------------------------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> # changind the column names
    >>> translation = {'nomvar':'fieldName','etiket':'pdsLabel','dateo':'dateOfObservation'}
    >>> df.rename(columns= translation, inplace=True)


Working with data
-----------------

Getting the associated data for each record in the dataframe
------------------------------------------------------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> # we don't want to get all the data so lets get a subset
    >>> uuvv_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (surface==True)')
    >>> tt_df = fstpy.select(df,'(nomvar=="TT") and (surface==True)')
    >>> # get the data for our new dataframes
    >>> # after this operation the 'd' column of each dataframe contains a numpy ndarray
    >>> uuvv_df = fstpy.load_data(uuvv_df)
    >>> tt_df = fstpy.load_data(tt_df)


Performing simple calculations
------------------------------

Wind Modulus
------------

    >>> # building on what we've selected in the previous code snippet
    >>> # first we need the wind modulus (we assume that we have only 1 level in each dataframe)
    >>> # let's separate uu and vv from uuvv_df
    >>> uu_df = fstpy.select(uuvv_df,'nomvar=="UU"')
    >>> vv_df = fstpy.select(uuvv_df,'nomvar=="VV"')
    >>> 
    >>> #let's create a record to hold the result and change the nomvar accordingly
    >>> uv_df = vv_df.copy(deep=True)
    >>> uv_df = fstpy.zap(uv_df, mark=False,nomvar='UV')
    >>> 
    >>> # compute
    >>> uu = (uu_df.at[0,'d']) #at[0,'d'] gets the first row of data from the dataframe
    >>> vv = (vv_df.at[0,'d']) 
    >>> 
    >>> # the algorithm, after this uv_df contains our result for the wind modulus in knots
    >>> uv_df.at[0,'d'] = (uu**2 + vv**2)**.5


Wind Chill
----------

    >>> import fstpy
    >>> import numpy as np
    >>> # at this point we have uv_df and tt_df but uv_df is in knots
    >>> # we need to do a unit conversion on uv_df to get it in kph
    >>> # print(UNITS) to get a list of units
    >>> uv_df = fstpy.do_unit_conversion(uv_df,'kilometer_per_hour')
    >>> 
    >>> # create a record to hold wind chill reseult
    >>> re_df = uv_df.copy(deep=True)
    >>> re_df = fstpy.zap(re_df, mark=False, nomvar='RE')
    >>> 
    >>> # compute            
    >>> tt = (tt_df.iloc[0,'d'])
    >>> uv = (uv_df.iloc[0,'d'])
    >>> 
    >>> # the algorithm, after this re_df contains our result for the wind chill in celsius
    >>> re_df.at[0,'d'] = np.where( (tt <= 0) & (uv >= 5), 13.12 + 0.6215 * tt + ( 0.3965 * tt - 11.37) * ( uv**0.16 ), tt)


Basic statistics for each record in a dataframe
-----------------------------------------------

    >>> import fstpy
    >>> import pandas as pd
    >>> import numpy as np
    >>> # read
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> df = fstpy.select(df,'nomvar=="TT"')
    >>> 
    >>> #load_data
    >>> df = fstpy.load_data(df)
    >>> 
    >>> # function to calculate stats on each row of the dataframe
    >>> # function exists in std.standardfile
    >>> def compute_basic_stats(df:pd.DataFrame) -> pd.DataFrame:
    >>>     """ compute for each row in a datarfame, min, max, mean, standard_deviation and the 2d indice of min and max"""
    >>>     df['min']=None
    >>>     df['max']=None
    >>>     df['mean']=None
    >>>     df['std']=None
    >>>     df['min_pos']=None
    >>>     df['max_pos']=None
    >>>     for i in df.index:
    >>>         df.at[i,'mean'] = df.loc[i,'d'].mean()
    >>>         df.at[i,'std'] = df.loc[i,'d'].std()
    >>>         df.at[i,'min'] = df.loc[i,'d'].min()
    >>>         df.at[i,'max'] = df.loc[i,'d'].max()
    >>>         # index (i,j) of min in record
    >>>         df.at[i,'min_pos'] = np.unravel_index(df.at[i,'d'].argmin(), (df.at[i,'ni'],df.at[i,'nj']))
    >>>         df.at[i,'min_pos'] = (df.at[i,'min_pos'][0] + 1, df.at[i,'min_pos'][1]+1)
    >>>         # index (i,j) of max in record
    >>>         df.at[i,'max_pos'] = np.unravel_index(df.at[i,'d'].argmax(), (df.at[i,'ni'],df.at[i,'nj']))
    >>>         df.at[i,'max_pos'] = (df.at[i,'max_pos'][0] + 1, df.at[i,'max_pos'][1]+1)
    >>>     return df
    >>> 
    >>> # now the dataframe contains extra columns [mean,std,min,max,min_pos,max_pos] with stats for each record in the dataframe 
    >>> df = compute_basic_stats(df)
    >>> # write the result
    >>> from os import getenv
    >>> USER = getenv("USER")
    >>> fstpy.StandardFileWriter('/tmp/%s/row_stats.std'%USER, df)


Basic statistics for each column of 3d matrix
---------------------------------------------

    >>> import fstpy
    >>> import pandas as pd
    >>> import numpy as np
    >>> # read
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> # get TT
    >>> tt_df = fstpy.select(df,'nomvar=="TT"')
    >>> 
    >>> #load_data
    >>> tt_df = fstpy.load_data(tt_df)
    >>> 
    >>> # flatten arrays of the dataframe since second dimension is'nt necessary
    >>> tt_df = fstpy.flatten_data_series(tt_df)
    >>> 
    >>> #get a 3d array of TT
    >>> array_3d = np.stack(tt_df['d'].to_list())
    >>> 
    >>> # gets the min value of every column
    >>> min_arr = np.min(array_3d, axis=0)
    >>> 
    >>> # gets the max value of every column
    >>> max_arr = np.max(array_3d, axis=0)
    >>> 
    >>> # gets the standard deviation value of every column
    >>> std_arr = np.std(array_3d, axis=0)
    >>> 
    >>> # gets the mean value of every column
    >>> mean_arr = np.mean(array_3d, axis=0)
    >>> 
    >>> # creates a 1 row dataframe based on a model dataframe
    >>> def create_result_df(df:pd.DataFrame, nomvar:str, operation_name:str) ->  pd.DataFrame:
    >>>     res_df = fstpy.create_1row_df_from_model(df)
    >>>     res_df = fstpy.zap(res_df, mark=False, nomvar=nomvar, etiket=operation_name)
    >>>     return res_df
    >>> 
    >>> 
    >>> # create result dataframes
    >>> min_df = create_result_df(tt_df,'MIN','MINIMUM')
    >>> max_df = create_result_df(tt_df,'MAX','MAXIMUM')
    >>> std_df = create_result_df(tt_df,'STD','STDDEV')
    >>> mean_df = create_result_df(tt_df,'MEAN','AVERAGE')
    >>> 
    >>> # assign resulting arrays to the dataframes
    >>> # .at gets the row at index in a dataframe, we have 1 row dataframes in each case and our arrays are simple 2d result arrays 
    >>> min_df.at[0,'d'] = min_arr
    >>> max_df.at[0,'d'] = max_arr 
    >>> std_df.at[0,'d'] = std_arr 
    >>> mean_df.at[0,'d'] = mean_arr 
    >>> 
    >>> # combine all results into a single dataframe
    >>> res_df = pd.concat([min_df,max_df,std_df,mean_df])
    >>> 
    >>> # write the result
    >>> from os import getenv
    >>> USER = getenv("USER")
    >>> fstpy.StandardFileWriter('/tmp/%s/column_stats.std'%USER, res_df)()


Getting groups of data
----------------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> tt_df = fstpy.select(df,'nomvar=="TT"')
    >>> 
    >>> # grouping data by grid, the usual case when you have multiple grids in a dataframe
    >>> grid_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=False,group_by_level=False)
    >>> 
    >>> for grid_df in grid_groups_list:
    >>>     print(grid_df)
    >>> 
    >>> # grouping data by forecast hour, the usual case when you have multiple forecast hours per grid in a dataframe
    >>> forecast_hour_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=True,group_by_level=False)
    >>> 
    >>> for fhour_df in forecast_hour_groups_list :
    >>>     print(fhour_df)
    >>> 
    >>> # grouping data by level, the usual case when you have multiple levels per grid in a dataframe
    >>> levels_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=True,group_by_level=True)
    >>> 
    >>> for level_df in levels_groups_list:
    >>>     print(level_df)


Exporting the data
------------------

Formats
-------

With fstpy 

Rpn standard file
-----------------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> # select TT only from input
    >>> tt_df = fstpy.select(df,'nomvar=="TT"')
    >>> 
    >>> # this will write the dataframe to the output file, if no data was fstpy.load_datad, the class will do it
    >>> from os import getenv
    >>> USER = getenv("USER")
    >>> std_file = fstpy.StandardFileWriter('/tmp/%s/TT.std'%USER, tt_df)
    >>> std_file.to_fst()


With [https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html pandas io - many other formats available]

Pickle
------

    >>> import fstpy
    >>> 
    >>> df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    >>> 
    >>> # select TT only from input
    >>> tt_df = fstpy.select(df,'nomvar=="TT"')

    >>> # this will write the complete dataframe to the compressed output file, if no data was fstpy.load_datad no data will be written, 
    >>> # 'd' column will be None
    >>> from os import getenv
    >>> USER = getenv("USER")
    >>> df.to_pickle("/tmp/%s/pickle_data.pkl.gz"%USER)

