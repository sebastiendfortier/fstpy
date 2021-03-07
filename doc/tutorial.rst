Tutorial
========

Getting and manipulating the meta data
--------------------------------------

Get a dataframe from a standard file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    # method 1 - to_pandas with explicit instance
    std_file = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std')
    df = std_file.to_pandas()

    # method 2 - to_pandas without explicit instance
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()

Get a dataframe from multiple standard files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader(['/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',etc...]).to_pandas()

See the contents of the dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',load_data=True,subset={'nomvar':'TT'}).to_pandas()
    # show the last rows of the dataframe
    print(df.tail())

.. code:: example

       nomvar typvar  ...                                               path        grid
    80     TT      P  ...  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_...  3379277761
    81     TT      P  ...  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_...  3379277761
    82     TT      P  ...  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_...  3379277761
    83     TT      P  ...  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_...  3379277761
    84     TT      P  ...  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_...  3379277761

    [5 rows x 26 columns]

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # show column names of the dataframe
    print(df.columns)

.. code:: example

    Index(['nomvar', 'typvar', 'etiket', 'ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2',
           'ip3', 'deet', 'npas', 'datyp', 'nbits', 'grtyp', 'ig1', 'ig2', 'ig3',
           'ig4', 'grid', 'datev', 'key', 'path', 'shape',
           'file_modification_time', 'd'],
          dtype='object')

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    # show the levels contained in the dataframe
    print(df.level)

.. code:: example

    0       0.997502
    1       0.992524
    2       0.986026
    3       0.977868
    4       0.968043
          ...   
    1869    0.000206
    1870    0.000128
    1871   -1.000000
    1872   -1.000000
    1873   -1.000000
    Name: level, Length: 1874, dtype: float32

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    # show the unique levels contained in the dataframe
    print(df.level.unique())

.. code:: example

    [ 9.97502e-01  9.92524e-01  9.86026e-01  9.77868e-01  9.68043e-01
      9.56665e-01  9.43925e-01  9.30004e-01  9.14966e-01  8.98642e-01
      8.80856e-01  8.61601e-01  8.40901e-01  8.18815e-01  7.95438e-01
      7.70904e-01  7.45386e-01  7.19089e-01  6.92242e-01  6.64956e-01
      ...
      1.57240e-02  1.31560e-02  1.08590e-02  8.82800e-03  7.05400e-03
      5.53100e-03  4.24500e-03  3.18500e-03  2.33100e-03  1.66300e-03
      1.15700e-03  7.83000e-04  5.14000e-04  3.28000e-04  2.06000e-04
      1.28000e-04  6.00000e+00  1.00000e+01]

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # show a subset of columns of the dataframe
    print(df[['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3']])

.. code:: example

     nomvar typvar     etiket    ni    nj  nk      dateo       ip1    ip2  ip3
    0        ^^      X  R1_V710_N     1  1081   1  442998800     50460  53326    4
    1        ^^      X  R1_V710_N     1  1078   1  442998800     35132  56748    1
    2        ^^      X  R1_V710_N     1  1082   1  442998800     33792  77761    1
    3        ZZ      P  R1_V710_N  1108  1082   1  442998800  95791989      6    0
    4        ZZ      P  R1_V710_N  1108  1082   1  442998800  94486466      6    0
    ...     ...    ...        ...   ...   ...  ..        ...       ...    ...  ...
    1869     >>      X  R1_V710_N  1104     1   1  442998800     35132  56748    1
    1870     >>      X  R1_V710_N  1108     1   1  442998800     33792  77761    1
    1871     5P      P  R1_V710_N  1104  1078   1  442998800         0      6    0
    1872     !!      X  R1_V710_N     3   175   1          0     33792  77761    0
    1873     !!      X  R1_V710_N     3   175   1          0     35132  56748    0

    [1874 rows x 10 columns]

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # show a voir like output of the dataframe
    fstpy.voir(df.head())

.. code:: example

      nomvar typvar     etiket    ni    nj  nk               dateo       ip1    ip2  ip3  deet  npas datyp  nbits grtyp    ig1    ig2    ig3    ig4     level    
    0     ZZ      P  R1_V710_N  1108  1082   1 2020-07-14 12:00:00  95791989      6    0   300    72     f     12     Z  33792  77761      1      0  0.037157  hy
    1     ZZ      P  R1_V710_N  1108  1082   1 2020-07-14 12:00:00  94486466      6    0   300    72     f     12     Z  33792  77761      1      0  0.114626  hy
    2     ^^      X  R1_V710_N     1  1081   1 2020-07-14 12:00:00     50460  53326    4     0     0     E     32     E   1470    560  54400  46560        -1    
    3     ^^      X  R1_V710_N     1  1078   1 2020-07-14 12:00:00     35132  56748    1     0     0     E     32     E   1470    560  54400  46560        -1    
    4     ^^      X  R1_V710_N     1  1082   1 2020-07-14 12:00:00     33792  77761    1     0     0     E     32     E   1470    560  54400  46560        -1    

select sub-sets of data
~~~~~~~~~~~~~~~~~~~~~~~

**Note**: fstpy.select is a wrapper for pandas.DataFrame.query method

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',load_data=True).to_pandas()
    # select TT
    sel_tt_df = fstpy.select(df,'nomvar=="TT"')
    print(sel_tt_df.head())

.. code:: example

      nomvar typvar     etiket  ...      datev        grid  file_modification_time
    0     TT      P  R1_V710_N  ...  443004200  3379277761     2021-01-26 09:31:54
    1     TT      P  R1_V710_N  ...  443004200  3379277761     2021-01-26 09:31:54
    2     TT      P  R1_V710_N  ...  443004200  3379277761     2021-01-26 09:31:54
    3     TT      P  R1_V710_N  ...  443004200  3379277761     2021-01-26 09:31:54
    4     TT      P  R1_V710_N  ...  443004200  3379277761     2021-01-26 09:31:54

    [5 rows x 26 columns]

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # select UU and VV
    sel_uuvv_df = fstpy.select(df,'nomvar in ["UU","VV"]')
    print(sel_uuvv_df.head())
    print(sel_uuvv_df.tail())

.. code:: example

      nomvar typvar     etiket  ...  file_modification_time        grid            shape
    0     VV      P  R1_V710_N  ...     2021-01-26 09:31:54  3379277761  (1108, 1082, 1)
    1     VV      P  R1_V710_N  ...     2021-01-26 09:31:54  3379277761  (1108, 1082, 1)
    2     VV      P  R1_V710_N  ...     2021-01-26 09:31:54  3379277761  (1108, 1082, 1)
    3     VV      P  R1_V710_N  ...     2021-01-26 09:31:54  3379277761  (1108, 1082, 1)
    4     VV      P  R1_V710_N  ...     2021-01-26 09:31:54  3379277761  (1108, 1082, 1)

    [5 rows x 26 columns]
    nomvar typvar  ...        grid            shape
    165     UU      P  ...  3379277761  (1108, 1082, 1)
    166     UU      P  ...  3379277761  (1108, 1082, 1)
    167     UU      P  ...  3379277761  (1108, 1082, 1)
    168     UU      P  ...  3379277761  (1108, 1082, 1)
    169     UU      P  ...  3379277761  (1108, 1082, 1)

    [5 rows x 26 columns]

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # select UU and VV with ip2 of 6
    sel_uuvv6_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (ip2==6)')
    print(sel_uuvv6_df.tail()[['nomvar','ip2']])

.. code:: example

        nomvar  ip2
    165     UU    6
    166     UU    6
    167     UU    6
    168     UU    6
    169     UU    6

selecting by date range
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from datetime import date,datetime
    import fstpy.all as fstpy
    import pandas as pd
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    start_date = datetime(2020, 7, 14)
    end_date = datetime(2020, 7, 15)
    print(start_date,end_date)
    df['date_of_observation'] = pd.to_datetime(df['date_of_observation'])
    mask = df['date_of_observation'].between(start_date, end_date, inclusive=True)
    sub_df = df[mask]
    print(sub_df.head().sort_values(by=['date_of_observation']))

Modify meta data
~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()
    # select TT
    sel_tt_df = fstpy.select(df,'nomvar=="TT"')
    # change nomvar from TT to TTI
    zapped_df = fstpy.zap(sel_tt_df,nomvar='TTI')
    print(zapped_df.head())

.. code:: example

      nomvar typvar  ...      key            shape
    0    TTI      P  ...  1263617  (1108, 1082, 1)
    1    TTI      P  ...   222209  (1108, 1082, 1)
    2    TTI      P  ...  1092609  (1108, 1082, 1)
    3    TTI      P  ...  1093633  (1108, 1082, 1)
    4    TTI      P  ...    23553  (1108, 1082, 1)

    [5 rows x 26 columns]

Reformatting meta data for other types or structures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    # changind the column names
    translation = {'nomvar':'fieldName','etiket':'pdsLabel','dateo':'dateOfObservation'}
    df.rename(columns=translation, inplace=True)
    print(df[['fieldName','pdsLabel','dateOfObservation']])

.. code:: example

     fieldName   pdsLabel  dateOfObservation
    0           QR  R1_V710_N          442998800
    1           QR  R1_V710_N          442998800
    2           QR  R1_V710_N          442998800
    3           QR  R1_V710_N          442998800
    4           QR  R1_V710_N          442998800
    ...        ...        ...                ...
    1869        ES  R1_V710_N          442998800
    1870        ES  R1_V710_N          442998800
    1871        ^^  R1_V710_N          442998800
    1872        >>  R1_V710_N          442998800
    1873        !!  R1_V710_N                  0

    [1874 rows x 3 columns]

Working with data
-----------------

Getting the associated data for each record in the dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    # we don't want to get all the data so lets get a subset
    uuvv_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (surface==True)')
    print(uuvv_df.head())
    tt_df = fstpy.select(df,'(nomvar=="TT") and (surface==True)')
    print(tt_df.head())
    # get the data for our new dataframes
    # after this operation the 'd' column of each dataframe contains a numpy ndarray
    uuvv_df = fstpy.load_data(uuvv_df)
    tt_df = fstpy.load_data(tt_df)
    print(tt_df[['nomvar','d']].head())

.. code:: example

      nomvar typvar     etiket    ni  ...  zapped  ip2_dec      datev  level
    0     VV      P  R1_V710_N  1108  ...   False      6.0  443004200   10.0
    1     UU      P  R1_V710_N  1108  ...   False      6.0  443004200   10.0

    [2 rows x 51 columns]
      nomvar typvar     etiket    ni  ...  zapped  ip2_dec      datev  level
    0     TT      P  R1_V710_N  1108  ...   False      6.0  443004200    1.5

    [1 rows x 51 columns]
      nomvar                                                  d
    0     TT  [[26.068878, 26.084503, 26.108917, 26.167511, ...

Performing simple calculations
------------------------------

Wind Modulus
~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    uuvv_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (surface==True)')
    uuvv_df = fstpy.load_data(uuvv_df)
    # first we need the wind modulus (we assume that we have only 1 level in each dataframe)
    # let's separate uu and vv from uuvv_df
    uu_df = fstpy.select(uuvv_df,'nomvar=="UU"')
    vv_df = fstpy.select(uuvv_df,'nomvar=="VV"')

    #let's create a record to hold the result and change the nomvar accordingly
    uv_df = vv_df.copy(deep=True)
    uv_df = fstpy.zap(uv_df,nomvar='UV')

    # compute
    uu = (uu_df.at[0,'d']) #at[0,'d'] gets the first row of data from the dataframe
    vv = (vv_df.at[0,'d']) 
    print('UU',uu)
    print('VV',vv)

    # the algorithm, after this uv_df contains our result for the wind modulus in knots
    uv_df.at[0,'d'] = (uu**2 + vv**2)**.5
    print(uv_df[['nomvar','d']])

.. code:: example

    UU [[-6.270401  -6.6483307 -6.9207916 ... -2.714737  -3.1170807 -3.4950104]
     [-6.3768463 -6.7743073 -7.084854  ... -2.951065  -3.0487213 -3.2401276]
     [-6.4569244 -6.8631744 -6.772354  ... -2.9207916 -2.982315  -3.0077057]
     ...
     [-3.9051666 -4.495987  -3.2821198 ... 16.506943  18.963974  18.807724 ]
     [-4.511612  -4.9618073 -4.6668854 ... 19.469833  19.06163   18.809677 ]
     [-5.322159  -5.699112  -5.058487  ... 19.53624   19.284286  18.87413  ]]
    VV [[18.56651    19.05479    19.56065    ... 16.365337   15.879009
      15.267681  ]
     [18.363384   18.89854    19.451275   ... 18.0919     17.53526
      16.802837  ]
     [18.183697   18.763775   18.240337   ... 18.511822   19.107525
      18.328228  ]
     ...
     [ 3.8301811   0.42588425 -1.4159126  ... -1.7069283  -2.0233345
      -1.9432564 ]
     [ 3.7754936   0.9708061  -1.281147   ... -2.1014595  -2.0487251
      -2.1034126 ]
     [ 3.3067436   1.390728   -0.0858345  ... -2.0838814  -2.0663033
      -2.3026314 ]]
      nomvar                                                  d
    0     UV  [[19.596766, 20.181313, 20.748888, 21.276947, ...

Wind Chill
~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    import numpy as np
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()
    uuvv_df = fstpy.select(df,'(nomvar in ["UU","VV"]) and (surface==True)')
    uuvv_df = fstpy.load_data(uuvv_df)
    uu_df = fstpy.select(uuvv_df,'nomvar=="UU"')
    vv_df = fstpy.select(uuvv_df,'nomvar=="VV"')
    uv_df = vv_df.copy(deep=True)
    uv_df = fstpy.zap(uv_df,nomvar='UV')
    uu = (uu_df.iloc[0]['d']) #iloc[0]['d'] gets the first row of data from the dataframe
    vv = (vv_df.iloc[0]['d']) 
    uv_df.at[0,'d'] = (uu**2 + vv**2)**.5
    tt_df = fstpy.select(df,'(nomvar=="TT") and (surface==True)')
    tt_df = fstpy.load_data(uuvv_df)
    # at this point we have uv_df and tt_df but uv_df is in knots
    # we need to do a unit conversion on uv_df to get it in kph
    # print(UNITS) to get a list of units
    uv_df = fstpy.do_unit_conversion(uv_df,'kilometer_per_hour')

    # create a record to hold wind chill reseult
    re_df = uv_df.copy(deep=True)
    re_df = fstpy.zap(re_df, nomvar='RE')

    # compute            
    tt = (tt_df.iloc[0]['d'])
    uv = (uv_df.iloc[0]['d'])

    # the algorithm, after this re_df contains our result for the wind chill in celsius
    re_df.at[0,'d'] = np.where( (tt <= 0) & (uv >= 5), 13.12 + 0.6215 * tt + ( 0.3965 * tt - 11.37) * ( uv**0.16 ), tt)
    print(re_df.head()[['nomvar','d']])

.. code:: example

      nomvar                                                  d
    0     RE  [[18.566509246826172, 19.054790496826172, 19.5...

Basic statistics for each record in a dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    import pandas as pd
    import numpy as np
    # read
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()

    df = fstpy.select(df,'nomvar=="TT"')

    #load_data
    df = fstpy.load_data(df)

    # function to calculate stats on each row of the dataframe
    # function exists in std.standardfile
    def compute_basic_stats(df:pd.DataFrame) -> pd.DataFrame:
        """ compute for each row in a datarfame, min, max, mean, standard_deviation and the 2d indice of min and max"""
        df['min']=None
        df['max']=None
        df['mean']=None
        df['std']=None
        df['min_pos']=None
        df['max_pos']=None
        for i in df.index:
            df.at[i,'mean'] = df.loc[i,'d'].mean()
            df.at[i,'std'] = df.loc[i,'d'].std()
            df.at[i,'min'] = df.loc[i,'d'].min()
            df.at[i,'max'] = df.loc[i,'d'].max()
            # index (i,j) of min in record
            df.at[i,'min_pos'] = np.unravel_index(df.at[i,'d'].argmin(), (df.at[i,'ni'],df.at[i,'nj']))
            df.at[i,'min_pos'] = (df.at[i,'min_pos'][0] + 1, df.at[i,'min_pos'][1]+1)
            # index (i,j) of max in record
            df.at[i,'max_pos'] = np.unravel_index(df.at[i,'d'].argmax(), (df.at[i,'ni'],df.at[i,'nj']))
            df.at[i,'max_pos'] = (df.at[i,'max_pos'][0] + 1, df.at[i,'max_pos'][1]+1)
        return df

    # now the dataframe contains extra columns [mean,std,min,max,min_pos,max_pos] with stats for each record in the dataframe 
    df = compute_basic_stats(df)
    print(df.head())

.. code:: example

      nomvar typvar     etiket    ni  ...     mean      std      min_pos     max_pos
    0     TT      P  R1_V710_N  1108  ...  9.62213  7.16631   (905, 751)  (631, 280)
    1     TT      P  R1_V710_N  1108  ... -9.47461  8.59939    (131, 66)  (818, 860)
    2     TT      P  R1_V710_N  1108  ... -41.0226  3.84673  (1009, 231)  (815, 967)
    3     TT      P  R1_V710_N  1108  ...  -43.062   3.9307    (655, 96)  (814, 782)
    4     TT      P  R1_V710_N  1108  ... -40.4699  5.70201   (413, 863)  (108, 244)

    [5 rows x 32 columns]

Basic statistics for each column of 3d matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy
    import pandas as pd
    import numpy as np
    # read
    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()

    # get TT
    tt_df = fstpy.select(df,'nomvar=="TT"')

    #load_data
    tt_df = fstpy.load_data(tt_df)

    # flatten arrays of the dataframe since second dimension is'nt necessary
    tt_df = fstpy.flatten_data_series(tt_df)

    #get a 3d array of TT
    array_3d = np.stack(tt_df['d'].to_list())

    # gets the min value of every column
    min_arr = np.min(array_3d, axis=0)

    # gets the max value of every column
    max_arr = np.max(array_3d, axis=0)

    # gets the standard deviation value of every column
    std_arr = np.std(array_3d, axis=0)

    # gets the mean value of every column
    mean_arr = np.mean(array_3d, axis=0)

    # creates a 1 row dataframe based on a model dataframe
    def create_result_df(df:pd.DataFrame, nomvar:str, operation_name:str) ->  pd.DataFrame:
        res_df = fstpy.create_1row_df_from_model(df)
        res_df = fstpy.zap(res_df, nomvar=nomvar, etiket=operation_name)
        return res_df


    # create result dataframes
    min_df = create_result_df(tt_df,'MIN','MINIMUM')
    max_df = create_result_df(tt_df,'MAX','MAXIMUM')
    std_df = create_result_df(tt_df,'STD','STDDEV')
    mean_df = create_result_df(tt_df,'MEAN','AVERAGE')

    # assign resulting arrays to the dataframes
    # .at gets the row at index in a dataframe, we have 1 row dataframes in each case and our arrays are simple 2d result arrays 
    min_df.at[0,'d'] = min_arr
    max_df.at[0,'d'] = max_arr 
    std_df.at[0,'d'] = std_arr 
    mean_df.at[0,'d'] = mean_arr 

    # combine all results into a single dataframe
    res_df = pd.concat([min_df,max_df,std_df,mean_df])
    print(res_df.to_string())

.. code:: example

      nomvar typvar   etiket    ni    nj  nk      dateo       ip1  ip2  ip3  deet  npas  datyp  nbits grtyp    ig1    ig2  ig3  ig4                                                    path      datev   key                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    d        grid file_modification_time            shape
    0    MIN      P  MINIMUM  1108  1082   1  442998800  95178882    6    0   300    72    134     16     Z  33792  77761    1    0  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std  443004200  None                                                                   [-78.92189, -78.9592, -78.99924, -79.03244, -79.056854, -79.079315, -79.09396, -79.11447, -79.13791, -79.1633, -79.19357, -79.25412, -79.32443, -79.38791, -79.451385, -79.49631, -79.52463, -79.54025, -79.55295, -79.58029, -79.616425, -79.66916, -79.732635, -79.79904, -79.860565, -79.91916, -79.97287, -80.02463, -80.07248, -80.1174, -80.15256, -80.18869, -80.229706, -80.263885, -80.29318, -80.32541, -80.351776, -80.37814, -80.40451, -80.43283, -80.46408, -80.49826, -80.53732, -80.57248, -80.60275, -80.62619, -80.63986, -80.64377, -80.635956, -80.619354, -80.603226, -80.61397, -80.61592, -80.606155, -80.61447, -80.619354, -80.60959, -80.59299, -80.576385, -80.56564, -80.55002, -80.52463, -80.48361, -80.431854, -80.372284, -80.30783, -80.321976, -80.3542, -80.38643, -80.41963, -80.45772, -80.50264, -80.54854, -80.59053, -80.62471, -80.65889, -80.70772, -80.78096, -80.864944, -80.927444, -80.94893, -80.92842, -80.89424, -80.86299, -80.84639, -80.853226, -80.86104, -80.86104, -80.8669, -80.87862, -80.896194, -80.9294, -80.97334, -81.02217, -81.07686, -81.13545, -81.196976, -81.26338, -81.32979, -81.39229, ...]  3379277761                   None  (1108, 1082, 1)
    0    MAX      P  MAXIMUM  1108  1082   1  442998800  95178882    6    0   300    72    134     16     Z  33792  77761    1    0  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std  443004200  None                                                                                                             [26.068878, 26.084503, 26.108917, 26.167511, 26.257141, 26.315735, 26.308136, 26.211456, 25.935852, 25.399933, 24.629425, 24.08255, 23.844269, 23.775696, 23.76593, 23.769073, 23.780792, 23.802277, 24.026886, 24.79132, 25.70169, 26.218292, 26.405792, 26.394073, 26.24173, 26.037415, 25.847198, 25.694855, 25.592316, 25.583527, 25.693878, 25.904602, 26.14093, 26.280792, 26.328644, 26.313995, 26.299347, 26.30597, 26.320618, 26.318878, 26.283722, 26.224152, 26.167511, 26.140167, 26.090149, 25.971008, 25.77179, 25.49759, 25.252472, 25.056183, 24.901886, 24.773956, 24.689972, 24.68509, 24.772003, 24.931183, 25.116516, 25.27298, 25.36966, 25.382355, 25.390167, 25.412628, 25.364777, 25.199738, 24.9263, 24.71048, 24.62259, 24.729034, 25.169464, 25.765167, 26.27591, 26.45169, 26.317902, 25.990753, 25.506378, 24.945831, 24.410675, 24.064972, 23.92511, 23.90384, 23.918488, 23.944855, 24.023743, 24.14212, 24.317902, 24.632141, 25.02298, 25.401886, 25.559113, 25.5513, 25.448761, 25.335266, 25.23587, 25.163605, 25.118683, 25.095032, 25.081573, 25.052277, 24.986633, 24.863586, ...]  3379277761                   None  (1108, 1082, 1)
    0    STD      P   STDDEV  1108  1082   1  442998800  95178882    6    0   300    72    134     16     Z  33792  77761    1    0  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std  443004200  None                                                                                                                                [34.22977, 34.22602, 34.22484, 34.229103, 34.237286, 34.243885, 34.238552, 34.22126, 34.169865, 34.090595, 34.013687, 33.979652, 33.982372, 33.98839, 34.000988, 34.006035, 34.00653, 34.002956, 34.018955, 34.084297, 34.177204, 34.258194, 34.301727, 34.314342, 34.31075, 34.297604, 34.27965, 34.25857, 34.23795, 34.23012, 34.24451, 34.275784, 34.307976, 34.33188, 34.338326, 34.332478, 34.326862, 34.324238, 34.324715, 34.327267, 34.327152, 34.32214, 34.315323, 34.313572, 34.30869, 34.295338, 34.27449, 34.24837, 34.22067, 34.19538, 34.177017, 34.161922, 34.15302, 34.15831, 34.170414, 34.185104, 34.198624, 34.21237, 34.224995, 34.23313, 34.231167, 34.21853, 34.203033, 34.1827, 34.160732, 34.156326, 34.170128, 34.20103, 34.251003, 34.30613, 34.349556, 34.369537, 34.358547, 34.31719, 34.24424, 34.167507, 34.13415, 34.131977, 34.15164, 34.178402, 34.19677, 34.202263, 34.20206, 34.197693, 34.19628, 34.218697, 34.256077, 34.29766, 34.3214, 34.329494, 34.327774, 34.33234, 34.339035, 34.3427, 34.341774, 34.33401, 34.31786, 34.29769, 34.275074, 34.250896, ...]  3379277761                   None  (1108, 1082, 1)
    0   MEAN      P  AVERAGE  1108  1082   1  442998800  95178882    6    0   300    72    134     16     Z  33792  77761    1    0  /fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std  443004200  None  [-26.538721, -26.543379, -26.546425, -26.546188, -26.542845, -26.53976, -26.544678, -26.557447, -26.594276, -26.649767, -26.701962, -26.72356, -26.721125, -26.717592, -26.71, -26.707247, -26.707603, -26.709988, -26.69944, -26.657293, -26.597906, -26.54439, -26.515244, -26.506212, -26.507097, -26.514818, -26.526806, -26.541588, -26.55631, -26.562681, -26.553864, -26.53313, -26.51206, -26.496609, -26.493168, -26.498043, -26.502571, -26.50523, -26.505661, -26.504059, -26.503473, -26.506207, -26.511078, -26.512217, -26.51534, -26.524021, -26.537653, -26.554794, -26.573309, -26.59061, -26.603525, -26.614422, -26.621218, -26.618534, -26.61148, -26.602444, -26.594223, -26.585602, -26.577393, -26.571579, -26.572733, -26.581533, -26.591978, -26.605179, -26.61912, -26.620504, -26.609562, -26.587952, -26.554398, -26.519053, -26.49326, -26.480333, -26.486721, -26.515226, -26.566519, -26.620028, -26.640518, -26.640284, -26.626255, -26.608137, -26.596315, -26.593777, -26.595034, -26.601124, -26.606384, -26.591473, -26.565317, -26.536768, -26.519413, -26.512785, -26.513681, -26.508263, -26.501232, -26.497831, -26.498922, -26.505598, -26.518724, -26.53443, -26.551273, -26.568586, ...]  3379277761                   None  (1108, 1082, 1)

Getting groups of data
~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fstpy.all as fstpy

    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()

    tt_df = fstpy.select(df,'nomvar in ["TT","QR"]')

    # grouping data by grid, the usual case when you have multiple grids in a dataframe
    grid_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=False,group_by_level=False)

    for grid_df in grid_groups_list:
        print(grid_df.head()[['nomvar','grid']])

.. code:: example

    nomvar        grid
    168     TT  3379277761
    167     TT  3379277761
    166     TT  3379277761
    165     TT  3379277761
    164     TT  3379277761
       nomvar        grid
    83     QR  5046053326
    82     QR  5046053326
    81     QR  5046053326
    80     QR  5046053326
    79     QR  5046053326

.. code:: python

    import fstpy.all as fstpy

    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()

    tt_df = fstpy.select(df,'nomvar in ["TT",">>"]')

    # grouping data by forecast hour, the usual case when you have multiple forecast hours per grid in a dataframe
    forecast_hour_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=True,group_by_level=False)

    for forecast_hour_df in forecast_hour_groups_list :
        print(forecast_hour_df.head())

.. code:: example

       nomvar typvar     etiket  ...   label  date_of_observation  ip2_kind
    87     >>      X  R1_V710_N  ...  _V710_  2020-07-14 12:00:00        -1

    [1 rows x 51 columns]
       nomvar typvar     etiket  ...   label  date_of_observation  ip2_kind
    86     TT      P  R1_V710_N  ...  _V710_  2020-07-14 12:00:00         2
    85     TT      P  R1_V710_N  ...  _V710_  2020-07-14 12:00:00         2
    84     TT      P  R1_V710_N  ...  _V710_  2020-07-14 12:00:00         2
    83     TT      P  R1_V710_N  ...  _V710_  2020-07-14 12:00:00         2
    82     TT      P  R1_V710_N  ...  _V710_  2020-07-14 12:00:00         2

    [5 rows x 51 columns]
      nomvar typvar     etiket  ...   label  date_of_observation  ip2_kind
    1     >>      X  R1_V710_N  ...  _V710_  2020-07-14 12:00:00        -1

    [1 rows x 51 columns]
      nomvar typvar     etiket  ...   label  date_of_observation  ip2_kind
    0     >>      X  R1_V710_N  ...  _V710_  2020-07-14 12:00:00        -1

    [1 rows x 51 columns]

.. code:: python

    import fstpy.all as fstpy

    df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std',decode_metadata=True).to_pandas()

    tt_df = fstpy.select(df,'nomvar in ["TT","UU","VV"]')

    # grouping data by level, the usual case when you have multiple levels per grid in a dataframe
    levels_groups_list = fstpy.get_groups(tt_df,group_by_forecast_hour=True,group_by_level=True)

    for level_df in levels_groups_list:
        print(level_df.head()[['nomvar','level']])

.. code:: example

    nomvar     level
    169     UU  0.000101
    84      VV  0.000101
    nomvar     level
    254     TT  0.000128
    nomvar     level
    168     UU  0.000163
    83      VV  0.000163
    nomvar     level
    253     TT  0.000206
    nomvar     level
    167     UU  0.000261
    82      VV  0.000261
    ...

Exporting the data
------------------

Formats
~~~~~~~

With fstpy

#. Rpn standard file

   .. code:: python

       import fstpy.all as fstpy

       df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()

       # select TT only from input
       tt_df = fstpy.select(df,'nomvar=="TT"')

       # this will write the dataframe to the output file, if no data was fstpy.load_datad, the class will do it
       from os import getenv
       USER = getenv("USER")
       std_file = fstpy.StandardFileWriter('/tmp/%s/TT.std'%USER, tt_df)
       std_file.to_fst()

   With `pandas io - many other formats
   available <https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html>`__

#. Pickle

   .. code:: python

       import fstpy.all as fstpy

       df = fstpy.StandardFileReader('/fs/site4/eccc/cmd/w/sbf000/fstpy/source_data_5005.std').to_pandas()

       # select TT only from input
       tt_df = fstpy.select(df,'nomvar=="TT"')

       # this will write the complete dataframe to the compressed output file, if no data was fstpy.load_datad no data will be written, 
       # 'd' column will be None
       from os import getenv
       USER = getenv("USER")
       df.to_pickle("/tmp/%s/pickle_data.pkl.bz2"%USER)
