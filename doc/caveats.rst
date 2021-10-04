Caveats
=======

Dask comes with some subtle requirements.

The d column
------------

In the 'd' column of the dataframe you wil notice the stored arrays are of dask.array type   
upon reading the file.  

Obtain a numpy array
--------------------

To obtain a numpy array, most of the time all you need to do is use the compute member function    

Getting the right type of array
-------------------------------

It's a little bit hard to keep track of when the 'd' column is a numpy array    
or a dask array while processing the data of a dataframe. For this situation I made the    
utility functions to_numpy and to_dask. Both of these check the instance type and return    
the appropriate requested type. Ex.: if arr is a numpy.array, a call to to_numpy is a no op.   
Otherwise, a .compute() is done to obtain the numpy array from the dask array.   

Numpy operations exceptions
---------------------------

Most numpy operations will be working out of the box, here are the exceptions i found.   

One dimension arrays
~~~~~~~~~~~~~~~~~~~~

When working with arrays of shape (1, x)  or (x, 1), dask seems to interpret then as beeing 1d.   
My woraround for this is to compute() the dask arrays to obtain numpy arrays for operations   
that need to be done on them.   

arg functions
~~~~~~~~~~~~~

numpy.argmax and the others of that family seem to be a little bit of a problem for dask.     
Dask does supply its own versions that can be called with dask.array.argmax as an example.       
My woraround for this is to compute() the dask arrays to obtain numpy arrays for these     
operations.

Printing the dataframe
~~~~~~~~~~~~~~~~~~~~~~

Since the 'd' column contains dask arrays, diplaying the contents of the dataframe is very slow.      
The solution is to print the dataframe without the d column.    
  
.. code:: python
   
    cols = list(df.columns)   
    cols.remove('d')   
    print(df[cols])   
    
    