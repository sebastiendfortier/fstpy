Introduction
============

What is it?
-----------

Fstpy is a high level interface to rpn's rpnpy python library that
produces pandas dataframes or Xarray's from CMC standard files. In order
to promote decoupling, modularization and collaboration, fstpy only
reads and writes. All other operations and algorithms can be
independent.

Fstpy philosophy
----------------

The idea of ​​using a dataframe is to have a pythonic way of working
with standard files without having to know the mechanics of rmnlib.
Since many people come here with numpy, pandas and xarray knowledge, the
learning curve is much less steep.

Dataframes
----------

They are good for organizing information. eg: select all the tt's, sort
them by grid then by level and produce 3d matrices for each tt of each
grid. Dataframes will help to integrate new model changes and new data
types. Thanks to the dataframes we can also export our results more
easily to different types of formats.


Dask
----

Dask is the type of array that is used by fstpy to wrap numpy arrays for 
parallelisation purposes. You can use most of numpy’s API directly on 
these types of arrays. The only difference is that until array.compute() 
is done, the dask array stores tasks instead of actually doing the 
computations.


Xarray's
--------

They are used to analyse grouped and indexed data. They are especially
good for working with n-dimensional meteorological data. They also offer
a great variety of built-in plotting functions.
