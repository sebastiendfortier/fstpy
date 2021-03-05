A pythonic library to work with RPN standard files transformed into
https://pandas.pydata.org/\ *pandas* Dataframes or
http://xarray.pydata.org/en/stable/#\ *xarray*

Requirements
------------

If you don't already have python 3.6 and numpy pandas dask xarray, install usage requirements in a personal conda environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    # This applies for CMC science users
    # create a link in your science home directory to the sitestore to put conda environments, defaults in your home directory (not good)  
    mkdir $sitestore_ppp4/conda/.conda  
    ln -s $sitestore_ppp4/conda/.conda /home/$science-user/.conda  
    # get conda if you don't already have it  
    . ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64   
    # create a conda environment for fstpy's requirements   
    conda create -n fstpy_req python=3.6   
    # whenever you need to use this environment on science run the following (if you have'nt loaded the conda ssm, you'll need to do it everytime)
    # unless you put it in your profile
    . activate fstpy_req   
    # installing required packages in fstpy_req environment  
    conda install -n fstpy_req numpy pandas dask xarray    
    # get rmn python library    
    . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2    
    # you are now ready to use fstpy
    # when you don't wnat to use the environment anymore run the following    
    # conda deactivate    

Using fstpy in scripts or Jupyter Lab/Notebook
----------------------------------------------

set your environment
~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    # activate your conda environment     
    . activate fstpy_req     
    # get rmn python library      
    . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2      
    # get fstpy ssm package
    . ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta-1.0.2      

use fstpy
~~~~~~~~~

.. code:: python

    # inside your script    
    import fstpy.all as fstpy   
    df = fstpy.StandardFileReader('path to my fst file').to_pandas()

Usage example
~~~~~~~~~~~~~

.. code:: python

    data_path = prefix + '/data/'    
    import fstpy.all as fstpy
    # setup your file to read    
    records=fstpy.StandardFileReader(data_path + 'ttuvre.std').to_pandas()    
    # display selected records in a rpn voir format    
    fstpy.voir(records)    
    # get statistics on the selected records    
    df = fstpy.fststat(records)    
    # get a subset of records containing only UU and VV momvar    
    just_tt_and_uv = fstpy.select(records,'nomvar in ["TT","UV"]')    
    # display selected records in a rpn voir format   
    fstpy.voir(just_tt_and_uv)    
    dest_path = '/tmp/out.std'    
    # write the selected records to the output file    
    fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    

Contributing
------------

Creating the developpement environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    # get conda if you don't already have it  
    . ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64   
    # create a conda environment for fstpy's requirements   
    conda create -n fstpy_dev python=3.6   
    # whenever you need to use this environment on science run the following (if you have'nt loaded the conda ssm, you'll need to do it everytime)
    # unless you put it in your profile
    . activate fstpy_dev   
    # installing required packages in fstpy_req environment  
    conda install sphinx
    conda install -c conda-forge sphinx-autodoc-typehints
    conda install -c conda-forge sphinx-gallery
    conda install -c conda-forge sphinx_rtd_theme
    conda install numpy pandas dask xarray pytest

Getting the source code
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    git clone git@gitlab.science.gc.ca:sbf000/fstpy.git
    # create a new branch
    git checkout -b my_change
    # modify the code
    # commit your changes
    # fetch changes
    git fetch
    # merge recent master
    git merge origin master
    # push your changes
    git push origin my_change

Then create a merge request on science's gitlab
https://gitlab.science.gc.ca/sbf000/fstpy/merge_requests

Testing
~~~~~~~

.. code:: bash

    # From the $project_root/test directory of the project
    . activate fstpy_dev    
    # get rmn python library      
    . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2     
    python -m pytest  

Building documentation
~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    # This will build documentation in docs/build and there you will find index.html 
    make clean    
    make html   
    sphinx-build source build 

Release History
---------------

0.0.0 CHANGE: Initial structure of the project 1.0.2 CHANGE: Functional
implementation

Meta
----

Sebastien Fortier sebastien.fortier@canada.ca

Project gitlab
--------------

https://gitlab.science.gc.ca/sbf000/fstpy/

Distributed under the GNU General Public License v3.0 license. See
**LICENSE** for more information.
