# Introduction

## What is it?

Fstpy is a high level interface to rpn\'s rpnpy python library that
produces pandas dataframes from CMC standard files. In order to promote
decoupling, modularization and collaboration, fstpy only reads and
writes. All other operations and algorithms can be independent.

## Fstpy philosophy

The idea of using a dataframe is to have a pythonic way of working with
standard files without having to know the mechanics of rmnlib. Since
many people come here with numpy, pandas knowledge, the learning curve
is much less steep.

## Dataframes

They are good for organizing information. eg: select all the tt\'s, sort
them by grid then by level and produce 3d matrices for each tt of each
grid. Dataframes will help to integrate new model changes and new data
types. Thanks to the dataframes we can also export our results more
easily to different types of formats.

## Dask

Dask is the type of array that is used by fstpy to wrap numpy arrays for
parallelisation purposes. You can use most of numpy\'s API directly on
these types of arrays. The only difference is that until array.compute()
is done, the dask array stores tasks instead of actually doing the
computations.

# Requirements

## run time packages

-   pandas\>=1.5.1
-   numpy\>=1.24.4
-   dask\>=2023.7.1

## developpement packages

-   ci_fstcomp\>=1.0.9
-   pandas\>=1.5.1
-   numpy\>=1.24.4
-   dask\>=2023.7.1
-   pytest\>=7.4.0
-   sphinx\>=5.3.0
-   sphinx_autodoc_typehints\>=1.21.8
-   sphinx_gallery\>=0.13.0
-   sphinx_rtd_theme\>=0.5.1
-   nbsphinx\>=0.9.2
-   ipython\>=8.14.0
-   jupyterlab\>=3.6.5
-   myst_parser\>=1.0.0

## CMDS Python environment

This is an ssm package that we use at CMC on the science network and
that contains a wide variety of packages

``` bash
# use CMDS Python environment
. ssmuse-sh -p /fs/ssm/eccc/cmd/cmds/env/python/py310_2023.07.28_all
```

# Installation

Use the ssm package

    # FSTPY and dependencies 
    . r.load.dot /fs/ssm/eccc/cmd/cmds/fstpy/bundle/20250300

Use the git repository package: at your own risk ;)

    python3 -m pip install git+http://gitlab.science.gc.ca/CMDS/fstpy.git

## Using fstpy in scripts or Jupyter Lab/Notebook

### Use pre-built developpement environment

``` bash
# use CMDS Python environment
. ssmuse-sh -p /fs/ssm/eccc/cmd/cmds/env/python/py310_2023.07.28_all
# FSTPY and dependencies 
. r.load.dot /fs/ssm/eccc/cmd/cmds/fstpy/bundle/20250300
```

### Use fstpy

``` python
# inside your script    
import fstpy   
df = fstpy.StandardFileReader('path to my fst file').to_pandas()
```

### Example

``` python
data_path = prefix + '/data/'    
import fstpy
# setup your file to read    
records=fstpy.StandardFileReader(data_path + 'ttuvre.std').to_pandas()    
# display selected records in a rpn voir format    
fstpy.voir(records)    
# get statistics on the selected records    
df = fstpy.fststat(records)    
# get a subset of records containing only UU and VV momvar    
just_tt_and_uv = records.query('nomvar in ["TT","UV"]')    
# display selected records in a rpn voir format   
fstpy.voir(just_tt_and_uv)    
dest_path = '/tmp/out.std'    
# write the selected records to the output file    
fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    
```

# Contributing

## Getting the source code

``` bash
git clone git@gitlab.science.gc.ca:cmds/fstpy.git  
# create a new branch  
git checkout -b my_change  
# modify the code  
# commit your changes  
# fetch changes  
git fetch  
# merge recent master  
git merge origin/master  
# push your change  
git push my_change  
```

Then create a merge request on science\'s gitlab
<https://gitlab.science.gc.ca/CMDS/fstpy/merge_requests>

## Using setup.sh to setup your developpement environment

``` bash
# From the $project_root directory of the project
source setup.sh  
```

## Testing

``` bash
# use CMDS Python environment
. ssmuse-sh -p /fs/ssm/eccc/cmd/cmds/env/python/py310_2023.07.28_all
# FSTPY and dependencies 
. r.load.dot /fs/ssm/eccc/cmd/cmds/fstpy/bundle/20250300
# get ci_fstcomp 
. ssmuse-sh -d  /fs/ssm/eccc/cmd/cmds/apps/ci_fstcomp/202501/00
# From the $project_root directory of the project
python -m pytest -vrf  
```

## Building documentation

``` bash
# This will build documentation in docs/build and there you will find index.html  
cd doc  
make clean 
make doc  
```

# Release

## Update release version number

``` bash
Update the version number in fstpy/__init__.py 
```

## Update configuration files

``` bash
Update the following configuration files:

   * cmds_python_env.txt:  CMDS python environment
   * ci_fstcomp.txt 
```

## Build documentation

``` bash
cd   doc  
make clean 
make doc  
```

## Commit changes

``` bash
Commit the following files:

* README.md
* ci_fstcomp.txt
```

# Creating the ssm package

``` bash
cd livraison
./make_ssm_package.py
```

# Acknowledgements

Great thanks to:

-   [Phillipe Carphin](mailto:Phillipe.Carphin2@canada.ca) for inspiring
    the use of pandas.
-   [Dominik Jacques](mailto:Dominik.Jacques@canada.ca) for the awsome
    domUtils project, a great structure of what should be a python
    project.
-   [Micheal Neish](mailto:Micheal.Neish@canada.ca) for the awsome
    fstd2nc project, great insights on how to develop xarray structure
    from CMC standard files and great functions to work on fst files. He
    played a pivotal role in the integration of dask into fstpy.
