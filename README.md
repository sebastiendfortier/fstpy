# Fstpy

A pythonic library to work with RPN standard files transformed into [pandas](https://pandas.pydata.org/) Dataframes or [xarray](http://xarray.pydata.org/en/stable/#)

- [Fstpy](#fstpy)
  * [Requirements](#requirements)
    + [Install requirements in a personal conda environment](#install-requirements-in-a-personal-conda-environment)
  * [Using fstpy in scripts or Jupyter Lab/Notebook](#using-fstpy-in-scripts-or-jupyter-lab-notebook)
    + [set your environment](#set-your-environment)
    + [Usage example](#usage-example)
  * [Testing](#testing)
  * [Documentation - TODO](#documentation---todo)
  * [Release History](#release-history)
  * [Meta](#meta)
      - [Project gitlab](#project-gitlab)
  * [Contributing](#contributing)
      - [Linux](#linux)
  * [Files](#files)
    + [Misc](#misc)
    + [StandardFileReader](#std_reader)
    + [Dictionnaries](#dictionnaries)
    + [Log](#log)
    + [Unit](#unit)
    + [Utils](#utils)


## Requirements
### Install requirements in a personal conda environment
create a link in your science home directory to the sitestore to put conda environments, defaults in your home directory (not good)  
```bash
mkdir $sitestore_ppp4/conda/.conda  
ln -s $sitestore_ppp4/conda/.conda /home/$science-user/.conda  
# get conda:    
. ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64   
# create a conda environment for fstpy   
conda create -n fstpy python=3.6   
# whenever you need to use this environment run the following  
conda activate fstpy    
# installing required packages in fstpy environment  
conda install -n fstpy bottleneck=1.2.1    
conda install -n fstpy pandas=1.1.5    
# when you don't wnat to use the environment anymore run the following    
conda deactivate    
# get rmn python library    
. r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2    
```
## Using fstpy in scripts or Jupyter Lab/Notebook  
### set your environment  
```bash
# activate your conda environment     
conda activate fstpy     
# get rmn python library      
. r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2      
# get fstpy ssm package
. ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta-1.0.1      
```
```python
# inside your script    
import fstpy    
```
### Usage example   
```python
data_path = prefix + '/data/'    
import fstpy
# setup your file to read    
records=fstpy.StandardFileReader(data_path + 'ttuvre.std').to_pandas()    
# display selected records in a rpn voir format    
fstpy.voir(records)    
# get statistics on the selected records    
fstpy.fststat(records)    
# get a subset of records containing only UU and VV momvar    
just_tt_and_uv = fstpy.select(records,'nomvar in ["TT","UV"]')    
# display selected records in a rpn voir format   
fstpy.voir(just_tt_and_uv)    
dest_path = '/tmp/out.std'    
# write the selected records to the output file    
fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    
```

## Testing
From the $project_root/test directory of the project   
```bash
conda activate fstpy    
# get rmn python library      
. r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2     
python -m pytest  
```  

## Documentation - TODO   
This will build documentation in docs/build and there you will find index.html     
> TODO   
> make clean    
> make html   
> sphinx-build source build    

## Release History   
0.0.0    
CHANGE: Initial structure of the project    

## Meta  
Sebastien Fortier  sebastien.fortier@canada.ca    

#### Project gitlab  
[https://gitlab.science.gc.ca/sbf000/fstpy/](https://gitlab.science.gc.ca/sbf000/fstpy/)    

Distributed under the GNU General Public License v3.0 license. See ``LICENSE`` for more information.     

## Contributing
1. Fork it [https://gitlab.science.gc.ca/sbf000/fstpy/fork](https://gitlab.science.gc.ca/sbf000/fstpy/fork)     
2. Create your feature branch (`git checkout -b feature/fooBar`)     
3. Commit your changes (`git commit -am 'Add some fooBar'`)     
4. Push to the branch (`git push origin feature/fooBar`)     
5. Create a new Pull Request.    

#### Linux 
use ssh link to clone the project [fstpy gitlab](https://gitlab.science.gc.ca/sbf000/fstpy/)       
git clone https://gitlab.science.gc.ca/sbf000/fstpy.git       
# inside the project folder     
source setup.sh   
# activate the conda environment that we previously created   
conda activate fstpy   



## Files
### Misc

![How to use as a submodule](misc/use_as_submodule.md)

### StandardFileReader 

![Main fstpy library](fstpy/std_reader.md)

### StandardFileWriter 

![Main fstpy library](fstpy/std_writer.md)

### DataFrame utils 

![Main fstpy library](fstpy/dataframe_utils.md)

### Dictionnaries

![Dictionaries and constants](fstpy/dictionaries.md)
### Log

![Simple logging module](fstpy/log.md)

### Unit

![Unit conversion utility](fstpy/unit.md)

### Utils

![Utilities](fstpy/utils.md)
