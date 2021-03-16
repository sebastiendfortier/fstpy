.. raw:: org

   #+TITLE_: USAGE

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
   . ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta-1.0.3      

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
