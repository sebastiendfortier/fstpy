Using fstpy in scripts or Jupyter Lab/Notebook
----------------------------------------------

Use pre-built developpement environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   # use surgepy      
   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmde/surge/surgepy/1.0.8/      
   # get rmn python library      
   . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2      
   # get fstpy ssm package
   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/fstpy/2.1.10/

Use fstpy
~~~~~~~~~

.. code:: python

   # inside your script    
   import fstpy.all as fstpy   
   df = fstpy.StandardFileReader('path to my fst file').to_pandas()

Example
~~~~~~~

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
   just_tt_and_uv = records.query('nomvar in ["TT","UV"]')    
   # display selected records in a rpn voir format   
   fstpy.voir(just_tt_and_uv)    
   dest_path = '/tmp/out.std'    
   # write the selected records to the output file    
   fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    

