Contributing
============

Getting the source code
-----------------------

.. code:: bash

   git clone git@gitlab.science.gc.ca:cmds/fstpy.git
   # create a new branch
   git checkout -b my_change
   # modify the code
   # commit your changes
   # fetch changes
   git fetch
   # merge recent master
   git merge origin/master
   # push your changes
   git push my_change

Then create a merge request on science's gitlab
https://gitlab.science.gc.ca/CMDS/fstpy/merge_requests

Using setup.sh to setup your developpement environment
------------------------------------------------------

.. code:: bash

   # From the $project_root directory of the project
   source setup.sh

Testing
-------

.. code:: bash

   
   # Use surgepy
   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmde/surge/surgepy/1.0.8
   # get rmn python library      
   . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2     
   # From the $project_root/test directory of the project
   python -m pytest -vrf

Building documentation
----------------------

.. code:: bash

   # This will build documentation in docs/build and there you will find index.html 
   cd doc
   make clean    
   make doc
