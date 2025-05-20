Release
========

Update release version number
-----------------------------

.. code:: bash

   Update the version number in fstpy/__init__.py 

Update configuration files
--------------------------
.. code:: bash

   Update the following configuration files:

      * cmds_python_env.txt:  CMDS python environment
      * ci_fstcomp.txt 

Build documentation
-------------------
.. code:: bash

   cd   doc  
   make clean 
   make doc  

Commit changes
--------------
.. code:: bash

   Commit the following files:

   * README.md
   * ci_fstcomp.txt

.. include:: ssm.rst
