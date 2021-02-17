
New feature, bug fix, etc. 
------------------------------------

If you want **fstpy** to be modified in any way, start by opening an issue
on gitlab. 

   #. Create an issue on *fstpy* `gitlab <https://gitlab.science.gc.ca/sbf000/fstpy>`_ page. 
      We will discuss the changes to be made and define a strategy for doing so. 

   #. Once the issue is created, fork the project. This will create your own gitlab repo where 
      you can make changes. 

   #. On your computer, clone the source code and go in the package 
      directory

        .. code-block:: bash

           git clone git@gitlab.com:<your-username>/fstpy.git 
           cd fstpy

   #. Create a new branch whose name is related to the issue you opened at step 1 above.   
      For example:

        .. code-block:: bash

           git checkout -b #666-include-cool-new-feature

   #. Create a clean `Anaconda <https://wiki.cmc.ec.gc.ca/wiki/Anaconda>`_ development environment 
      and activate it. 
      You will need internet access for this.

        .. code-block:: bash

           conda env create --name fstpy_dev_env -f doc/environment.yml
           conda activate fstpy_dev_env
   
   #. It is a good practice to start by writing a unit test that will pass once your feature/bugfix
      is correctly implemented. Look in the 

        .. code-block:: bash

           test/

      directories of the different _fstpy_ modules for examples of such tests.


   #. Modify the code to address the issue. Make sure to include examples and/or tests in the docstrings.  

   #. If applicable, describe the new functionality in the documentation.

   #. Modify the 
      files:
        .. code-block:: bash

           VERSION
           CHANGELOG.md

      To reflect your changes.

   #. Run tests
        
        .. code-block:: bash
        
            python -m pytest

   #. Run doctest

        .. code-block:: bash

           cd doc
           make doctest
      
      Make sure that there are no failures in the tests.

      
   #. If you modified the documentation in functions docstrings, you probably want to check the 
      changes by creating your local version of the documentation.

        .. code-block:: bash
      
           cd doc
           make html

      You can see the output in any web browser 
      pointing to:

        .. code-block:: bash
  
           fstpy/doc/_build/html/

   #. While you are working, it is normal to commit changes several times on you local branch. 
      However, before you push to your fork on gitlab, it is probably a good idea to 
      `squash <https://blog.carbonfive.com/2017/08/28/always-squash-and-rebase-your-git-commits/>`_
      all you intermediate commits into one, or a few commits, that clearly link to the issue 
      being worked on. 
      The resulting squashed commit  should pass the tests. 

   #. Once you are happy with the modifications, push the new version
      on your fork on gitlab

        .. code-block:: bash

           git push -u origin #666-include-cool-new-feature

   #. From the gitlab web interface, create a pull request to me. We will then 
      discuss the changes until they are accepted and merged into the master branch. 



    

