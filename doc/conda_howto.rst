.. raw:: org

   #+TITLE_: CMC CONDA HOWTO

Conda basics
============

`conda
reference <https://kiwidamien.github.io/save-the-environment-with-conda-and-how-to-let-others-run-your-programs.html>`__

get cmc conda
-------------

.. code:: bash

   . ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64

create an environment
---------------------

.. code:: bash

   conda create --name fstpy_dev_env python=3.6

activate an environment
-----------------------

.. code:: bash

   . activate fstpy_dev_env

install stuff in the env
------------------------

.. code:: bash

   conda install -c conda-forge sphinx-autodoc-typehints
   conda install -c conda-forge sphinx-gallery
   conda install -c conda-forge sphinx_rtd_theme
   conda install ipykernel
   conda install jupyter-lab
   conda install numpy pandas dask xarray pytest
   conda install sphinx

export env to file
------------------

.. code:: bash

   conda env export > environment.yaml

deactivate the env
------------------

.. code:: bash

   conda deactivate

deleting the env
----------------

.. code:: bash

   conda env remove --name fstpy_dev_env

list all envs
-------------

.. code:: bash

   conda info --envs

recreate the env from yml specs
-------------------------------

.. code:: bash

   conda env create --file environment.yaml
