https://kiwidamien.github.io/save-the-environment-with-conda-and-how-to-let-others-run-your-programs.html
# get cmc conda
. ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64

# creat an environment
conda create --name fstpy_dev_env python=3.6

# activate an environment
. activate fstpy_dev_env

# install stuff in the env
conda install -c conda-forge sphinx-autodoc-typehints
conda install -c conda-forge sphinx-gallery
conda install -c conda-forge sphinx_rtd_theme
conda install ipykernel
conda install jupyter-lab
conda install numpy pandas dask xarray pytest
conda install sphinx

# export env to file
conda env export > environment.yaml

# deactivate the env
conda deactivate

# deleting the env
conda env remove --name fstpy_dev_env

# list all envs
conda info --envs

# recreate the env from yml specs
conda env create --file environment.yaml

