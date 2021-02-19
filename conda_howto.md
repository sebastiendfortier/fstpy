https://kiwidamien.github.io/save-the-environment-with-conda-and-how-to-let-others-run-your-programs.html
. ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64
conda activate fstpy_env
conda create --name fstpy_dev_env python=3.6
conda deactivate
conda env export > environment.yaml
conda env list
conda env remove --name fstpy
conda info --envs
conda install -c conda-forge sphinx-autodoc-typehints
conda install -c conda-forge sphinx-gallery
conda install -c conda-forge sphinx_rtd_theme
conda install ipykernel
conda install jupyter-lab
conda install numpy pandas dask xarray pytest
conda install sphinx
conda remove --name testing --all
conda env create --file environment.yaml
. activate fstpy_dev_env
