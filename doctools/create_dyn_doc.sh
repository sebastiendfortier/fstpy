#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo ${DIR}
cd ${DIR}
DOC_DIR=${DIR:0:${#DIR}-8}doc
echo ${DOCDIR}

cd ../
VERSION_FILE="fstpy/__init__.py"
VERSION=$(grep __version__ $VERSION_FILE|cut -d"=" -f2)

if [ -z "$VERSION" ]; then
    echo "Error: Could not find __version__ in $VERSION_FILE"
    exit 1
fi

year=$(echo $VERSION | cut -d '.' -f 1)
month=$(echo $VERSION | cut -d '.' -f 2)
version_part=$(echo $VERSION | cut -d '.' -f 3)
VERSION_PATH=$year$month$version_part

OUTPUT=fstpy_env.txt
rm -f $OUTPUT
echo "   # FSTPY and dependencies " >> $OUTPUT
echo "   . r.load.dot /fs/ssm/eccc/cmd/cmds/fstpy/bundle/${VERSION_PATH//\"/}" >> $OUTPUT

cd doc
OUTPUT=install.rst
rm -f $OUTPUT

echo "Installation" >> $OUTPUT
echo "============" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use the ssm package" >> $OUTPUT
echo "" >> $OUTPUT
echo "::" >> $OUTPUT
echo "" >> $OUTPUT

echo "$(cat ../fstpy_env.txt)" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use the git repository package: at your own risk ;)" >> $OUTPUT
echo "" >> $OUTPUT
echo "::" >> $OUTPUT
echo "" >> $OUTPUT
echo "   python3 -m pip install git+http://gitlab.science.gc.ca/CMDS/fstpy.git" >> $OUTPUT



OUTPUT=usage.rst
rm -f $OUTPUT
echo "Using fstpy in scripts or Jupyter Lab/Notebook" >> $OUTPUT
echo "----------------------------------------------" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use pre-built developpement environment" >> $OUTPUT
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT

echo "$(cat ../cmds_python_env.txt)" >> $OUTPUT
echo "$(cat ../fstpy_env.txt)" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use fstpy" >> $OUTPUT
echo "~~~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: python" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # inside your script    " >> $OUTPUT
echo "   import fstpy   " >> $OUTPUT
echo "   df = fstpy.StandardFileReader('path to my fst file').to_pandas()" >> $OUTPUT
echo "" >> $OUTPUT
echo "Example" >> $OUTPUT
echo "~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: python" >> $OUTPUT
echo "" >> $OUTPUT
echo "   data_path = prefix + '/data/'    " >> $OUTPUT
echo "   import fstpy" >> $OUTPUT
echo "   # setup your file to read    " >> $OUTPUT
echo "   records=fstpy.StandardFileReader(data_path + 'ttuvre.std').to_pandas()    " >> $OUTPUT
echo "   # display selected records in a rpn voir format    " >> $OUTPUT
echo "   fstpy.voir(records)    " >> $OUTPUT
echo "   # get statistics on the selected records    " >> $OUTPUT
echo "   df = fstpy.fststat(records)    " >> $OUTPUT
echo "   # get a subset of records containing only UU and VV momvar    " >> $OUTPUT
echo "   just_tt_and_uv = records.query('nomvar in [\"TT\",\"UV\"]')    " >> $OUTPUT
echo "   # display selected records in a rpn voir format   " >> $OUTPUT
echo "   fstpy.voir(just_tt_and_uv)    " >> $OUTPUT
echo "   dest_path = '/tmp/out.std'    " >> $OUTPUT
echo "   # write the selected records to the output file    " >> $OUTPUT
echo "   fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    " >> $OUTPUT
echo "" >> $OUTPUT

OUTPUT=contributing.rst
rm -f $OUTPUT
echo "Contributing" >> $OUTPUT
echo "============" >> $OUTPUT
echo "" >> $OUTPUT
echo "Getting the source code" >> $OUTPUT
echo "-----------------------" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "   git clone git@gitlab.science.gc.ca:cmds/fstpy.git  " >> $OUTPUT
echo "   # create a new branch  " >> $OUTPUT
echo "   git checkout -b my_change  " >> $OUTPUT
echo "   # modify the code  " >> $OUTPUT
echo "   # commit your changes  " >> $OUTPUT
echo "   # fetch changes  " >> $OUTPUT
echo "   git fetch  " >> $OUTPUT
echo "   # merge recent master  " >> $OUTPUT
echo "   git merge origin/master  " >> $OUTPUT
echo "   # push your change  " >> $OUTPUT
echo "   git push my_change  " >> $OUTPUT
echo "" >> $OUTPUT
echo "Then create a merge request on science's gitlab " >> $OUTPUT
echo "https://gitlab.science.gc.ca/CMDS/fstpy/merge_requests" >> $OUTPUT
echo "" >> $OUTPUT
echo "Using setup.sh to setup your developpement environment" >> $OUTPUT
echo "------------------------------------------------------" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # From the \$project_root directory of the project" >> $OUTPUT
echo "   source setup.sh  " >> $OUTPUT
echo "" >> $OUTPUT
echo "Testing" >> $OUTPUT
echo "-------" >> $OUTPUT
echo "" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "$(cat ../cmds_python_env.txt)" >> $OUTPUT
echo "$(cat  ../fstpy_env.txt)" >> $OUTPUT
echo "$(cat  ../ci_fstcomp.txt)" >> $OUTPUT
echo "   # From the \$project_root directory of the project" >> $OUTPUT
echo "   python -m pytest -vrf  " >> $OUTPUT
echo "" >> $OUTPUT
echo "Building documentation" >> $OUTPUT
echo "----------------------" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # This will build documentation in docs/build and there you will find index.html  " >> $OUTPUT
echo "   cd doc  " >> $OUTPUT
echo "   make clean " >> $OUTPUT
echo "   make doc  " >> $OUTPUT

OUTPUT=requirements.rst
rm -f $OUTPUT
echo "Requirements" >> $OUTPUT
echo "============" >> $OUTPUT
echo "" >> $OUTPUT
echo "run time packages" >> $OUTPUT
echo "-----------------" >> $OUTPUT
echo "" >> $OUTPUT
echo "- pandas>=1.5.1" >> $OUTPUT
echo "- numpy>=1.24.4" >> $OUTPUT
echo "- dask>=2023.7.1" >> $OUTPUT
echo "" >> $OUTPUT
echo "developpement packages" >> $OUTPUT
echo "----------------------" >> $OUTPUT
echo "- ci_fstcomp>=1.0.9" >> $OUTPUT
echo "- pandas>=1.5.1" >> $OUTPUT
echo "- numpy>=1.24.4" >> $OUTPUT
echo "- dask>=2023.7.1" >> $OUTPUT

echo "- pytest>=7.4.0" >> $OUTPUT
echo "- sphinx>=5.3.0" >> $OUTPUT
echo "- sphinx_autodoc_typehints>=1.21.8" >> $OUTPUT
echo "- sphinx_gallery>=0.13.0" >> $OUTPUT
echo "- sphinx_rtd_theme>=0.5.1" >> $OUTPUT
echo "- nbsphinx>=0.9.2" >> $OUTPUT
echo "- ipython>=8.14.0" >> $OUTPUT
echo "- jupyterlab>=3.6.5" >> $OUTPUT
echo "- myst_parser>=1.0.0" >> $OUTPUT
echo "" >> $OUTPUT

echo "CMDS Python environment" >> $OUTPUT
echo "-----------------------" >> $OUTPUT
echo "" >> $OUTPUT
echo "This is an ssm package that we use at CMC on the science network and
that contains a wide variety of packages" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "$(cat ../cmds_python_env.txt)" >> $OUTPUT
