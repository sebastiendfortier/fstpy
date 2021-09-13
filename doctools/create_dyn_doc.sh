#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo ${DIR}
cd ${DIR}
DOC_DIR=${DIR:0:${#DIR}-8}doc
echo ${DOCDIR}

cd ../
VERSION=$(head -n 1 VERSION)
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
echo "   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/fstpy/$VERSION/" >> $OUTPUT
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
echo "Use pre-build developpement environment" >> $OUTPUT
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # get rmn python library      " >> $OUTPUT
echo "   . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2      " >> $OUTPUT
echo "   # get fstpy ssm package" >> $OUTPUT
echo "   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/fstpy/$VERSION/" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. _use-pre-build-developpement-environment-1:" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use pre-build developpement environment" >> $OUTPUT
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: bash" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # get conda if you don't already have it  " >> $OUTPUT
echo "   . ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64   " >> $OUTPUT
echo "   # create a link to pre-built environment" >> $OUTPUT
echo "   cd ~/.conda/envs/" >> $OUTPUT
echo "   ln -s ~sgci800/.conda/envs/fstpy_full" >> $OUTPUT
echo "   # whenever you need to use this environment on science run the following (if you have'nt loaded the conda ssm, you'll need to do it everytime)" >> $OUTPUT
echo "   # unless you put it in your profile" >> $OUTPUT
echo "   # activate your conda environment     " >> $OUTPUT
echo "   . activate fstpy_full     " >> $OUTPUT
echo "   # get rmn python library      " >> $OUTPUT
echo "   . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1 eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2      " >> $OUTPUT
echo "   # get fstpy ssm package" >> $OUTPUT
echo "   . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/fstpy/$VERSION/" >> $OUTPUT
echo "" >> $OUTPUT
echo "Use fstpy" >> $OUTPUT
echo "~~~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: python" >> $OUTPUT
echo "" >> $OUTPUT
echo "   # inside your script    " >> $OUTPUT
echo "   import fstpy.all as fstpy   " >> $OUTPUT
echo "   df = fstpy.StandardFileReader('path to my fst file').to_pandas()" >> $OUTPUT
echo "" >> $OUTPUT
echo "Example" >> $OUTPUT
echo "~~~~~~~" >> $OUTPUT
echo "" >> $OUTPUT
echo ".. code:: python" >> $OUTPUT
echo "" >> $OUTPUT
echo "   data_path = prefix + '/data/'    " >> $OUTPUT
echo "   import fstpy.all as fstpy" >> $OUTPUT
echo "   # setup your file to read    " >> $OUTPUT
echo "   records=fstpy.StandardFileReader(data_path + 'ttuvre.std').to_pandas()    " >> $OUTPUT
echo "   # display selected records in a rpn voir format    " >> $OUTPUT
echo "   fstpy.voir(records)    " >> $OUTPUT
echo "   # get statistics on the selected records    " >> $OUTPUT
echo "   df = fstpy.fststat(records)    " >> $OUTPUT
echo "   # get a subset of records containing only UU and VV momvar    " >> $OUTPUT
echo "   just_tt_and_uv = fstpy.select(records,'nomvar in [\"TT\",\"UV\"]')    " >> $OUTPUT
echo "   # display selected records in a rpn voir format   " >> $OUTPUT
echo "   fstpy.voir(just_tt_and_uv)    " >> $OUTPUT
echo "   dest_path = '/tmp/out.std'    " >> $OUTPUT
echo "   # write the selected records to the output file    " >> $OUTPUT
echo "   fstpy.StandardFileWriter(dest_path,just_tt_and_uv).to_fst()    " >> $OUTPUT
echo "" >> $OUTPUT
