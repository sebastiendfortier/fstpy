#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
#echo ${DIR}
cd ${DIR}
DOC_DIR=${DIR:0:${#DIR}-3}doc
#echo ${DOCDIR}
for f in `ls *.org`
do
    echo 'converting '$f&&/home/sbf000/.conda/envs/fstpy_dev/bin/pandoc $f -o ${DOC_DIR}/${f%.org}.rst
done