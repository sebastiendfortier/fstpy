#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR=${DIR:0:${#DIR}-9}
cd ${DIR}
#echo ${ROOT_DIR}
/home/sbf000/.conda/envs/fstpy_dev/bin/pandoc -f org -t gfm README.org -o ${ROOT_DIR}README.md
