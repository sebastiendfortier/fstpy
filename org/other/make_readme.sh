#!/bin/bash
. ssmuse-sh -x cmd/cmdm/satellite/master_u1/miniconda3_4.9.2_ubuntu-18.04-skylake-64
. activate fstpy_full
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR=${DIR:0:${#DIR}-9}
cd ${DIR}
#echo ${ROOT_DIR}
pandoc -f org -t gfm README.org -o ${ROOT_DIR}README.md
