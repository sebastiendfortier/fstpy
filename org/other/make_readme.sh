#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR=${DIR:0:${#DIR}-9}
cd ${DIR}
#echo ${ROOT_DIR}
pandoc -f org -t gfm README.org -o ${ROOT_DIR}README.md
