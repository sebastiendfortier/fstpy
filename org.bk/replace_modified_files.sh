#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
#echo ${DIR}
ROOT_DIR=${DIR:0:${#DIR}-3}
#echo ${ROOT_DIR}
cd ${DIR}
VERSION=$(head -n 1 ${ROOT_DIR}VERSION)
#echo ${VERSION}
mv install.org.bk install.org 
mv usage.org.bk usage.org
