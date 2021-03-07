#!/bin/bash

for f in `ls *.org`
do
    echo 'converting '$f&&/home/sbf000/.conda/envs/fstpy_dev_env/bin/pandoc $f -o ../doc/${f%.org}.rst
done