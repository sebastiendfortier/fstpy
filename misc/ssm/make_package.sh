#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR=${DIR:0:${#DIR}-8}
cd ${DIR}
VERSION=$(head -n 1 ${ROOT_DIR}VERSION)
#echo ${VERSION}
PKGNAME=fstpy_${VERSION}_${BASE_ARCH}

echo '{' > control.json
echo '    "name": "fstpy",' >> control.json
echo '    "version": "'${VERSION}'",' >> control.json
echo '    "platform": "'${BASE_ARCH}'",' >> control.json
echo '    "maintainer": "CMDS",' >> control.json
echo '    "description": "fstpy package",' >> control.json
echo '    "x-build-date": "'`date`'",' >> control.json
echo '    "x-build-platform": "'${BASE_ARCH}'",' >> control.json
echo '    "x-build-host": "'`hostname -f`'",' >> control.json
echo '    "x-build-user": "'${USER}'",' >> control.json
echo '    "x-build-uname": "('`uname -s`', '`uname -n`', '`uname -r`', '`uname -v`', '`uname -m`')"' >> control.json
echo '}' >> control.json


echo 'Building package '${PKGNAME}
mkdir -p ${PKGNAME}/lib/packages/fstpy
mkdir -p ${PKGNAME}/.ssm.d
mkdir -p ${PKGNAME}/bin
mkdir -p ${PKGNAME}/etc/profile.d

PROJECT_ROOT=../../fstpy/
echo 'Copying files to '${PKGNAME}' directory'
cp fstpy_setup.sh ${PKGNAME}/etc/profile.d/${PKGNAME}.sh
cp control.json ${PKGNAME}/.ssm.d/.
cp -rf ${PROJECT_ROOT}/* ${PKGNAME}/lib/packages/fstpy/.
cp -rf requirements.txt ${PKGNAME}/etc/profile.d/.
echo 'Creating ssm archive '${PKGNAME}'.ssm'
tar -zcvf ${PKGNAME}.ssm ${PKGNAME}
echo 'Cleaning up '${PKGNAME}' directory'
rm -rf control.json
rm -rf ${PKGNAME}/

mv ${PKGNAME}.ssm /tmp/${USER}/.

FSTPY_SSM_BASE=/fs/site4/eccc/cmd/w/sbf000
echo 'ssm domain is '${FSTPY_SSM_BASE}
#echo 'unpublish old package'
ssh sbf000@ppp4 source .profile&&ssm unpublish -d ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION} -p ${PKGNAME}
ssh sbf000@ppp4 source .profile&&ssm uninstall -d ${FSTPY_SSM_BASE}/master -p ${PKGNAME}

#ssm created -d ${FSTPY_SSM_BASE}/master
echo 'Installing package to '${FSTPY_SSM_BASE}/master
ssh sbf000@ppp4 source .profile&&ssm install -d ${FSTPY_SSM_BASE}/master -f /tmp/${USER}/${PKGNAME}.ssm
echo 'Publishing package '${PKGNAME}' to '${FSTPY_SSM_BASE}/fstpy-beta-${VERSION}
ssh sbf000@ppp4 source .profile&&ssm created -d ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION}
ssh sbf000@ppp4 source .profile&&ssm publish -d ${FSTPY_SSM_BASE}/master -P ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION} -p ${PKGNAME}

rm /tmp/${USER}/${PKGNAME}.ssm

echo 'Execute . ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta'-${VERSION}' to use this package'


