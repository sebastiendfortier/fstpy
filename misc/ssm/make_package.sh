#!/bin/bash
VERSION=1.0
PKGNAME=fstpy_${VERSION}_ubuntu-18.04-skylake-64
echo 'Building package '${PKGNAME}
mkdir -p ${PKGNAME}/lib/packages/
mkdir -p ${PKGNAME}/.ssm.d
mkdir -p ${PKGNAME}/bin
mkdir -p ${PKGNAME}/etc/profile.d

PROJECT_ROOT=../../packages/fstpy
echo 'Copying files to '${PKGNAME}' directory'
cp ${PKGNAME}.sh ${PKGNAME}/etc/profile.d/.
cp control.json ${PKGNAME}/.ssm.d/.
cp -rf ${PROJECT_ROOT}/* ${PKGNAME}/lib/packages/.
cp -rf requirements.txt ${PKGNAME}/etc/profile.d/.
echo 'Creating ssm archive '${PKGNAME}'.ssm'
tar -zcvf ${PKGNAME}.ssm ${PKGNAME}
echo 'Cleaning up '${PKGNAME}' directory'
rm -rf ${PKGNAME}/

mv ${PKGNAME}.ssm /tmp/sbf000/.

FSTPY_SSM_BASE=/fs/site4/eccc/cmd/w/sbf000/
echo 'ssm domain is '${FSTPY_SSM_BASE}
#echo 'unpublish old package'
#ssm unpublish -d ${FSTPY_SSM_BASE}/fstpy-beta -p ${PKGNAME}
#ssm uninstall -d ${FSTPY_SSM_BASE}/master -p ${PKGNAME}

#ssm created -d ${FSTPY_SSM_BASE}/master
echo 'Installing package to '${FSTPY_SSM_BASE}/master
ssm install -d ${FSTPY_SSM_BASE}/master -f /tmp/sbf000/${PKGNAME}.ssm
echo 'Publishing package '${PKGNAME}' to '${FSTPY_SSM_BASE}/fstpy-beta
ssm created -d ${FSTPY_SSM_BASE}/fstpy-beta
ssm publish -d ${FSTPY_SSM_BASE}/master -P ${FSTPY_SSM_BASE}/fstpy-beta -p ${PKGNAME}

rm /tmp/sbf000/${PKGNAME}.ssm

echo 'Execute . ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta'-${VERSION}' to use this package'


