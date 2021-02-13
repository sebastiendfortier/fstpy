#!/bin/bash
VERSION=1.0.1
PKGNAME=fstpy_${VERSION}_ubuntu-18.04-skylake-64

echo '{' > control.json
echo '    "name": "fstpy",' >> control.json
echo '    "version": "'${VERSION}'",' >> control.json
echo '    "platform": "'${BASE_ARCH}'",' >> control.json
echo '    "maintainer": "CMDS",' >> control.json
echo '    "description": "fstpy package",' >> control.json
echo '    "x-build-date": "'`date`'",' >> control.json
echo '    "x-build-platform": "'${BASE_ARCH}'",' >> control.json
echo '    "x-build-host": "'${TRUE_HOST}'",' >> control.json
echo '    "x-build-user": "'${USER}'",' >> control.json
echo '    "x-build-uname": [' >> control.json
echo '        "Linux",' >> control.json
echo '        "'${TRUE_HOST}'",' >> control.json
echo '        "3.10.0-957.el7.x86_64",' >> control.json
echo '        "#1 SMP Thu Oct 4 20:48:51 UTC 2018",' >> control.json
echo '        "x86_64"' >> control.json
echo '    ]' >> control.json
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

mv ${PKGNAME}.ssm /tmp/sbf000/.

FSTPY_SSM_BASE=/fs/site4/eccc/cmd/w/sbf000
echo 'ssm domain is '${FSTPY_SSM_BASE}
#echo 'unpublish old package'
ssm unpublish -d ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION} -p ${PKGNAME}
ssm uninstall -d ${FSTPY_SSM_BASE}/master -p ${PKGNAME}

#ssm created -d ${FSTPY_SSM_BASE}/master
echo 'Installing package to '${FSTPY_SSM_BASE}/master
ssm install -d ${FSTPY_SSM_BASE}/master -f /tmp/sbf000/${PKGNAME}.ssm
echo 'Publishing package '${PKGNAME}' to '${FSTPY_SSM_BASE}/fstpy-beta-${VERSION}
ssm created -d ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION}
ssm publish -d ${FSTPY_SSM_BASE}/master -P ${FSTPY_SSM_BASE}/fstpy-beta-${VERSION} -p ${PKGNAME}

rm /tmp/sbf000/${PKGNAME}.ssm

echo 'Execute . ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta'-${VERSION}' to use this package'


