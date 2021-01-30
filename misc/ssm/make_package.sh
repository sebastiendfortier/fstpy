#!/bin/bash
PKGNAME=fstpy_1.0_ubuntu-18.04-skylake-64
mkdir -p ${PKGNAME}/lib/packages/
mkdir -p ${PKGNAME}/.ssm.d
mkdir -p ${PKGNAME}/bin
mkdir -p ${PKGNAME}/etc/profile.d


cp ${PKGNAME}.sh ${PKGNAME}/etc/profile.d/.
cp control.json ${PKGNAME}/.ssm.d/.
cp -rf ../../spooki-pure/spooki-pure/packages/std ${PKGNAME}/lib/packages/.
cp -rf ../../spooki-pure/spooki-pure/packages/dictionaries ${PKGNAME}/lib/packages/.
cp -rf ../../spooki-pure/spooki-pure/packages/utils ${PKGNAME}/lib/packages/.
cp -rf ../../spooki-pure/spooki-pure/packages/unit ${PKGNAME}/lib/packages/.
cp -rf ../../spooki-pure/spooki-pure/packages/log ${PKGNAME}/lib/packages/.
cp -rf ../../spooki-pure/spooki-pure/packages/__init__.py ${PKGNAME}/lib/packages/.
cp -rf requirements.txt ${PKGNAME}/etc/profile.d/.

tar -zcvf ${PKGNAME}.ssm ${PKGNAME}
rm -rf ${PKGNAME}/

mv ${PKGNAME}.ssm /tmp/sbf000/.

SPOOKI_SSM_BASE=/fs/site4/eccc/cmd/w/sbf000/

ssm unpublish -d ${SPOOKI_SSM_BASE}/fstpy-beta -p ${PKGNAME}
ssm uninstall -d ${SPOOKI_SSM_BASE}/master -p ${PKGNAME}

#ssm created -d ${SPOOKI_SSM_BASE}/master
ssm install -d ${SPOOKI_SSM_BASE}/master -f /tmp/sbf000/${PKGNAME}.ssm
#ssm created -d ${SPOOKI_SSM_BASE}/fstpy-beta
ssm publish -d ${SPOOKI_SSM_BASE}/master -P ${SPOOKI_SSM_BASE}/fstpy-beta -p ${PKGNAME}

rm /tmp/sbf000/${PKGNAME}.ssm

#. ssmuse-sh -d /fs/site4/eccc/cmd/w/sbf000/fstpy-beta


