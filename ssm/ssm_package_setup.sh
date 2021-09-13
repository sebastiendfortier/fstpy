main(){
    if ! require_ordenv_loaded ; then return 1 ; fi


    if ((${#BASH_SOURCE[@]})); then
        #bash
        shell_source=${BASH_SOURCE[0]}
    elif ((${#KSH_VERSION[@]})); then
        #ksh
        shell_source=${.sh.file}
    fi

    sourced_file="$(cd "$(dirname "${shell_source}")"; pwd -P)/$(basename "${shell_source}")"

    in_ssm=false
    if [ -d "$(dirname ${sourced_file})/../../../etc/ssm.d" ] ; then
        in_ssm=true
    fi

    if $in_ssm ; then
        real_file=${sourced_file}
    else
        real_file=$(readlink -f ${sourced_file})
    fi

    base_path="$(cd "$(dirname ${real_file})/../.."; pwd)"

    if in_build_dir ; then
        message "ERROR: This script is only meant for the installed version of fstpy. "
        return 2
    fi

    # warn_override_variables PYTHONPATH

    # echo ${base_path}
    # list = $(ls -lart ${base_path})
    # echo $list

    # [ -z "$PYTHONPATH" ] && export PYTHONPATH=${base_path}/lib/packages/ || export PYTHONPATH=${base_path}/lib/packages/:$PYTHONPATH


    python_base_path="$(cd "$(dirname ${sourced_file})/../../../../.."; pwd)"
    requirements_file=${base_path}/share/requirements.txt
    if [ -f $requirements_file ]; then
        load_requirements ${python_base_path} ${requirements_file}
    fi
    if $in_ssm ; then
        load_spooki_runtime_dependencies
        message "SUCCESS: Using fstpy from ${base_path}"
    fi
}

message(){
   echo $(tput -T xterm setaf 3)${sourced_file}: $@$(tput -T xterm sgr 0) >&2
   true
}

require_ordenv_loaded(){
    if ! [ -v ORDENV_SETUP ] ; then
        message "ERROR: ORDENV_SETUP is not set, ordenv must be loaded to use fstpy."
        return 1
    fi
}

warn_override_variables(){
    for v in $@ ; do
        if [ -v $v ] ; then
            eval message "WARNING: Overriding variable $v \(previous value: \$$v\)"
        fi
    done
}

# check_directory_variables(){
#     missing_dir=0
#     for dv in $@ ; do
#         if eval ! [ -d \$$dv ] ; then
#             eval message "ERROR: Directory referenced by $dv does not exist \(\$$dv\)"
#             missing_dir=1
#         fi
#     done
#     return $missing_dir
# }

in_build_dir(){
    [ -e ${base_path}/CMakeFiles ]
}

print_and_do(){
   message $@
   eval $@
}

load_spooki_runtime_dependencies(){
    message "Loading fstpy runtime dependencies ..."
    print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1
    print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2
    echo 'if you dont have pandas >= 1.0.0, use the folowwing package'
    echo '. ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/python_packages/python3.6/all/2021.07'
    #print_and_do python3 -m pip install -r ${base_path}/etc/profile.d/requirements.txt
    message "... done loading fstpy runtime dependencies."
}

# $1 : python_base_path
# $2 : requirements file path
load_requirements(){
    while IFS= read -r line
    do
        if [[ $line != "" ]]; then
            load_ssm ${1} ${line}
        fi
    done <"${2}"
}

# $1 : python_base_path
# $2 : requirement, format is: package_name==version
load_ssm(){
    requirement_path=$(echo $2 | awk '{ gsub(/==/, "/" ); print; }')
    . ssmuse-sh -d ${1}/${requirement_path}
}

main
