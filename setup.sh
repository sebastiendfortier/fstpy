
# Check that the shell script is being sourced by attempting
# to use return outside a function, which can only be done in
# if the script is being sourced and not if is being run.
#
# The true is important here to clear the previous exit
# code.  Without this, doing `false ; source setup.sh`
# would enter this if and call exit.
if ! (true && (return 2>/dev/null)) ; then
    echo "$0 ERROR : This script must be sourced"
    exit 1
fi

# Ensure that Ordenv is loaded
if [[ $ORDENV_SETUP != 1 ]] ; then
    echo "${BASH_SOURCE[0]} ERROR : Ordenv setup must be done to load rpnpy"
    return 1
fi

use_fstpy(){
    use_fstpy_deps
    add_fstpy_to_pythonpath
}

add_fstpy_to_pythonpath(){
    # Note the use of python for portability.
    # On darwin, readlink does not have the -f option
    bash_source=$(python3 -c "import os; print(os.path.realpath('${BASH_SOURCE[0]}'))")
    this_dir=$(cd -P $(dirname $bash_source) 2>/dev/null && pwd)
    fstpy_packages_dir=$this_dir
    export PYTHONPATH=$fstpy_packages_dir:$PYTHONPATH
}

message(){
   echo $(tput -T xterm setaf 3)$@$(tput -T xterm sgr 0) >&2
   true
}


use_fstpy_deps() {
    # Les fichiers ci_fstcomp.txt, cmds_python_env.txt et fstpy_env.txt contiennent les packages necessaires a sourcer 
    local filenames=("cmds_python_env.txt" "fstpy_env.txt" "ci_fstcomp.txt")
    
    for filename in "${filenames[@]}"; do
        message "Processing $filename"
        
        while IFS= read -r line || [[ -n "$line" ]]; do
            if [[ $line =~ ^[[:space:]]*\..* ]]; then
                eval "$line"
            fi
        done < "$filename"
    done
}

use_fstpy
