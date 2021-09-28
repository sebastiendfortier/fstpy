main(){
      load_spooki_runtime_dependencies
}

message(){
   echo $(tput -T xterm setaf 3)$@$(tput -T xterm sgr 0) >&2
   true
}

print_and_do(){
   message $@
   eval $@
}

load_spooki_runtime_dependencies(){
    message "Loading fstpy runtime dependencies ..."
    print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1
    print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2
    print_and_do . ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/apps/ci_fstcomp/1.0.2
}

main
