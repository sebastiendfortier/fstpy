main(){
      load_runtime_dependencies
}

message(){
   echo $(tput -T xterm setaf 3)$@$(tput -T xterm sgr 0) >&2
   true
}

print_and_do(){
   message $@
   eval $@
}

load_runtime_dependencies(){
   message "Load ci_fstcomp for developpement ..."
   message '. ssmuse-sh -d /fs/ssm/eccc/cmd/cmds/apps/ci_fstcomp/(check directory for latest version)/'
   if [[ ! "$PYTHONPATH" == *"rpnpy_2.1.2"* ]]
   then 
      print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/migdep/5.1.1
      print_and_do . r.load.dot eccc/mrd/rpn/MIG/ENV/rpnpy/2.1.2
   fi
}

main
