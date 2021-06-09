# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build
ORG_DIR       = ../org

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)


insert_version:
	@$(ORG_DIR)/insert_version.sh 	

rst:
	@$(ORG_DIR)/convert_and_send_to_doc.sh 

readme: 
	@$(ORG_DIR)/other/make_readme.sh

replace:
	@$(ORG_DIR)/replace_modified_files.sh

doc: rst readme insert_version
	make --no-print-directory html

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
