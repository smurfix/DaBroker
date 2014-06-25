#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)

test:
	@set -ex; \
	for f in tests/test??_*.py ; do \
		python $$f ; \
		python3 $$f ; \
	done
dtest:
	export TRACE=1; make test

t%: 
	python tests/test$(subst t,,$@)_*.py
	
d%: 
	export TRACE=1; python tests/test$(subst d,,$@)_*.py

update:
	@sh utils/update_boilerplate
	
.PHONY: test dtest update
