#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)

test:
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
	
.PHONY: test dtest
