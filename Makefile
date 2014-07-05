#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)

test:
	make test2 DAB_CODEC=bson
	make test2 DAB_CODEC=json
	make test2 DAB_CODEC=marshal
	make test3 DAB_CODEC=bson
	make test3 DAB_CODEC=marshal

test2:
	@set -ex; \
	for f in tests/test??_*.py ; do \
		python $$f ; \
	done

test3:
	@set -ex; \
	for f in tests/test??_*.py ; do \
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
	
.PHONY: test test2 test3 dtest update
