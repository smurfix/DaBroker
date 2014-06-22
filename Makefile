#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)

test:
	for f in tests/test_*.py ; do \
		python $$f ; \
		python3 $$f ; \
	done
dtest:
	export TRACE=1; make test

.PHONY: test dtest
