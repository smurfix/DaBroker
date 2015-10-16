#!/usr/bin/make -f

export PYTHONPATH=$(shell pwd)

test: test.cfg
	py.test-3

test.cfg:
	@cp test.cfg.sample $@
	@echo "Warning: copied test.cfg.sample to $@" >&2

otest:
	@set -ex; for T in 2 3 y ; do for C in bson json marshal; do \
	make test$$T DAB_CODEC=$$C; \
	done; done


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

testy:
	@set -ex; \
	for f in tests/test??_*.py ; do \
		pypy $$f ; \
	done

dtest:
	export TRACE=1; make test

# Shortcuts for running tests.
# "make t30" would run tests/test30*.py with Python 2.x.

2t% t%:
	python tests/test$*_*.py
2t% d%:
	export TRACE=1; python tests/test$*_*.py

3t%: 
	python3 tests/test$*_*.py
3d%: 
	export TRACE=1; python3 tests/test$*_*.py

yt%: 
	pypy tests/test$*_*.py
yd%: 
	export TRACE=1; pypy tests/test$*_*.py

update:
	@sh utils/update_boilerplate
	
.PHONY: test test2 test3 dtest update
