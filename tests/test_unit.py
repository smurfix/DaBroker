# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This file is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

import pytest
import os
from dabroker.unit import Unit
from yaml import safe_load

# load a config file
def load_cfg(cfg):
	global cfgpath
	if os.path.exists(cfg):
		pass
	elif os.path.exists(os.path.join("tests",cfg)):
		cfg = os.path.join("tests",cfg)
	elif os.path.exists(os.path.join(os.pardir,cfg)):
		cfg = os.path.join(os.pardir,cfg)
	else:
		raise RuntimeError("Config file '%s' not found" % (cfg,))

	cfgpath = cfg
	with open(cfg) as f:
		cfg = safe_load(f)

	return cfg

@pytest.fixture
def unit1():
	cfg = load_cfg("test.cfg")
	return Unit("test.one", cfg)
	
@pytest.fixture
def unit2():
	cfg = load_cfg("test.cfg")
	return Unit("test.two", cfg)

def test_unit(unit1, unit2):
	pass
