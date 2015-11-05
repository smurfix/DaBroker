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
import pytest_asyncio.plugin
import os
import asyncio
from dabroker.unit import Unit
from yaml import safe_load
import unittest
from unittest.mock import Mock

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

def test_basic():
	cfg = load_cfg("test.cfg")
	u = Unit("test.zero", cfg)
	loop = asyncio.get_event_loop()
	loop.run_until_complete(u.start())
	loop.run_until_complete(u.stop())

@pytest.yield_fixture
def unit1(event_loop):
	g = _unit("one",event_loop)
	yield next(g)
	next(g)
@pytest.yield_fixture
def unit2(event_loop):
	g = _unit("two",event_loop)
	yield next(g)
	next(g)
@asyncio.coroutine
def _unit(name,loop):
	cfg = load_cfg("test.cfg")
	u = Unit("test."+name, cfg)
	loop.run_until_complete(u.start())
	yield u
	loop.run_until_complete(u.stop())
	
@pytest.mark.asyncio
def test_unit(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda x: "foo "+x)
	alert_me = Mock(side_effect=lambda y: "bar "+y)
	with pytest.raises(AssertionError):
		yield from unit1.register_rpc("my.call",call_me)
	yield from unit1.register_rpc("my.call",call_me, async=True)
	with pytest.raises(AssertionError):
		yield from unit1.register_alert("my.alert",alert_me)
	yield from unit1.register_alert("my.alert",alert_me, async=True)
	yield from unit2.register_alert("my.alert",alert_me, async=True)
	res = yield from unit2.rpc("my.call", "one")
	assert res == "foo one"
	res = yield from unit1.rpc("my.call", x="two")
	assert res == "foo bar"
	n = 0
	def cb(x):
		nonlocal n
		n += 1
		assert x == "bar dud", x
	yield from unit2.alert("my.alert","dud")
	assert n == 2


