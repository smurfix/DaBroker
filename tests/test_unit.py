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
from dabroker.unit import Unit, CC_DICT,CC_DATA,CC_MSG
from dabroker.unit.msg import ReturnedError,AlertMsg
from dabroker.util.tests import load_cfg
import unittest
from unittest.mock import Mock

def test_basic(event_loop):
	cfg = load_cfg("test.cfg")
	u = Unit("test.zero", cfg)
	event_loop.run_until_complete(u.start())
	event_loop.run_until_complete(u.stop())

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
def test_conn_not(event_loop):
	cfg = load_cfg("test.cfg")
	u = Unit("test.no_port", cfg)
	with pytest.raises(ConnectionRefusedError):
		yield from u.start()

@pytest.mark.asyncio
def test_rpc_basic(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda x: "foo "+x)
	call_msg = Mock(side_effect=lambda m: "foo "+m.data['x'])
	yield from unit1.register_rpc("my.call",call_me, async=True,call_conv=CC_DATA)
	yield from unit1.register_rpc("my.call.x",call_me, async=True,call_conv=CC_DICT)
	yield from unit1.register_rpc("my.call.m",call_msg, async=True,call_conv=CC_MSG)
	res = yield from unit2.rpc("my.call", "one")
	assert res == "foo one"
	res = yield from unit1.rpc("my.call", "two")
	assert res == "foo two"
	with pytest.raises(ReturnedError):
		res = yield from unit1.rpc("my.call", x="two")
	res = yield from unit1.rpc("my.call.x", x="three")
	assert res == "foo three"
	with pytest.raises(ReturnedError):
		res = yield from unit1.rpc("my.call", y="duh")
	res = yield from unit1.rpc("my.call.m", x="four")
	assert res == "foo four"

@pytest.mark.asyncio
def test_rpc_unencoded(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda : object())
	yield from unit1.register_rpc("my.call",call_me, async=True,call_conv=CC_DICT)
	try:
		r = unit2.rpc("my.call")
		r = yield from asyncio.wait_for(r, timeout=0.2)
	except ReturnedError as exc:
		import pdb;pdb.set_trace()
		assert False,"should not reply"
	except asyncio.TimeoutError:
		pass
	except Exception as exc:
		assert False,exc
	else:
		assert False,r

def something_named(foo):
	return "bar "+foo

@pytest.mark.asyncio
def test_rpc_named(unit1, unit2, event_loop):
	yield from unit1.register_rpc(something_named, async=True,call_conv=CC_DATA)
	res = yield from unit2.rpc("something.named", "one")
	assert res == "bar one"
	res = yield from unit1.rpc("something.named", "two")
	assert res == "bar two"

@pytest.mark.asyncio
def test_rpc_explicit(unit1, unit2, event_loop):
	from dabroker.unit.rpc import RPCservice
	s = RPCservice(something_named,call_conv=CC_DATA)
	yield from unit1.register_rpc(s, async=True)
	res = yield from unit2.rpc("tests.test_unit.something_named", "one")
	assert res == "bar one"
	res = yield from unit1.rpc("tests.test_unit.something_named", "two")
	assert res == "bar two"

@pytest.mark.asyncio
def test_alert_callback(unit1, unit2, event_loop):
	alert_me = Mock(side_effect=lambda y: "bar "+y)
	yield from unit1.register_alert("my.alert",alert_me, async=True,call_conv=CC_DICT)
	yield from unit2.register_alert("my.alert",alert_me, async=True,call_conv=CC_DICT)
	n = 0
	def cb(x):
		nonlocal n
		n += 1
		assert x == "bar dud", x
	yield from unit2.alert("my.alert",y="dud",callback=cb,call_conv=CC_DATA, timeout=0.2)
	assert n == 2
	n = 0
	yield from unit1.alert("my.alert",_data={'y':"dud"},callback=cb,call_conv=CC_DATA, timeout=0.2)
	assert n == 2

@pytest.mark.asyncio
def test_alert_uncodeable(unit1, unit2, event_loop):
	alert_me = Mock(side_effect=lambda : object())
	yield from unit1.register_alert("my.alert",alert_me, async=True,call_conv=CC_DICT)
	def cb(msg):
		assert False,"Called?"
	n = yield from unit2.alert("my.alert",callback=cb, timeout=0.2)
	assert n == 0

@pytest.mark.asyncio
def test_alert_oneway(unit1, unit2, event_loop):
	alert_me1 = Mock()
	alert_me2 = Mock()
	alert_me3 = Mock()
	yield from unit1.register_alert("my.alert1",alert_me1, async=True,call_conv=CC_DICT)
	yield from unit1.register_alert("my.alert2",alert_me2, async=True,call_conv=CC_DATA)
	yield from unit1.register_alert("my.alert3",alert_me3, async=True) # default is CC_MSG
	yield from unit2.alert("my.alert1",_data={'y':"dud"})
	yield from unit2.alert("my.alert2",_data={'y':"dud"})
	yield from unit2.alert("my.alert3",_data={'y':"dud"})
	yield from asyncio.sleep(0.1)
	alert_me1.assert_called_with(y='dud')
	alert_me2.assert_called_with(dict(y='dud'))
	alert_me3.assert_called_with(AlertMsg(data=dict(y='dud')))

@pytest.mark.asyncio
def test_alert_no_data(unit1, unit2, event_loop):
	alert_me1 = Mock(side_effect=lambda x: "")
	alert_me2 = Mock(side_effect=lambda : {})
	yield from unit1.register_alert("my.alert1",alert_me1, async=True,call_conv=CC_DATA)
	yield from unit2.register_alert("my.alert2",alert_me2, async=True,call_conv=CC_DICT)
	def recv1(d):
		assert d == 0
	@asyncio.coroutine
	def recv2(*a,**k):
		yield from asyncio.sleep(0.01)
		assert not a
		assert not k
		return {}
	res = yield from unit2.alert("my.alert1",_data="", callback=recv1, call_conv=CC_DATA, timeout=0.2)
	alert_me1.assert_called_with(0)
	assert res == 1
	res = yield from unit2.alert("my.alert2", callback=recv2, call_conv=CC_DICT, timeout=0.2)
	alert_me2.assert_called_with()
	assert res == 1

@pytest.mark.asyncio
def test_alert_stop(unit1, unit2, event_loop):
	@asyncio.coroutine
	def sleep1():
		yield from asyncio.sleep(0.1)
	def sleep2():
		yield from asyncio.sleep(0.2)
	yield from unit1.register_alert("my.sleep",sleep1, async=True)
	yield from unit2.register_alert("my.sleep",sleep2, async=True)
	def recv(msg):
		raise StopIteration
	res = yield from unit2.alert("my.sleep",_data="", callback=recv, timeout=0.15)
	assert res == 1

@pytest.mark.asyncio
def test_reg(unit1, unit2, event_loop):
	def recv(**d):
		if d['uuid'] == unit1.uuid:
			assert d['app'] == unit1.app
		elif d['uuid'] == unit2.uuid:
			assert d['app'] == unit2.app
		else:
			assert False,d
	res = yield from unit2.alert("dabroker.ping", callback=recv, timeout=0.2, call_conv=CC_DICT)
	assert res == 2

	res = yield from unit2.rpc("dabroker.ping."+unit1.uuid)
	assert res['app'] == unit1.app
	assert "dabroker.ping."+unit1.uuid in res['rpc_endpoints']

@pytest.mark.asyncio
def test_alert_error(unit1, unit2, event_loop):
	def err(x):
		raise RuntimeError("dad")
	error_me1 = Mock(side_effect=err)
	yield from unit1.register_alert("my.error1",error_me1, async=True, call_conv=CC_DATA)
	def recv1(d):
		assert d.error.cls == "RuntimeError"
		assert d.error.message == "dad"
	res = yield from unit2.alert("my.error1", _data="", callback=recv1, timeout=0.2)
	error_me1.assert_called_with(0)
	assert res == 1

	res = yield from unit2.alert("my.error1", callback=recv1, call_conv=CC_DATA, timeout=0.2)
	assert res == 0

	def recv2(msg):
		msg.raise_if_error()
	with pytest.raises(ReturnedError):
		yield from unit2.alert("my.error1", callback=recv2, timeout=0.2)

@pytest.mark.asyncio
def test_reg_error(unit1):
	with pytest.raises(AssertionError):
		yield from unit1.register_rpc("my.call",Mock())
	with pytest.raises(AssertionError):
		yield from unit1.register_alert("my.alert",Mock())

@pytest.mark.asyncio
def test_reg_error(unit1):
	with pytest.raises(AssertionError):
		yield from unit1.register_rpc("my.call",Mock())
	with pytest.raises(AssertionError):
		yield from unit1.register_alert("my.alert",Mock())

@pytest.mark.asyncio
def test_rpc_bad_params(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda x: "foo "+x)
	yield from unit1.register_rpc("my.call",call_me, async=True,call_conv=CC_DATA)
	try:
		res = yield from unit2.rpc("my.call", x="two")
	except ReturnedError as exc:
		assert exc.error.cls == "TypeError"
		assert "convert" in str(exc)
	else:
		assert False,"exception not called"
	
def test_reg_sync(event_loop):
	cfg = load_cfg("test.cfg")
	u = Unit("test.three", cfg)
	@u.register_rpc("foo.bar")
	def foo_bar_0(msg):
		return "quux from "+msg.data['baz']
	@u.register_rpc
	def foo_bar_1(msg):
		return "quux from "+msg.data['baz']
	@u.register_rpc(call_conv=CC_DICT)
	def foo_bar_2(baz):
		return "quux from "+baz
	event_loop.run_until_complete(u.start())
	x = event_loop.run_until_complete(u.rpc("foo.bar",baz="nixx"))
	y = event_loop.run_until_complete(u.rpc("foo.bar.1",baz="nixy"))
	z = event_loop.run_until_complete(u.rpc("foo.bar.2",baz="nixz"))
	assert x == "quux from nixx"
	assert y == "quux from nixy"
	assert z == "quux from nixz"
	event_loop.run_until_complete(u.stop())

