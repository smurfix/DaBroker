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
from dabroker.unit import make_unit as unit,Unit, CC_DICT,CC_DATA,CC_MSG
from dabroker.unit.msg import ReturnedError,AlertMsg
from dabroker.util.tests import load_cfg
import unittest
from unittest.mock import Mock

def test_basic(event_loop):
	cfg = load_cfg("test.cfg")
	u = Unit("test.zero", cfg['config'])
	event_loop.run_until_complete(u.start())
	event_loop.run_until_complete(u.stop())

@pytest.yield_fixture
def unit1(event_loop):
	yield from _unit("one",event_loop)
@pytest.yield_fixture
def unit2(event_loop):
	yield from _unit("two",event_loop)
def _unit(name,loop):
	cfg = load_cfg("test.cfg")['config']
	u = loop.run_until_complete(unit("test."+name, cfg))
	yield u
	x = u.stop()
	loop.run_until_complete(x)

@pytest.mark.asyncio
async def test_conn_not(event_loop, unused_tcp_port):
	cfg = load_cfg("test.cfg")['config']
	cfg['amqp']['server']['port'] = unused_tcp_port
	with pytest.raises(OSError):
		await unit("test.no_port", cfg)

@pytest.mark.asyncio
async def test_rpc_basic(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda x: "foo "+x)
	call_msg = Mock(side_effect=lambda m: "foo "+m.data['x'])
	await unit1.register_rpc_async("my.call",call_me, call_conv=CC_DATA)
	await unit1.register_rpc_async("my.call.x",call_me, call_conv=CC_DICT)
	await unit1.register_rpc_async("my.call.m",call_msg, call_conv=CC_MSG)
	res = await unit2.rpc("my.call", "one")
	assert res == "foo one"
	res = await unit1.rpc("my.call", "two")
	assert res == "foo two"
	with pytest.raises(ReturnedError):
		res = await unit1.rpc("my.call", x="two")
	res = await unit1.rpc("my.call.x", x="three")
	assert res == "foo three"
	with pytest.raises(ReturnedError):
		res = await unit1.rpc("my.call", y="duh")
	res = await unit1.rpc("my.call.m", x="four")
	assert res == "foo four"

@pytest.mark.asyncio
async def test_rpc_unencoded(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda : object())
	await unit1.register_rpc_async("my.call",call_me, call_conv=CC_DICT)
	try:
		r = unit2.rpc("my.call")
		r = await asyncio.wait_for(r, timeout=0.2)
	except ReturnedError as exc:
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
async def test_rpc_named(unit1, unit2, event_loop):
	await unit1.register_rpc_async(something_named, call_conv=CC_DATA)
	res = await unit2.rpc("something.named", "one")
	assert res == "bar one"
	res = await unit1.rpc("something.named", "two")
	assert res == "bar two"

@pytest.mark.asyncio
async def test_rpc_explicit(unit1, unit2, event_loop):
	from dabroker.unit.rpc import RPCservice
	s = RPCservice(something_named,call_conv=CC_DATA)
	await unit1.register_rpc_async(s)
	res = await unit2.rpc("tests.test_unit.something_named", "one")
	assert res == "bar one"
	res = await unit1.rpc("tests.test_unit.something_named", "two")
	assert res == "bar two"

@pytest.mark.asyncio
async def test_alert_callback(unit1, unit2, event_loop):
	alert_me = Mock(side_effect=lambda y: "bar "+y)
	await unit1.register_alert_async("my.alert",alert_me, call_conv=CC_DICT)
	await unit2.register_alert_async("my.alert",alert_me, call_conv=CC_DICT)
	n = 0
	def cb(x):
		nonlocal n
		n += 1
		assert x == "bar dud", x
	await unit2.alert("my.alert",y="dud",callback=cb,call_conv=CC_DATA, timeout=0.2)
	assert n == 2
	n = 0
	await unit1.alert("my.alert",_data={'y':"dud"},callback=cb,call_conv=CC_DATA, timeout=0.2)
	assert n == 2

@pytest.mark.asyncio
async def test_alert_uncodeable(unit1, unit2, event_loop):
	alert_me = Mock(side_effect=lambda : object())
	await unit1.register_alert_async("my.alert",alert_me, call_conv=CC_DICT)
	def cb(msg):
		assert False,"Called?"
	n = await unit2.alert("my.alert",callback=cb, timeout=0.2)
	assert n == 0

@pytest.mark.asyncio
async def test_alert_oneway(unit1, unit2, event_loop):
	alert_me1 = Mock()
	alert_me2 = Mock()
	alert_me3 = Mock()
	await unit1.register_alert_async("my.alert1",alert_me1, call_conv=CC_DICT)
	await unit1.register_alert_async("my.alert2",alert_me2, call_conv=CC_DATA)
	await unit1.register_alert_async("my.alert3",alert_me3) # default is CC_MSG
	await unit2.alert("my.alert1",_data={'y':"dud"})
	await unit2.alert("my.alert2",_data={'y':"dud"})
	await unit2.alert("my.alert3",_data={'y':"dud"})
	await asyncio.sleep(0.1)
	alert_me1.assert_called_with(y='dud')
	alert_me2.assert_called_with(dict(y='dud'))
	alert_me3.assert_called_with(AlertMsg(data=dict(y='dud')))

@pytest.mark.asyncio
async def test_alert_no_data(unit1, unit2, event_loop):
	alert_me1 = Mock(side_effect=lambda x: "")
	alert_me2 = Mock(side_effect=lambda : {})
	await unit1.register_alert_async("my.alert1",alert_me1, call_conv=CC_DATA)
	await unit2.register_alert_async("my.alert2",alert_me2, call_conv=CC_DICT)
	def recv1(d):
		assert d == 0
	async def recv2(*a,**k):
		await asyncio.sleep(0.01)
		assert not a
		assert not k
		return {}
	res = await unit2.alert("my.alert1",_data="", callback=recv1, call_conv=CC_DATA, timeout=0.2)
	alert_me1.assert_called_with(0)
	assert res == 1
	res = await unit2.alert("my.alert2", callback=recv2, call_conv=CC_DICT, timeout=0.2)
	alert_me2.assert_called_with()
	assert res == 1

@pytest.mark.asyncio
async def test_alert_stop(unit1, unit2, event_loop):
	async def sleep1():
		await asyncio.sleep(0.1)
	async def sleep2():
		await asyncio.sleep(0.2)
	await unit1.register_alert_async("my.sleep",sleep1)
	await unit2.register_alert_async("my.sleep",sleep2)
	def recv(msg):
		raise StopIteration
	res = await unit2.alert("my.sleep",_data="", callback=recv, timeout=0.15)
	assert res == 1

@pytest.mark.asyncio
async def test_reg(unit1, unit2, event_loop):
	def recv(**d):
		if d['uuid'] == unit1.uuid:
			assert d['app'] == unit1.app
		elif d['uuid'] == unit2.uuid:
			assert d['app'] == unit2.app
		else:
			assert False,d
	res = await unit2.alert("dabroker.ping", callback=recv, timeout=0.2, call_conv=CC_DICT)
	assert res == 2

	res = await unit2.rpc("dabroker.ping."+unit1.uuid)
	assert res['app'] == unit1.app
	assert "dabroker.ping."+unit1.uuid in res['rpc_endpoints']

@pytest.mark.asyncio
async def test_alert_error(unit1, unit2, event_loop):
	def err(x):
		raise RuntimeError("dad")
	error_me1 = Mock(side_effect=err)
	await unit1.register_alert_async("my.error1",error_me1, call_conv=CC_DATA)
	def recv1(d):
		assert d.error.cls == "RuntimeError"
		assert d.error.message == "dad"
	res = await unit2.alert("my.error1", _data="", callback=recv1, timeout=0.2)
	error_me1.assert_called_with(0)
	assert res == 1

	res = await unit2.alert("my.error1", callback=recv1, call_conv=CC_DATA, timeout=0.2)
	assert res == 0

	def recv2(msg):
		msg.raise_if_error()
	with pytest.raises(ReturnedError):
		await unit2.alert("my.error1", callback=recv2, timeout=0.2)

@pytest.mark.asyncio
async def test_reg_error(unit1):
	with pytest.raises(AssertionError):
		await unit1.register_rpc("my.call",Mock())
	with pytest.raises(AssertionError):
		await unit1.register_alert("my.alert",Mock())

@pytest.mark.asyncio
async def test_reg_error(unit1):
	with pytest.raises(AssertionError):
		await unit1.register_rpc("my.call",Mock())
	with pytest.raises(AssertionError):
		await unit1.register_alert("my.alert",Mock())

@pytest.mark.asyncio
async def test_rpc_bad_params(unit1, unit2, event_loop):
	call_me = Mock(side_effect=lambda x: "foo "+x)
	await unit1.register_rpc_async("my.call",call_me, call_conv=CC_DATA)
	try:
		res = await unit2.rpc("my.call", x="two")
	except ReturnedError as exc:
		assert exc.error.cls == "TypeError"
		assert "convert" in str(exc)
	else:
		assert False,"exception not called"
	
def test_reg_sync(event_loop):
	cfg = load_cfg("test.cfg")['config']
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

