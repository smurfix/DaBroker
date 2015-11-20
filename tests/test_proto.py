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
from dabroker.proto import ProtocolClient,ProtocolInteraction
from dabroker.proto.lines import LineProtocol
from dabroker.util.tests import load_cfg
import unittest
from unittest.mock import Mock

D=0.2

class EchoServerClientProtocol(asyncio.Protocol):
	def __init__(self,loop):
		self._loop = loop

	def connection_made(self, transport):
		peername = transport.get_extra_info('peername')
		print('Connection from:',peername)
		self.transport = transport

	def data_received(self, data):
		message = data.decode()
		print("Message:",message)
		self._loop.call_later(3*D, self._echo,data)

	def _echo(self,data):
		self.transport.write(data)

	def connection_lost(self, exc):
		print("Conn closed:",exc)

class EchoClientProtocol(asyncio.Protocol):
	def __init__(self, future):
		self.future = future
		self.done = False

	def connection_made(self, transport):
		self.transport = transport
		transport.write(b"Hello!\n");

	def data_received(self, data):
		assert data == b"Hello!\n"
		self.done = True
		self.transport.close()

	def connection_lost(self, exc):
		self.future.set_result(self.done)

@pytest.yield_fixture
def echoserver(event_loop, unused_tcp_port):
	# Each client connection will create a new protocol instance
	coro = event_loop.create_server(lambda: EchoServerClientProtocol(event_loop), '127.0.0.1', unused_tcp_port)
	server = event_loop.run_until_complete(coro)
	server.port = unused_tcp_port
	yield server
	server.close()
	event_loop.run_until_complete(server.wait_closed())

@pytest.mark.asyncio
async def test_echo(event_loop, echoserver):
	f = asyncio.Future()
	coro = event_loop.create_connection(lambda: EchoClientProtocol(f),
							  '127.0.0.1', echoserver.port)
	await coro
	await f
	assert f.result() is True

class LinesTester(ProtocolInteraction):
	def __init__(self,conn_store=None,**k):
		self.conn_store = conn_store
		super().__init__(**k)

	async def interact(self):
		if self.conn_store is not None:
			self.conn_store.append(self._protocol)
		self.send("whatever\nwherever")
		await asyncio.sleep(D/2, loop=self._loop)
		self.send("whenever")
		m = await self.recv()
		n = await self.recv()
		o = await self.recv()
		assert m == "whatever"
		assert n == "wherever"
		assert o == "whenever"
	
@pytest.mark.asyncio
async def test_lines(echoserver, event_loop):
	"""Use the "lines" protocol to go through the basic protocol features"""
	c = ProtocolClient(LineProtocol, "127.0.0.1",echoserver.port, loop=event_loop)
	await c.run(LinesTester(loop=event_loop))
	assert len(c.conns) == 1
	fff=[]
	tp = LinesTester(fff,loop=event_loop,conn=c)
	e = asyncio.ensure_future(tp.run(), loop=event_loop)
	# give 'e' time to start up
	await asyncio.sleep(D/3, loop=event_loop)
	# make sure close() waits for 'e' and doesn't break it
	await c.close()
	assert e.done()
	e.result()
	assert len(c.conns) == 0
	# create an idle connection which the next step can re-use
	eee=[]
	await c.run(LinesTester(eee, loop=event_loop))
	assert len(c.conns) == 1

	# now do two at the same time, and abort
	fff.pop()
	ff = c.run(tp)
	f = asyncio.ensure_future(ff, loop=event_loop)
	await asyncio.sleep(D)
	with pytest.raises(RuntimeError):
		await c.run(tp)
	g = asyncio.ensure_future(c.run(LinesTester(loop=event_loop)), loop=event_loop)
	await asyncio.sleep(D)
	await f
	# check that the connection is reused
	assert len(eee) == 1
	assert len(fff) == 1
	assert eee[0] == fff[0]

	c.MAX_IDLE = 0
	hhh = []
	hh = c.run(LinesTester(hhh, loop=event_loop))
	h = asyncio.ensure_future(hh, loop=event_loop)
	await asyncio.sleep(D/3, loop=event_loop)
	assert len(hhh) == 1
	assert eee[0] != hhh[0]
	c.abort()
	assert len(c.conns) == 0
	with pytest.raises(asyncio.CancelledError):
		await g
	with pytest.raises(asyncio.CancelledError):
		g.result()
	with pytest.raises(asyncio.CancelledError):
		await h

	# let the mainloop process things
	await asyncio.sleep(D/2, loop=event_loop)

