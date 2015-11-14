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
def test_echo(event_loop, echoserver):
	f = asyncio.Future()
	coro = event_loop.create_connection(lambda: EchoClientProtocol(f),
							  '127.0.0.1', echoserver.port)
	yield from coro
	yield from f
	assert f.result() is True

class LinesTester(ProtocolInteraction):
	@asyncio.coroutine
	def interact(self):
		self.send("whatever\nwherever")
		yield from asyncio.sleep(D/2, loop=self._loop)
		self.send("whenever")
		m = yield from self.recv()
		n = yield from self.recv()
		o = yield from self.recv()
		assert m == "whatever"
		assert n == "wherever"
		assert o == "whenever"
	
@pytest.mark.asyncio
def test_lines(echoserver, event_loop):
	c = ProtocolClient(LineProtocol, "127.0.0.1",echoserver.port, loop=event_loop)
	yield from c.run(LinesTester(loop=event_loop))
	yield from c.close()

	# now do two at the same time, and abort
	f = asyncio.async(c.run(LinesTester(loop=event_loop)), loop=event_loop)
	yield from asyncio.sleep(D)
	g = asyncio.async(c.run(LinesTester(loop=event_loop)), loop=event_loop)
	yield from f
	c.abort()
	with pytest.raises(asyncio.CancelledError):
		yield from g
	with pytest.raises(asyncio.CancelledError):
		g.result()

	# let the mainloop process things
	yield from asyncio.sleep(D/2, loop=event_loop)

