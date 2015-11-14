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

"""\
	This implements a bunch of mostly-generic protocol handling classes.
	"""
import asyncio

class Disconnected(BaseException):
	pass

class Protocol(asyncio.Protocol):
	paused = asyncio.Future()
	paused.set_result(False)

	def __init__(self):
		self.queue = asyncio.Queue()

	def close(self):
		self.transport.close()
	
	def connection_lost(self, exc):
		if exc is None:
			exc = Disconnected()
		if not self.paused.done():
			self.paused.set_exception(exc)
		self.queue.put_nowait(exc)
		
	def data_received(self, data):
		"""Override this method to assemble and yield messages."""
		try:
			for m in self.received(self,data):
				self.queue.put_nowait(m)
		except BaseException as exc:
			if not self.paused.done():
				self.paused.set_exception(exc)
			self.queue.put_nowait(exc)

	def received(self, data):
		raise NotImplementedError("You need to override %s.receive" % self.__class__.__name__)

	def pause_writing(self):
		self.paused = asyncio.Future()
	def resume_writing(self):
		self.paused.set_result(True)
		
class ProtocolInteraction(object):
	"""\
		A generic message read/write thing.

		You override interact() to send and receive messages.
		A client typically sends a message, waits for a reply (or more), possibly repeats, then exits.

		@asyncio.coroutine
		def interact(self):
			yield from self.paused ## periodically do this if you send lots
			self.send("Foo!")
			assert (yield from self.recv()) == "Bar?"
			
		"""

	def __init__(self):
		self._protocol = None

	@property
	def paused(self):
		return self._paused()
	@asyncio.coroutine
	def _paused(self):
		p = self._protocol.paused
		if not p.done():
			yield p
			self._protocol.paused.result()

	@asyncio.coroutine
	def interact(self,*a,**k):
		raise NotImplementedError("You need to override %s.interact" % self.__class__.__name__)

	def send(self,*a,**k):
		self._protocol.send(*a,**k)

	@asyncio.coroutine
	def recv(self):
		res = yield from self._protocol.queue.get()
		if isinstance(res,BaseException):
			raise res
		return res
		
class ProtocolClient(object):
	"""\
		A generic streaming client, using multiple connections
		"""
	MAX_IDLE = 10
    def __init__(self, host,port):
		self.protocol = protocol
        self.host = host
        self.port = port
        self.conns = []

    @asyncio.coroutine
    def _get_conn(self):
        now = time()
        while self.conns:
            ts,conn = conns.pop()
            if ts > now-self.MAX_IDLE:
                break
			conn.close()
        else:
            conn = yield from loop.create_connection(self.protocol, self.host,self.port)
        return conn
        
    @asyncio.coroutine
    def run(self, interaction):
		"""\
			Run the interaction on this connection.
			"""
        conn = yield from self._get_conn()
        try:
			assert interaction._protocol is None
			interaction._protocol = conn
            yield from interaction.interact(conn)
        else:
            self._put_conn(conn)
            conn = None
        finally:
			interaction._protocol = None
            if conn is not None:
                conn.close()

