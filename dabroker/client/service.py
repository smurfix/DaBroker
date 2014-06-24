# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including an optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

# This module implements the basic client server.

RETR_TIMEOUT = 10
CACHE_SIZE=10000

from ..base.serial import Codec
from .serial import adapters, client_broker_info_meta

import logging
logger = logging.getLogger("dabroker.client.service")

from weakref import WeakValueDictionary
from collections import deque
from gevent.event import AsyncResult

class ServerError(Exception):
	"""An encapsulation for a server error (with traceback)"""
	def __init__(self,err,tb):
		self.err = err
		self.tb = tb

	def __repr__(self):
		return "ServerError({})".format(repr(self.err))

	def __str__(self):
		r = repr(self)
		if self.tb is None: return r
		return r+"\n"+self.tb

class BrokerClient(object):
	"""\
		The basic client implementation. Singleton (for now).

		@server: a callable which sends a message to the server (and returns a reply)
		"""
	root_key = None

	def __init__(self, server):
		global client
		assert client is None

		self.server = server
		self.codec = Codec()
		self.codec.register(adapters)

		# Basic cache.
		self._cache = WeakValueDictionary()
		self._lru = deque(maxlen=CACHE_SIZE)

		self._add_to_cache(client_broker_info_meta)

		client = self

	def _add_to_cache(self, obj):
		key = getattr(obj,'_key',None)
		if key is None:
			old = None
		else:
			old = self._cache.get(key, None)
			if old is obj:
				return
		if obj is not None:
			self._cache[key] = obj
			self._lru.append(obj)
		if isinstance(old,AsyncResult):
			old.set(obj)

	def get(self, key):
		"""Get an object, from cache or from the server."""
		obj = self._cache.get(key,None)
		if obj is not None:
			if isinstance(obj,AsyncResult):
				obj = obj.get(timeout=RETR_TIMEOUT)
			return obj
		self._cache[key] = ar = AsyncResult()
		try:
			obj = self._send("get",key)
		except Exception as e:
			# Remove the AsyncResult from cache and forward the excption to any waiters
			self._cache.pop(key).set_exception(e)
			del ar # otherwise the optimizer will drop this, and thus
			       # delete the weakref, _before_ the previous line!
			raise
		else:
			self._add_to_cache(obj) # sends to the AsyncResult as a side effect
			return obj
		
	@property
	def root(self):
		"""Get the object root. This may or may not be a cacheable object."""
		rk = self.root_key
		if rk is not None:
			if isinstance(rk,AsyncResult):
				return rk.get(timeout=RETR_TIMEOUT)
			return self.get(self.root_key)

		self.root_key = rk = AsyncResult()
		try:
			obj = self._send("root")
		except Exception as e:
			self.root_key = None
			rk.set_exception(e)
			raise
		else:
			self.root_key = getattr(obj,"_key",None)
			if self.root_key is not None:
				self._add_to_cache(obj)
			rk.set(obj)
			return obj

	def _send(self, action, msg=None, **kw):
		"""Low-level method for RPCing the server"""
		logger.debug("send dec %s %r",action,msg)
		kw['m'] = msg
		kw['a'] = action
		msg = self.codec.encode(kw)
		logger.debug("send raw %r",msg)

		msg = self.server(msg)

		logger.debug("recv raw %r",msg)
		msg = self.codec.decode(msg)
		logger.debug("recv dec %r",msg)

		if isinstance(msg,dict) and 'error' in msg:
			raise ServerError(msg['error'],msg.get('tb',None))
		return msg

client = None
