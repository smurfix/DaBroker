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

import gevent

import logging 
logger = logging.getLogger("dabroker.base.transport")

class ConnectionError(RuntimeError):
	pass

class BaseCallbacks(object):
	def recv(self,msg):
		"""Incoming message from the other side. NOT used for receiving replies!"""
		raise NotImplementedError("You need to override {}.recv()".format(self.__class__.__name__))

	def send(self,msg):
		"""Outgoing message to the other side. NOT used for sending replies!"""
		raise NotImplementedError("You need to override {}.send()".format(self.__class__.__name__))

	def ended(self,err=None):
		"""Called on receiver error. Do not reconnect here!"""
		pass

	def reconnect(self,err=None):
		"""Called after a closed connection has been cleaned up"""
		pass

	def register_codec(self,codec):
		raise NotImplementedError("You need to override {}.register_codec()".format(self.__class__.__name__))

class RelayedError(Exception):
	"""An encapsulation for a server error (with traceback)"""
	def __init__(self,err,tb):
		self.err = str(err)
		self.tb = tb

	def __repr__(self):
		return "{}({})".format(self.__class__.__name__,self.err)

	def __str__(self):
		r = repr(self)
		if self.tb is None: return r
		return r+"\n"+self.tb

class BaseTransport(object):
	_job = None
	_ready = None
	defaults = {}
	connection = None

	def __init__(self,callbacks, cfg={}):
		self.cfg = self.defaults.copy()
		self.cfg.update(cfg)
		self.callbacks = callbacks

	def connect(self):
		assert self.callbacks is not None
		assert self.connection is None

		if self._job is not None:
			raise RuntimeError("Already connecting")
		logger.debug("connecting: %r",self)

		from gevent.event import AsyncResult
		ready = AsyncResult()
		self.run_loop(ready)
		ready.get()

	def disconnect(self):
		"""Sever the connection."""
		logger.debug("disconnecting: %r",self)
		j,self._job = self.job,None
		if j:
			j.kill()

	def disconnected(self, err=None):
		"""Clear connection objects or whatever.

			This will be called by the reader task, as it exits."""
		logger.debug("disconnected: %r",self)
	
	def send(self,typ,msg):
		raise NotImplementedError("You need to override {}.send()".format(self.__class__.__name__))
	
	def run(self):
		raise NotImplementedError("You need to override {}.run()".format(self.__class__.__name__))

	def _run_job(self):
		try:
			logger.debug("Running receiver loop: %r",self)
			self.run()
		except BaseException as e:
			logger.exception("Receiver loop error: %r",self)
			self.callbacks.error(e)
		else:
			logger.debug("Receiver loop ends: %r",self)
			self.callbacks.error(None)
		finally:
			self.disconnected()
			self._job = None
			self.callbacks.reconnect(e)

	def run_loop(self, ready):
		"""Start/register the receiver loop. Default is to spawn a runner thread.
		
			@ready: AsyncResult to set when connected
			"""
		assert self._job is None
		ready.set(None)
		self._job = gevent.spawn(self._run_job)
		
