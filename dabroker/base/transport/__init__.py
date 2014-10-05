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

from gevent import GreenletExit

from dabroker.util.thread import prep_spawned

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
	defaults = {}
	connection = None
	last_msgid = 0

	def __init__(self,callbacks, cfg={}):
		self.cfg = self.defaults.copy()
		self.cfg.update(cfg)
		self.callbacks = callbacks

	def connect(self, purge=False):
		"""Connect. (Synchronously.)
		
		Do not override!
		Override .connect1() (setup) and .connect2() (initial tasks)"""
		assert self.callbacks is not None
		assert self.connection is None
		self.connect1()
		if purge:
			self.purge_all()
		self.connect2()

	def connect1(self):
		"""Set up a connection.

		Call super() before your code."""

		if self._job is not None:
			raise RuntimeError("Already connected")
		logger.debug("connecting: %r",self)

	def connect2(self):
		"""Add initial tasks after a connection has been established.

		Call super() after your code."""
		assert self._job is None
		self._job = self._run_job()
		self._job.start()

	def disconnect(self):
		"""Sever the connection; do not auto-reconnect."""
		logger.debug("disconnecting: %r",self)
		j,self._job = self._job,None
		if j:
			j.stop()

	def disconnected(self, err=None):
		"""Clear connection objects.

			This will be called by the reader task as it exits.
			Do not reconnect from here; do that in your .reconnect"""
		logger.debug("disconnected: %r",self)
	
	def purge_all(self):
		"""
			Clear this transport's message queue.

			This should only be called when client and server are known to
			be idle AND when you suspect an unprocessable message might
			clog the queue.
			"""
		pass

	def send(self,msg):
		raise NotImplementedError("You need to override {}.send()".format(self.__class__.__name__))
	
	def run(self):
		raise NotImplementedError("You need to override {}.run()".format(self.__class__.__name__))

	@prep_spawned
	def _run_job(self):
		try:
			logger.debug("Running receiver loop: %r",self)
			self.run()
		except GreenletExit:
			err=None
			logger.debug("Receiver loop ends: %r",self)
			self.callbacks.ended(None)
		except BaseException as e:
			err = e
			logger.exception("Receiver loop error: %r",self)
			self.callbacks.ended(e)
		else:
			err=None
			logger.debug("Receiver loop ends: %r",self)
			self.callbacks.ended(None)
		finally:
			self.disconnected()
			if self._job is not None:
				self._job = None
				self.callbacks.reconnect(err)

