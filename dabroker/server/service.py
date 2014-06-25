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

# This implements the main broker server.

from ..base.serial import Codec
from .loader import get
from .serial import adapters

from traceback import format_exc

import logging
logger = logging.getLogger("dabroker.server.service")

class UnknownCommandError(Exception):
	def __init__(self, cmd):
		self.cmd = cmd
	def __repr__(self):
		return "{}({})".format(self.__class__.__name__,repr(self.cmd))
	def __str__(self):
		return "Unknown command: {}".format(repr(self.cmd))

class BrokerServer(object):
	"""\
		The main server.

		@sender: code which broadcasts to all clients.
		"""
	def __init__(self, sender=None):
		# the sender might be set later
		self.codec = Codec()
		self.codec.register(adapters)
		self.sender = sender

	def do_echo(self,msg):
		logger.debug("echo %r",msg)
		return msg

	def do_ping(self,msg):
		logger.debug("ping %r",msg)
		
	def do_pong(self,msg):
		logger.debug("pong %r",msg)
		
	def do_call(self,msg,o=None,a=(),k={}):
		logger.debug("call %r.%r(*%r, **%r)",o,msg,a,k)
		assert msg in o._meta.calls,"You cannot call method {} of {}".format(msg,o)
		res = getattr(o,msg)(*a,**k)
		logger.debug("call %r.%r(*%r, **%r) = %r",o,msg,a,k, res)
		return res

	def do_get(self, key):
		key = tuple(key)
		logger.debug("get %r",key)
		return get(key)
	do_get.include = True
		
	def do_find(self, key, lim=None, k={}):
		logger.debug("find %r %r",key,k)
		key = tuple(key)
		info = get(key)
		return info.obj_find(_limit=lim,**k)
	do_find.include = True
		
	def recv(self, msg):
		"""Basic message receiver. Ususally in a separate thread."""
		logger.debug("recv raw %r",msg)
		msg = self.codec.decode(msg)
		logger.debug("recv dec %r",msg)
		job = msg.pop('_a')
		m = msg.pop('_m',msg)

		try:
			try:
				proc = getattr(self,'do_'+job)
			except AttributeError:
				raise UnknownCommandError(job)
			msg = proc(m,**msg)
			logger.debug("send dec %r",msg)
			msg = self.codec.encode(msg, include=getattr(proc,'include',False))
			logger.debug("send raw %r",msg)
			return msg
		except Exception as e:
			tb = format_exc()
			return {'error': str(e), 'tb':tb}

	def send(self, action, msg=None, **kw):
		"""Basic message broadcaster"""
		logger.debug("bcast dec %s %r",action,msg)
		kw['_m'] = msg
		kw['_a'] = action
		msg = self.codec.encode(kw)
		logger.debug("bcast raw %r",msg)
		self.sender(msg)

