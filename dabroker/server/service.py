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

# This implements the main broker server.

from .loader import Loaders
from .codec import adapters as default_adapters

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
	def __init__(self, server, loader=None, adapters=None):
		# the sender might be set later
		self.server = server
		if loader is None:
			loader = Loaders(server=self)
		self.loader = loader
		if adapters is None:
			adapters = default_adapters
		self.server.register_codec(adapters)

	# remote calls

	def do_root(self,msg):
		logger.debug("Get root %r",msg)
		res = self.server.root
		if not hasattr(res,'_key'):
			self.loader.static.add(res,'root')
		if not hasattr(res._meta,'_key'):
			self.loader.static.add(res._meta,'root','meta')
		return res
	do_root.include = True

	def do_echo(self,msg):
		logger.debug("echo %r",msg)
		return msg

	def do_ping(self,msg):
		logger.debug("ping %r",msg)

	def do_pong(self,msg):
		logger.debug("pong %r",msg)

	def do_get(self, key):
		"""Fetch an object by ID"""
		key = tuple(key)
		logger.debug("get %r",key)
		return self.loader.get(key)
	do_get.include = True

	def do_update(self,key,k={}):
		logger.debug("update %r %r",key,k)
		key = tuple(key)
		obj = self.loader.get(key)
		return obj._meta.update(obj,**k)

	def do_find(self, key, lim=None, k={}):
		"""Search for objects"""
		logger.debug("find %r %r",key,k)
		key = tuple(key)
		info = self.loader.get(key)
		return info.obj_find(_limit=lim,**k)
	do_find.include = True
		
	# Broadcast messages to clients

	def send_ping(self, msg):
		self.send("ping",msg)
		
	# The next three broadcast messages are used for broadcastng object
	# changes. They will invalidate possibly-matching search results.
	def send_created(self, obj):
		"""This object has been created."""
		attrs = dict((k,(v,)) for k,v in obj._attrs.items())
		self.send("invalid_key",None, m=obj._meta._key, k=attrs)

	def send_deleted(self, obj):
		"""This object has been deleted."""
		attrs = dict((k,(v,)) for k,v in obj._attrs.items())
		self.send("invalid_key",obj._key, m=obj._meta._key, k=attrs)

	def send_updated(self, obj, attrs):
		"""\
			An object has been updated.
		
			@kw is _attrs from the old object state.
			"""
		key = obj._key
		mkey = obj._meta._key
		refs = obj._meta.refs
		for k,on in attrs.items():
			if k in refs:
				ov,nv = on
				if ov is not None: ov = ov._key
				if nv is not None: nv = nv._key
				attrs[k] = (ov,nv)
		self.send("invalid_key",key, m=mkey, k=attrs)
		
	def recv(self, msg):
		"""Basic message receiver. Usually called from a separate thread."""
		#logger.debug("recv raw %r",msg)
		logger.debug("recv %r",msg)
		job = msg.pop('_a')
		m = msg.pop('_m',msg)

		try:
			if job == "call":
				# unwrap it so we can get at the proc's attributes
				a = msg.get('a',())
				k = msg.get('k',{})
				o = msg['o']
				assert m in o._meta.calls,"You cannot call method {} of {}".format(msg,o)
				proc = getattr(o,m)
			else:
				a = (m,)
				k = msg
				proc = getattr(self,'do_'+job)
		except AttributeError:
			raise UnknownCommandError(job)
		msg = proc(*a,**k)
		logger.debug("reply %r",msg)
		attrs = {'include':getattr(proc,'include',False)}
		return msg,attrs

	def send(self, action, *a, **k):
		"""Basic message broadcaster"""
		logger.debug("bcast %s %r %r",action,a,k)
		msg = {'_m':action,'_a':a,'_k':k}
		self.server.send(msg)

