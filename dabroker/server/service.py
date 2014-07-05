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
from ..base import UnknownCommandError
from ..util import import_string
from ..base.config import default_config
from ..base.transport import BaseCallbacks
from .codec import adapters as default_adapters

import sys
from traceback import format_exc

import logging
logger = logging.getLogger("dabroker.server.service")

class BrokerServer(BaseCallbacks):
	"""\
		The main server.

		"""
	root = None

	def __init__(self, cfg={}, loader=None, adapters=()):
		# the sender might be set later
		logger.debug("Setting up")
		self.cfg = default_config.copy()
		self.cfg.update(cfg)

		if loader is None:
			loader = Loaders(server=self)
		self.loader = loader
		self.codec = self.make_codec(default_adapters)
		self.transport = self.make_transport()
		self.register_codec(adapters)
		super(BrokerServer,self).__init__()

	def make_loader(self):
		from .loader import Loaders
		return Loaders()

	def make_transport(self):
		name = self.cfg['transport']
		if '.' not in name:
			name = "dabroker.server.transport."+name+".Transport"
		return import_string(name)(cfg=self.cfg, callbacks=self)

	def make_codec(self, adapters):
		name = self.cfg['codec']
		if '.' not in name:
			name = "dabroker.base.codec."+name+".Codec"
		return import_string(name)(loader=self.loader, adapters=adapters, cfg=self.cfg)

	def register_codec(self,adapter):
		self.codec.register(adapter)

	def start(self):
		self.transport.connect()

	def stop(self):
		self.transport.disconnect()

	# remote calls

	def do_root(self):
		logger.debug("Get root")
		res = self.root
		if not hasattr(res,'_key'):
			self.loader.static.add(res,'root')
		if not hasattr(res._meta,'_key'):
			self.loader.static.add(res._meta,'root','meta')
		return res
	do_root.include = True

	def do_echo(self,msg):
		logger.debug("echo %r",msg)
		return msg

	def do_ping(self):
		logger.debug("ping")

	def do_pong(self):
		logger.debug("pong")

	def do_get(self, key):
		"""Fetch an object by its key"""
		key = tuple(key)
		logger.debug("get %r",key)
		return self.loader.get(key)
	do_get.include = True

	def do_update(self,key,k={}):
		"""Update an object.
		
			@k: A map of key => (old_value,new_value)
			"""
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
		self.send("invalid_key", _meta=obj._meta._key, **attrs)

	def send_deleted(self, obj):
		"""This object has been deleted."""
		attrs = dict((k,(v,)) for k,v in obj._attrs.items())
		self.send("invalid_key", _key=obj._key, _meta=obj._meta._key, **attrs)

	def send_updated(self, obj, attrs):
		"""\
			An object has been updated.
		
			@attrs is the object change map.
			"""
		key = obj._key
		mkey = obj._meta._key
		refs = obj._meta.refs
		for k,on in attrs.items():
			ov,nv = on
			if k in refs:
				if ov is not None: ov = ov._key
				if nv is not None: nv = nv._key
			if ov != nv:
				attrs[k] = (ov,nv)
		self.send("invalid_key", _key=key, _meta=mkey, **attrs)
		
	# Basic transport handling

	def recv(self, msg):
		"""Receive a message. Usually called from a separate thread."""
		#logger.debug("recv raw %r",msg)
		try:
			msg = self.codec.decode(msg)

			logger.debug("recv %r",msg)
			m = msg.pop('_m')
			o = msg.pop('_o',None)
			a = msg.pop('_a',())

			try:
				if o is not None:
					assert m in o._meta.calls,"You cannot call method {} of {}".format(m,o)
					proc = getattr(o,m)
				else:
					proc = getattr(self,'do_'+m)
			except AttributeError:
				raise UnknownCommandError((m,o))
			msg = proc(*a,**msg)
			logger.debug("reply %r",msg)
			attrs = {'include':getattr(proc,'include',False)}
			msg = self.codec.encode(msg, include=attrs.get('include',False))
			return msg

		except BaseException as e:
			return self.codec.encode_error(e, sys.exc_info()[2])

	def send(self, action, *a, **k):
		"""Broadcast a message to all clients"""
		logger.debug("bcast %s %r %r",action,a,k)
		msg = k
		msg['_m'] = action
		if a:
			msg['_a'] = a

		msg = self.codec.encode(msg)
		self.transport.send(msg)


