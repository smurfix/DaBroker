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

# This is the main code of the broker.

from ..util import import_string
from ..util.thread import Main
from ..base.transport import BaseCallbacks
from ..base.config import default_config
from .codec import adapters
from .service import BrokerServer

from gevent import sleep
from gevent.event import AsyncResult

import sys
import logging
logger = logging.getLogger("dabroker.server")

class BrokerMain(Main,BaseCallbacks):
	"""\
		Base class for the DaBroker server.
		"""
	queue = None
	root = None
	quitting = None
	ready = None
	
	def __init__(self, cfg={}, ready=None):
		logger.debug("Setting up")
		self.ready = ready
		self.quitting = AsyncResult()

		self.cfg = default_config.copy()
		self.cfg.update(cfg)
		super(BrokerMain,self).__init__()

	def make_loader(self):
		from .loader import Loaders
		return Loaders()

	def make_transport(self):
		name = self.cfg['transport']
		if '.' not in name:
			name = "dabroker.server.transport."+name+".Transport"
		return import_string(name)(cfg=self.cfg, callbacks=self)

	def make_codec(self, loader, adapters):
		name = self.cfg['codec']
		if '.' not in name:
			name = "dabroker.base.codec."+name+".Codec"
		return import_string(name)(loader=loader, adapters=adapters, cfg=self.cfg)

	def register_codec(self,adapter):
		self.codec.register(adapter)

	@property
	def root(self):
		raise NotImplementedError("You need to override the root object generator")

	def make_server(self):
		return BrokerServer(self, loader=self.loader)

	def setup(self):
		self.loader = self.make_loader()
		self.codec = self.make_codec(self.loader,adapters)
		self.transport = self.make_transport()
		self.server = self.make_server()

		self.transport.connect()

	def send(self,msg):
		msg = self.codec.encode(msg)
		self.transport.send(msg)
		
	def recv(self,msg):
		try:
			msg = self.codec.decode(msg)
			msg,attrs = self.server.recv(msg)
			msg = self.codec.encode(msg, include=attrs.get('include',False))
			return msg
		except BaseException as e:
			return self.codec.encode_error(e, sys.exc_info()[2])
		
	def main(self):
		logger.debug("Running")
		if self.ready is not None:
			self.ready.set(None)

		try:
			self.quitting.get()
		finally:
			self.quitting = None
		
	def end(self):
		if self.quitting is not None:
			self.quitting.set(None)
		super(BrokerMain,self).end()

