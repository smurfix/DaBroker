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

from ..util import import_string
from ..util.thread import Main
from ..base.config import default_config
from ..base.codec import ServerError
from ..base.transport import BaseCallbacks
from .codec import adapters
from .service import BrokerClient
from gevent import sleep
from gevent.event import AsyncResult

import logging
logger = logging.getLogger("dabroker.client")

class ClientMain(Main,BaseCallbacks):
	"""\
		Base class for the DaBroker client.

		You need to override at least .main(), because, well, duh.
		"""
	
	codec = None

	def __init__(self, cfg={}):
		logger.debug("Setting up")
		self._adapters = []

		self.cfg = default_config.copy()
		self.cfg.update(cfg)
		super(ClientMain,self).__init__()

	def make_transport(self):
		name = self.cfg['transport']
		if '.' not in name:
			name = "dabroker.server.transport."+name+".Transport"
		return import_string(name)(cfg=self.cfg, callbacks=self)

	def make_codec(self, loader, adapters=()):
		self._adapters.extend(adapters)
		name = self.cfg['codec']
		if '.' not in name:
			name = "dabroker.base.codec."+name+".Codec"
		return import_string(name)(loader=loader, cfg=self.cfg)

	def register_codec(self,adapter):
		self.codec.register(adapter)

	def make_client(self):
		return BrokerClient(self)

	def setup(self):
		self.client = self.make_client()
		self.codec = self.make_codec(loader=self.client, adapters=adapters)
		self.transport = self.make_transport()
		self.codec.register(self._adapters)

		self.transport.connect()

	@property
	def root(self):
		return self.client.root

	def register_codec(self,adapter):
		if self.codec is None:
			self._adapters.append(adapter)
		else:
			self.codec.register(adapter)

	def loader(self,key):
		return self.client.get(key)

	def recv(self,msg):
		try:
			msg = self.codec.decode(msg)
		except ServerError as e:
			logger.exception("Server sends us an error. Shutting down.")
			self.end()
		else:
			self.client.recv(msg)
		
	def send(self,msg):
		"""Low-level message sender"""
		logger.debug("Send req: %r",msg)
		msg = self.codec.encode(msg)
		msg = self.transport.send(msg)
		msg = self.codec.decode(msg)
		logger.debug("Recv reply: %r",msg)
		return msg

	def main(self):
		raise NotImplementedError("You need to override the main code if you want to actually do anything!")
		
	def end(self):
		super(ClientMain,self).end()

