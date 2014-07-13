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

from . import BaseTransport

import amqp

import logging
logger = logging.getLogger("dabroker.server.transport.amqp")

class AmqpTransport(BaseTransport):
	defaults = dict(host='localhost', username='', password='', virtual_host='/', rpc_queue='rpc_queue', info_queue='dab_info', exchange='dab_alert')
	connection = None

	_server = False

	def connect1(self):
		try:
			logger.debug("Connecting %s %s %s",self.cfg['host'],self.cfg['virtual_host'],self.cfg['username'])
			self.connection = amqp.connection.Connection(host=self.cfg['host'], userid=self.cfg['username'], password=self.cfg['password'], login_method='AMQPLAIN', login_response=None, virtual_host=self.cfg['virtual_host'])
			self.setup_channels()
		except Exception as e:
			logger.error("Not connected!")
			c,self.connection = self.connection,None
			if c is not None:
				c.close()
			self.disconnected()
			raise
		else:
			logger.debug("Connected: %s %r",self.__class__.__module__,self.connection)
		super(AmqpTransport,self).connect1()

	def disconnect(self):
		super(AmqpTransport,self).disconnect()
		c,self.connection = self.connection,None
		if c is not None:
			c.close()
		
	def disconnected(self, err=None):
		if self.connection:
			try: self.connection.close()
			except Exception: logger.exception("closing channel")
			self.connection = None
		super(AmqpTransport,self).disconnected(err)

	def setup_channels(self):
		raise NotImplementedError("Duh")

	def encode_msg(self,msg, **attrs):
		attrs.setdefault('content_type','application/x-dab')
		msg = amqp.Message(#application_headers={'type':typ},
			body=msg, **attrs)
		return msg
 
	def decode_msg(self,msg):
		return msg.body

	def run(self):
		logger.debug("Receiver loop on %r %r",self.connection,self.channel)
		while True:
			self.channel.wait()

