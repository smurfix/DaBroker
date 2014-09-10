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
	"""Talk using RabbitMQ or another AMQP-compabible messaging system."""
	defaults = dict(host='localhost', user='', password='', virtual_host='/', rpc_queue='rpc_queue', exchange='dab_alert')
	connection = None
	content_type = None

	def __init__(self,*a,**k):
		super(AmqpTransport,self).__init__(*a,**k)
		self.content_type = 'application/x-dab-'+self.cfg['codec']

	_server = False

	def connect1(self):
		try:
			logger.debug("Connecting %s %s %s",self.cfg['amqp_host'],self.cfg['amqp_virtual_host'],self.cfg['amqp_user'])
			self.connection = amqp.connection.Connection(host=self.cfg['amqp_host'], userid=self.cfg['amqp_user'], password=self.cfg['amqp_password'], login_method='AMQPLAIN', login_response=None, virtual_host=self.cfg['amqp_virtual_host'])
			self.setup_channels()
		except Exception as e:
			logger.error("Not connected to AMPQ: host=%s vhost=%s user=%s", self.cfg['amqp_host'],self.cfg['amqp_virtual_host'],self.cfg['amqp_user'])
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
		attrs.setdefault('content_type',self.content_type)
		msg = amqp.Message(body=msg, **attrs)
		return msg
 
	def decode_msg(self,msg):
		assert msg.content_type == self.content_type, (msg.content_type,self.content_type)
		return msg.body

	def run(self):
		logger.debug("Receiver loop on %r %r",self.connection,self.channel)
		while True:
			self.channel.wait()

