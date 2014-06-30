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
	defaults = { "host":'localhost',"username":'',"password":'', "virtual_host"='/', "rpc_queue":'dab_rpc', "info_queue":'dab_info', "exchange":'dab_alert'}
	connection = None

	_server = False

	def disconnected(self):
		if self.connection:
			try: self.connection.close()
			except Exception: logger.exception("closing channel")
			self.connection = None
		super(Transport,self).disconnected()

	def setup_channels(self):
		raise NotImplementedError("Duh")

	def encode_msg(self,typ,msg, **attrs):
		attrs.setdefault('content_type','application/x-dab')
		msg = amqp.Message(application_headers={'type':typ}, body=msg, **attrs)
 
	def decode_msg(self,msg):
		import pdb;pdb.set_trace()
		return msg.body

	def run(self,ready):
		logger.debug("Start connecting")
		try:
			self.connection = amqp.connection.Connection(host=self.cfg['host'], userid=self.cfg['username'], password=self.cfg['password'], login_method='AMQPLAIN', login_response=None, virtual_host=self.cfg['virtual_host'])
			self.setup_channels()
			
		except Exception as e:
			self.disconnected()
			ready.set_exception(e)
			return
		else:
			logger.debug("Connected!")
			ready.set(None)

		while True: channel.wait()

