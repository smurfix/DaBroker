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

from ...base.transport.amqp import AmqpTransport

import amqp

import logging
logger = logging.getLogger("dabroker.server.transport.amqp")

class Transport(AmqpTransport):
	_server = True

	def on_rpc(self, msg):
		body = msg.body
		props = msg.properties
		ch = msg.channel
		delivery_info = msg.delivery_info

		m = self.decode_msg(msg)
		if m is None: return
		try:
			response = self.callbacks.recv(m)
		except Exception as e:
			logger.exception("This should never happen")
			raise
		msg = self.encode_msg(response, correlation_id=props['correlation_id'])
		ch.basic_publish(msg=msg, exchange='', routing_key=props['reply_to'])
		ch.basic_ack(delivery_tag = delivery_info['delivery_tag'])

	def purge_all(self):
		super(Transport,self).purge_all()
		self.channel.queue_purge(queue=self.cfg['rpc_queue'])

	def setup_channels(self):
		self.channel = self.connection.channel()
		self.channel.queue_declare(queue=self.cfg['rpc_queue'], auto_delete=False, passive=False)
		self.channel.basic_qos(prefetch_count=1,prefetch_size=0,a_global=False)
		logger.debug("Listen RPC %s",self.cfg['rpc_queue'])
		self.channel.basic_consume(callback=self.on_rpc, queue=self.cfg['rpc_queue'])

		self.channel.exchange_declare(exchange=self.cfg['rpc_exchange'], type='fanout', auto_delete=False, passive=False)

	def send(self, msg):
		msg = self.encode_msg(msg)
		self.channel.basic_publish(msg=msg, exchange=self.cfg['rpc_exchange'], routing_key='dab_info')#typ)
		
