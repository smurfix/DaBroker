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

from ..base.transport.amqp import AmqpTransport

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
		try:
			response = self.callbacks.recv(m)
		except Exception as e:
			response = str(e)
			msg = self.encode_msg('rpc_error', response, correlation_id=props['correlation_id'])
		else:
			msg = self.encode_msg('rpc_response', msg)
		ch.basic_publish(msg=msg, exchange='', routing_key=props['reply_to'])
		ch.basic_ack(delivery_tag = delivery_info['delivery_tag'])

	def setup_channels(self):
		self.rpc_channel = self.connection.channel()
		self.rpc_channel.queue_declare(queue=self.cfg['rpc_queue'], auto_delete=False, passive=False)

		self.rpc_channel.basic_qos(prefetch_count=1,prefetch_size=0,a_global=False)
		self.rpc_channel.basic_consume(callback=self.on_rpc, queue=self.cfg['info_queue'])

		self.info_channel = self.connection.channel()
		self.info_channel.exchange_declare(exchange=self.cfg['exchange'], type='fanout')

	def send(self, typ,msg):
        msg = self.encode_msg(typ,msg)
        self.info_channel.basic_publish(msg=msg, exchange=self.cfg['exchange'], routing_key=typ)
		