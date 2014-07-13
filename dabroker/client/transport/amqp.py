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

from ...base.transport import ConnectionError
from ...base.transport.amqp import AmqpTransport
from ...util.thread import AsyncResult

import os
import base64
import amqp

def random_id():
	res = os.urandom(15)
	return str(base64.b64encode(res))

import logging
logger = logging.getLogger("dabroker.client.transport.amqp")

class Transport(AmqpTransport):
	def __init__(self,*a,**k):
		self.replies = {}
		super(Transport,self).__init__(*a,**k)

	def disconnected(self,err=None):
		if err is None:
			e = ConnectionError("disconnected")
		else:
			e = err
		repl,self.replies = self.replies,{}
		for v in repl.values():
			v.set_exception(e)
		super(Transport,self).disconnected(err)

	def on_rpc_response(self, msg):
		# TODO: read the type and emit an error if it's not a sane reply
		msgid = msg.properties['correlation_id']
		m = self.decode_msg(msg)
		if msgid in self.replies:
			self.replies[msgid].set(m)
		else:
			logger.warning("Unknown message: %s %r",msgid,m)

	def on_info_msg(self, msg):
		# TODO: read the type and emit an error if it's not a sane reply
		m = self.decode_msg(msg)
		self.callbacks.recv(m)

	def _asyncDecode(res,res_dec):
		try:
			res_dec.set(self.decode_msg(res.get()))
		except BaseException as e:
			res_dec.set_exception(e)

	def send(self, msg):
		msgid = random_id()
		res = AsyncResult()
		assert msgid not in self.replies
		self.replies[msgid] = res

		msg = self.encode_msg(msg, correlation_id=msgid, reply_to = self.callback_queue)
		logger.debug("send %s %r to %s",msgid,msg, self.cfg['rpc_queue'])
		#self.rpc_channel.basic_publish(exchange='', routing_key=self.cfg['rpc_queue'], msg=msg)
		self.channel.basic_publish(exchange='', routing_key=self.cfg['rpc_queue'], msg=msg)
		return res.get()

	def setup_channels(self):
		self.channel = self.connection.channel()
		res = self.channel.queue_declare(exclusive=True)
		self.callback_queue = res.queue
		self.channel.basic_consume(callback=self.on_rpc_response, queue=self.callback_queue, no_ack=True)

#		self.rpc_channel = self.connection.channel()
#		self.rpc_channel.queue_declare(queue=self.cfg['rpc_queue'], auto_delete=False, passive=True)

#		self.info_channel = self.connection.channel()
		self.channel.exchange_declare(exchange=self.cfg['exchange'], type='fanout', auto_delete=False)

		res = self.channel.queue_declare(exclusive=True)
		self.channel.queue_bind(exchange=self.cfg['exchange'], queue=res.queue)
		self.channel.basic_consume(callback=self.on_info_msg, queue=res.queue, no_ack=True)

