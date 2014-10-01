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

# generic test setup

import logging,sys,os
logger = logging.getLogger("dabroker.client.transport.local")

from dabroker.base.transport import BaseTransport
from dabroker.base.transport.local import RPCmessage,Queue
from dabroker.util import format_msg
from dabroker.util.thread import AsyncResult, spawned
from dabroker.client import BrokerClient
from dabroker.client import service as cs
from dabroker.server import BrokerServer

_client_id = 0

class Transport(BaseTransport):
	def __init__(self,callbacks,cfg):
		self.callbacks = callbacks
		self.p = cfg['_LocalQueue']

		self.reply_q = Queue()
		self.q = {} # msgid => AsyncResult for the answer
		self.next_id = 1

		global _client_id
		_client_id += 1
		self.client_id = _client_id

	def connect(self):
		self.p.server().clients[self.client_id] = self
		super(Transport,self).connect()

	def disconnect(self):
		s = self.p.server
		if s:
			s = s()
			if s: 
				s.clients.pop(self.client_id,None)
		super(Transport,self).disconnect()

	@spawned
	def run_recv(self,msg):
		self.callbacks.recv(msg)
		
	def run(self):
		while self.p.server is not None and self.p.server() is not None:
			msg = self.reply_q.get()
			if msg.msgid < 0:
				logger.debug("Client: get msg %s",msg.msgid)
				self.run_recv(msg.msg)
			else:
				r = self.q.pop(msg.msgid,None)
				if r is not None:
					m = msg.msg
					logger.debug("Client: get msg %s",msg.msgid)
					r.set(m)
			
	def send(self,msg):
		m = msg
		msg = RPCmessage(msg,self.reply_q)
		res = AsyncResult()

		msg.msgid = self.next_id
		msg.q = self.reply_q
		self.q[self.next_id] = res
		self.next_id += 1

		logger.debug("Client: send msg %s:\n%s",msg.msgid,format_msg(m))
		self.p.request_q.put(msg)
		res = res.get()
		return res

