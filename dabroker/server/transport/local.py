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

from weakref import ref, WeakValueDictionary

from dabroker.base.transport import BaseTransport
from dabroker.base.transport.local import RPCmessage,LocalQueue
from dabroker.util import format_msg
from dabroker.util.thread import spawned

import logging,sys,os
logger = logging.getLogger("dabroker.server.transport.local")

class Transport(BaseTransport):
	"""Server side of the LocalQueue transport"""
	def __init__(self,callbacks,cfg):
		#logger.debug("Server: setting up")
		self.callbacks = callbacks
		self.p = LocalQueue(cfg)
		self.p.server = ref(self) # for clients to find me
		self.clients = WeakValueDictionary() # clients add themselves here

	@spawned
	def _process(self,msg):
		m = self.callbacks.recv(msg.msg)
		msg.reply(m)

	def run(self):
		logger.debug("Server: wait for messages")
		while self.p.request_q is not None:
			msg = self.p.request_q.get()
			#logger.debug("Server: received %r",msg)
			self._process(msg)

	def send(self,msg):
		m = msg
		msg = RPCmessage(msg)
		self.last_msgid -= 1
		msg.msgid = self.last_msgid

		logger.debug("Server: send msg %s:\n%s",msg.msgid,format_msg(m))
		for c in self.clients.values():
			c.reply_q.put(msg)

