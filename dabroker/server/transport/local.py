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

from gevent import spawn,sleep,GreenletExit
from weakref import ref, WeakValueDictionary

import logging,sys,os
logger = logging.getLogger("dabroker.server.transport.local")

def test_init(who):
	if os.environ.get("TRACE","0") == '1':
		level = logging.DEBUG
	else:
		level = logging.WARN

	logger = logging.getLogger(who)
	logging.basicConfig(stream=sys.stderr,level=level)

	return logger

# reduce cache sizes and timers

from dabroker.base import BaseObj,BrokeredInfo, Field,Ref,Callable
from dabroker.base.config import default_config
from dabroker.base.transport import BaseTransport
from dabroker.base.transport.local import RPCmessage,LocalQueue
from dabroker.util import format_msg
from dabroker.util.thread import Main, AsyncResult, spawned
from dabroker.client import BrokerClient
from dabroker.client import service as cs
from dabroker.server import BrokerServer

class Transport(BaseTransport):
	"""Server side of the LocalQueue transport"""
	def __init__(self,callbacks,cfg):
		logger.debug("Server: setting up")
		self.callbacks = callbacks
		self.p = LocalQueue(cfg)
		self.p.server = ref(self) # for clients to find me
		self.clients = WeakValueDictionary() # clients add themselves here
		self.next_id = -1

	@spawned
	def _process(self,msg):
		m = self.callbacks.recv(msg.msg)
		msg.reply(m)

	def run(self):
		logger.debug("Server: wait for messages")
		while self.p.request_q is not None:
			msg = self.p.request_q.get()
			logger.debug("Server: received %r",msg)
			self._process(msg)

	def send(self,msg):
		m = msg
		msg = RPCmessage(msg)
		msg.msgid = self.next_id
		self.next_id -= 1
		logger.debug("Server: send msg %s:\n%s",msg.msgid,format_msg(m))
		for c in self.clients.values():
			c.reply_q.put(msg)

