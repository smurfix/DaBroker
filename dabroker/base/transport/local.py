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

from dabroker.util import format_msg

import logging,os
logger = logging.getLogger("dabroker.base.transport.local")

try:
	from queue import Queue
except ImportError:
	from Queue import Queue

class RPCmessage(object):
	msgid = None
	def __init__(self,msg,q=None):
		self.msg = msg
		self.q = q

	def reply(self,msg):
		logger.debug("Reply to %s:\n%s", self.msgid,format_msg(msg))
		msg = type(self)(msg,None)
		msg.msgid = self.msgid
		self.q.put(msg)

class LocalQueue(object):
	"""\
		Queue manager for transferring data between a server and a couple
		of receivers within the same process.
	
		You need to instantiate the server first.
		"""
	def __init__(self, cfg):
		assert '_LocalQueue' not in cfg

		self.request_q = Queue()
		self.server = None
		cfg['_LocalQueue'] = self

