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

# This test mangles SQL, courtesy of sqlalchemy.

import os
import sys
from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
from dabroker.server.loader.sqlalchemy import SQLLoader
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj
from dabroker.client.service import BrokerClient
from dabroker.util.thread import AsyncResult, Thread

from gevent import spawn

from tests import test_init,LocalQueue,TestMain,test_cfg_s,test_cfg_c, cfg_merge

logger = test_init("test.30.amqp")

cfg = {'transport':'amqp', 'codec':'marshal'}

class ServerThread(Thread):
	def run(self):
		cfg=self.k['cfg']
		ready=self.k['ready']
		from tests.t30_server import TestServer
		logger.debug("Starting the server")
		try:
			ts = TestServer(cfg)
			ts.start()
			logger.debug("server running")
			if ready is not None:
				ready.set(ts)
		except Exception as e:
			if ready is not None:
				logger.exception("Server startup")
				ready.set_exception(e)
			else:
				raise
	
class ClientThread(Thread):
	def run(self):
		cfg=self.k['cfg']
		from tests.t30_client import TestClient
		logger.debug("Starting the client")
		cfg = cfg.copy()
		cfg["host"] = "127.0.0.1"
		tc = TestClient(cfg)
		tc.start()
		try:
			logger.debug("client run")
			tc.main()
		finally:
			tc.stop()

logger.debug("Starting the server")
e = AsyncResult()
s = ServerThread(cfg=cfg_merge(test_cfg_s,cfg), ready=e).start()
ts = e.get(timeout=5)
if ts is None:
	raise RuntimeError("Server did not run")

c = ClientThread(cfg=cfg_merge(test_cfg_c,cfg)).start()
c.join(timeout=5)
if not c.ready:
	c.kill()
	ts.stop()
	raise RuntimeError("Client did not run")
ts.stop()

