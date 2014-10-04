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
from dabroker.client.service import BrokerClient
from dabroker.util.thread import AsyncResult, Thread

from gevent import spawn

from dabroker.util.tests import test_init,TestMain,test_cfg, cfg_merge

logger = test_init("test.31.speed")

cfg = {'transport':'amqp', 'codec':os.environ.get('DAB_CODEC','marshal')}

class ServerThread(Thread):
	def code(self, cfg,ready):
		from tests.t31_server import TestServer
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
	def code(self,cfg):
		from tests.t31_client import TestClient
		logger.debug("Starting the client")
		cfg = cfg.copy()
		tc = TestClient(cfg)
		tc.start()
		try:
			logger.debug("client run")
			tc.main()
		finally:
			tc.stop()

logger.debug("Starting the server")
e = AsyncResult()
s = ServerThread(cfg=cfg_merge(test_cfg,cfg), ready=e).start()
ts = e.get(timeout=15)
if ts is None:
	raise RuntimeError("Server did not run")

c = ClientThread(cfg=cfg_merge(test_cfg,cfg)).start()
c.join(timeout=15)
if not c.ready:
	c.kill()
	ts.stop()
	raise RuntimeError("Client did not run")
ts.stop()

