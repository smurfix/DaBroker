
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including an optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

# This test runs the test environment's local queue implementation.

from dabroker import patch; patch()
from dabroker.util.thread import Main
from dabroker.server.service import BrokerServer
from dabroker.server.loader import add as store_add
from dabroker.base import BrokeredInfo, Field, BaseObj
from dabroker.client.service import BrokerClient

from gevent import spawn,sleep

from tests import test_init,LocalQueue

logger = test_init("test.21.objbase")
logger_s = test_init("test.21.objbase.server")

rootMeta = BrokeredInfo("rootMeta")
rootMeta.add(Field("hello"))
store_add(rootMeta,0,1)

class RootObj(BaseObj):
	_meta = rootMeta
	hello = "Hello!"

theRootObj = RootObj()
store_add(theRootObj,0,2,99)

class TestBrokerServer(BrokerServer):
	def do_root(self,msg):
		logger_s.debug("Get root %r",msg)
		return theRootObj
	do_root.include = True

class Broker(Main):
	c = q = s = None
	def setup(self):
		self.s = TestBrokerServer()
		self.q = LocalQueue(self.s.recv)
		self.c = BrokerClient(self.q.send)
		super(Broker,self).setup()
	def stop(self):
		if self.q is not None:
			self.q.shutdown()
		super(Broker,self).stop()

	def job(self,i):
		logger.debug("Get the root")
		res = self.c.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		assert res._meta.name == "rootMeta"
		assert res._meta.name == "rootMeta" # again, to check caching

	def main(self):
		jobs = []
		for i in range(1): # 3
			jobs.append(spawn(self.job,i+1))
		for j in jobs:
			j.join()

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()

logger.debug("Exiting")
