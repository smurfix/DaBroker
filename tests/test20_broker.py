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

# This test runs the test environment's local queue implementation.

from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
from dabroker.client.service import BrokerClient, ServerError

from gevent import spawn,sleep

from tests import test_init,LocalQueue,TestMain

logger = test_init("test.20.broker")

class Broker(TestMain):
	def setup(self):
		self.s = BrokerServer()
		self.q = LocalQueue(self.s.recv)
		self.c = BrokerClient(self.q.send)
		super(Broker,self).setup()
	def stop(self):
		self.q.shutdown()
		super(Broker,self).stop()

	def job(self,i):
		logger.debug("Sending an uninteresting message")
		msg = {'message':'not interesting'}
		msg['self referential'] = msg
		res = self.c.send("echo",msg)
		logger.debug("recv %r",res)
		assert res['message'] == "not interesting"
		assert res['self referential'] is res
		assert res is not msg
		assert len(res) == 2
		res = self.c.send("echo",msg)
		try:
			res = self.c.send("unknown",msg)
		except ServerError as e:
			assert "UnknownCommandError" in str(e), str(e)
		else:
			assert False,"No error sent"
			
		
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
