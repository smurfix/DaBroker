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

# This test checks the test environment's local queue implementation
# and, incidentally, basic object transfer and messaging.

from dabroker import patch; patch()
from dabroker.util.thread import spawned

from gevent import spawn

from dabroker.util.tests import test_init,TestMain,TestRoot,TestClient,TestServer
from dabroker.util import exported

logger = test_init("test.08.localmsg")

counter = 0

# Server's root object
class Test08_root(TestRoot):
	@exported
	def callme(self,msg):
		return msg*msg

class Test08_client(TestClient):
	def main(self):
		logger.debug("Client started")
		jobs = []
		for i in range(3):
			jobs.append(self.mult(i+1))
		for j in jobs:
			j.join()
		logger.debug("Client stopped")

	@spawned
	def mult(self,i):
		global counter
		logger.debug("Send %r",i)
		res = self.root.callme(i)
		logger.debug("Sent %r, got %r",i,res)
		counter += res

class Test08_server(TestServer):
	@property
	def root(self):
		return Test08_root()

class Tester(TestMain):
	client_factory = Test08_client
	server_factory = Test08_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

assert counter == 1+4+9,counter

logger.debug("Exiting")
