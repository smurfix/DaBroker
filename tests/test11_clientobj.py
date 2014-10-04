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

# This test verifies that instantiating specific objects on the client side
# works.

from dabroker import patch; patch()
from dabroker.util.thread import spawned
from dabroker.util import exported
from dabroker.client.codec import baseclass_for, ClientBaseObj

from gevent import spawn

from dabroker.util.tests import test_init,TestMain,TestRoot,TestClient,TestServer

logger = test_init("test.11.lclientobj")

counter = 0

# client's root object
@baseclass_for("_s","root","meta")
class my_root(ClientBaseObj):
	def test_me(self):
		global counter
		counter += 100
		logger.debug("Local call")

# Server's root object
class Test11_root(TestRoot):
	@exported
	def callme(self,msg):
		return msg*msg

class Test11_client(TestClient):
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
		self.root.test_me()
		res = self.root.callme(i)
		logger.debug("Sent %r, got %r",i,res)
		counter += res

class Test11_server(TestServer):
	@property
	def root(self):
		return Test11_root()

class Tester(TestMain):
	client_factory = Test11_client
	server_factory = Test11_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

assert counter == 300+1+4+9,counter

logger.debug("Exiting")
