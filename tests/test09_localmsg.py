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

# This test runs the test environment's notification system.

from dabroker import patch; patch()

from gevent import spawn,sleep

from tests import test_init,LocalQueue,TestMain,TestRoot,TestClient,TestServer

logger = test_init("test.09.localmsg")

counter = 0

# Server's root object
class Test09_root(TestRoot):
	def __init__(self,server):
		self._server = server
		super(Test09_root,self).__init__()
	def callme(self,msg):
		self._server.send("more",msg*10)
		return msg*msg

class Test09_client(TestClient):
	def main(self):
		jobs = []
		for i in range(3):
			jobs.append(spawn(self.mult,i+1))
		for j in jobs:
			j.join()

	def mult(self,i):
		global counter
		res = self.root.callme(i)
		logger.debug("Sent %r, got %r",i,res)
		counter += res
	
	def make_client(self):
		return Test09_clientbroker(self)

	def do_more(self,msg):
		global counter
		logger.debug("more %s",msg)
		counter += msg

class Test09_server(TestServer):
	@property
	def root(self):
		return Test09_root(self)
		
class Tester(TestMain):
	client_factory = Test09_client
	server_factory = Test09_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

assert counter == 1+4+9+10+20+30,counter

logger.debug("Exiting")
