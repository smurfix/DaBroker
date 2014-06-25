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

from gevent import spawn,sleep

from tests import test_init,LocalQueue,TestMain

logger = test_init("test.09.localmsg")

counter = 0

def quadrat(msg):
	msg = msg['m']
	return {'r':msg*msg}

class Broker(TestMain):
	def setup(self):
		self.q = LocalQueue(quadrat)
		super(Broker,self).setup()
	def stop(self):
		self.q.shutdown()
		super(Broker,self).stop()

	def mult(self,i):
		global counter
		res = self.q.send({'m':i})
		logger.debug("Sent %r, got %r",i,res)
		counter += res['r']
		
	def main(self):
		jobs = []
		for i in range(3):
			jobs.append(spawn(self.mult,i+1))
		for j in jobs:
			j.join()

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()

assert counter == 1+4+9,counter

logger.debug("Exiting")
