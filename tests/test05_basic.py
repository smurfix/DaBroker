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

# This is a basic test which shows how to fork and kill threads.

from dabroker import patch; patch()

from gevent import spawn,sleep

from dabroker.util.tests import test_init,TestBasicMain

logger = test_init("test.05.basic")

counter = 0

class Tester(TestBasicMain):
	def main(self):
		global counter
		logger.debug("Startup")
		counter += 1
		sleep(0.5)
		counter += 2
		logger.error("did not kill me")

def killme():
	global counter
	logger.debug("started killer task")
	counter += 4
	sleep(0.2)
	counter += 8
	logger.debug("Terminating")
	t.stop()
waiter = spawn(killme)

t = Tester()
t.register_stop(waiter.kill) # not necessary here
t.register_stop(logger.debug,"shutting down")
t.run()
logger.debug("Exiting")

assert counter == 1+4+8,counter

