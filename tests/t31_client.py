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
from dabroker.client.service import BrokerClient
from dabroker.util.thread import AsyncResult
from time import time

from gevent import spawn

from tests import test_init

logger = test_init("test.31.amqp.speed")

class TestClient(BrokerClient):
	def main(self):
		done = 0
		res = self.root
		t=time()
		# Make sure that the resolution is OK
		assert t != time()
		assert t != time()

		pypy = "pypy" in sys.version.lower()
		warmup = 8 if pypy else 1
		while time()-t <= warmup:
			res.pling("This",root=res)
		t=time()
		while time()-t <= 1:
			res.pling("This",root=res)
			done += 1

		assert done > 10 # assume that somethign is wrong, otherwise
		logger.warning("%d iterations (%s and %s) with %s",done, self.cfg['codec'],self.cfg['transport'], sys.version)
