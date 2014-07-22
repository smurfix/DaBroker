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

# Test the dict cache

from gevent import spawn,sleep
from dabroker.util.tests import test_init,TestBasicMain
logger = test_init("test.06.cachedict")

from dabroker.client.service import CacheDict

freed = 0
KEEP=5
d = CacheDict()

class CacheItem(object):
	def __init__(self,x):
		self.x = x
	def __del__(self):
		global freed
		if freed is not None:
			freed |= 1<<self.x
		
class Tester(TestBasicMain):
	def main(self):
		d.lru_size=KEEP*2
		d.heap_min=KEEP*2

		# For this test to work, these are minimum values
		d.heap_max=KEEP*3+KEEP+2
		d.lru_size=KEEP*1+KEEP+2
		logger.debug("Startup")
		for i in range(KEEP*30):
			d[i]=CacheItem(i)
			d[i%KEEP] # access an item that should still be in the cache

			# Here I access an item quite often, then not at all
			if KEEP < i < KEEP*5:
				d[KEEP]
			if i == KEEP*5:
				del d[KEEP]
		logger.debug("Shutdown %d",len(d))

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

for i in range(KEEP):
	assert not((1<<i)& freed),(i,freed)
assert (1<<KEEP) & freed,(i,freed)
assert (1<<(KEEP+1)) & freed,(i,freed)

logger.debug("Exiting %x",freed)
