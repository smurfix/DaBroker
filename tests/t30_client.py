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

from gevent import spawn

from dabroker.util.tests import test_init

logger = test_init("test.30.amqp.client")

done = 0

class TestClient(BrokerClient):

	def do_trigger(self,msg):
		logger.debug("GOTO {} starts".format(msg))
		ar = self.a[msg]
		ar.set(None)
	
	def jump(self,i,n):
		"""task-switch between jobs."""
		if i and n:
			logger.debug("GOTO {} to {}".format(i,n))
		elif n:
			logger.debug("GOTO trigger {}".format(n))
		elif i:
			logger.debug("GOTO {} waits".format(i))

		if n:
			self.send("trigger",n)
		if i:
			self.a[i].get()
			self.a[i] = AsyncResult()
			logger.debug("GOTO {} runs".format(i))

	def ref(self,p):
		k = p._key
		def res():
			return self.get(k)
		return res

	def job1(self):
		logger.debug("Get the root A")
		res = self.root

		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		P = res.data['Person']

		self.jump(1,0)

		assert P.name == 'Person',P.name
		r = P.find()
		assert len(r) == 0, r

		# A: create
		p1 = P.new(name="Fred Flintstone")
		p1r = self.ref(p1)
		self.commit()

		self.jump(1,2) # goto B

		# D: check
		p1 = p1r()
		assert p1.name == "Freddy Firestone", p1.name

		self.jump(0,2) # goto E

		global done
		done |= 1

	def job2(self):
		logger.debug("Get the root B")
		res = self.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		
		self.jump(2,0)

		# B: check+modify
		p1 = P.get(name="Fred Flintstone")
		p1.name="Freddy Firestone"
		self.commit()

		self.jump(2,3) # goto C

		# E: delete
		P.delete(p1)
		self.commit()

		self.jump(0,3) # goto F

		global done
		done |= 2
	
	def job3(self):
		logger.debug("Get the root C")
		res = self.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		
		self.jump(3,0)

		self.send("check","Freddy Firestone")

		self.jump(3,1) # goto D

		# F: check
		res = self.send("list_me",None)
		assert len(res)==0

		global done
		done |= 4

	def main(self):
		self.a = [None]
		for i in range(3):
			self.a.append(AsyncResult())

		j1 = spawn(self.job1)
		j2 = spawn(self.job2)
		j3 = spawn(self.job3)
		self.jump(0,1)
		j1.join()
		j2.join()
		j3.join()

		global done
		assert done==1+2+4, done
		logger.debug("Success!")
