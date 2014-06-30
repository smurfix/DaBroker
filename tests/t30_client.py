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

from gevent import spawn
from gevent.event import AsyncResult

logger = test_init("test.30.sql")
logger_s = test_init("test.30.sql.server")

class TestClient(BrokerClient):

	def do_trigger(self,msg):
		ar = self.q.a[msg]
		self.q.a[msg] = AsyncResult()
		ar.set(None)
	
	def main(self):

		self.a = [None]
		for i in range(3):
			self.a.append(AsyncResult())
		self.s.sender = self.q.notify
		super(Broker,self).setup()

	def jump(self,i,n):
		if i and n:
			logger.debug("GOTO {} to {}".format(i,n))
		elif n:
			logger.debug("GOTO trigger {}".format(n))
		elif i:
			logger.debug("GOTO {} waits".format(i))

		if n:
			self.c.send("trigger",n)
		if i:
			self.a[i].get()
			logger.debug("GOTO {} runs".format(i))

	def stop(self):
		if self.q is not None:
			self.q.shutdown()
		super(Broker,self).stop()

	def ref(self,p):
		k = p._key
		def res():
			return self.c.get(k)
		return res

	@property
	def cid(self):
		return self.q.cq.next_id

	def job1(self):
		self.jump(1,0)
		logger.debug("Get the root")
		res = self.c.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		P = res.data['Person']
		assert P.name == 'Person',P.name
		r = P.find()
		assert len(r) == 0, r

		# A: create
		p1 = P.new(name="Fred Flintstone")
		p1r = self.ref(p1)
		self.c.commit()

		self.jump(1,2) # goto B

		# D: check
		p1 = p1r()
		assert p1.name == "Freddy Firestone", p1.name

		self.jump(0,2) # goto E

		global done
		done |= 1

	def job2(self):
		logger.debug("Get the root 2")
		res = self.c.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		
		self.jump(2,0)

		# B: check+modify
		p1 = P.get(name="Fred Flintstone")
		p1.name="Freddy Firestone"
		self.c.commit()

		self.jump(2,3) # goto C

		# E: delete
		P.delete(p1)
		self.c.commit()

		self.jump(0,3) # goto F

		global done
		done |= 2
	
	def job3(self):
		self.jump(3,0)

		# C: check
		session = DBSession()
		res = list(session.query(Person))
		assert len(res)==1
		res = res[0]
		assert res.name=="Freddy Firestone",res.name
		del session

		self.jump(3,1) # goto D

		# F: check
		session = DBSession()
		res = list(session.query(Person))
		assert len(res)==0

		global done
		done |= 4

	def main(self):
		j1 = spawn(self.job1)
		j2 = spawn(self.job2)
		j3 = spawn(self.job3)
		self.jump(0,1)
		j1.join()
		j2.join()
		j3.join()

		

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()
assert done==7,done

logger.debug("Exiting")
