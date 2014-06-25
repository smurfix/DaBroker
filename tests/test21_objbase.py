
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
from dabroker.server.service import BrokerServer
from dabroker.server.loader import add as store_add
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj
from dabroker.client.service import BrokerClient

from gevent import spawn,sleep

from tests import test_init,LocalQueue,TestMain

logger = test_init("test.21.objbase")
logger_s = test_init("test.21.objbase.server")

class SearchBrokeredInfo(BrokeredInfo):
	objs = []
	def obj_add(self,obj):
		self.objs.append(obj)
	def obj_find(self,_limit=None,**kw):
		res = []
		for obj in self.objs:
			for k,v in kw.items():
				if getattr(obj,k,None) != v:
					break
			else:
				res.append(obj)
		return res

rootMeta = BrokeredInfo("rootMeta")
rootMeta.add(Field("hello"))
rootMeta.add(Ref("ops"))
store_add(rootMeta,0,1)

opsMeta = SearchBrokeredInfo("opsMeta")
opsMeta.add(Callable("rev"))
opsMeta.add(Field("hell"))
store_add(opsMeta,0,2)

class RootObj(BaseObj):
	_meta = rootMeta
	hello = "Hello!"

class OpsObj(BaseObj):
	_meta = opsMeta
	def __init__(self, h="Oh?"):
		self.hell = h
	def rev(self,s):
		s = [c for c in s]
		s.reverse()
		return "".join(s)
	def __str__(self):
		return "OpsObj:%r:%s"%(self._key,self.hell)
	def __repr__(self):
		return "<%s>"%self

theRootObj = RootObj()
store_add(theRootObj,0,2,99)

theOpsObj = OpsObj("Oh?")
store_add(theOpsObj,0,34)
theRootObj.ops = theOpsObj

for i,n in ((0,"Zero"),(1,"One"),(2,"Two"),(3,"Three")):
	o = OpsObj(n)
	store_add(o,0,10,i)
	opsMeta.obj_add(o)

class TestBrokerServer(BrokerServer):
	def do_root(self,msg):
		logger_s.debug("Get root %r",msg)
		return theRootObj
	do_root.include = True

	def do_update(self,msg):
		if msg == 1:
			theOpsObj.hell = "Yeah!"
			self.send("invalid",(theOpsObj._key,(3,4,5))) # the latter is unknown
		elif msg == 2:
			obj = opsMeta.objs[2]
			attrs = obj._attrs
			obj.hell = "Two2"
			self.updated(obj,attrs)
		else:
			raise RuntimeError(msg)
	
class Broker(TestMain):
	c = q = s = None
	def setup(self):
		self.s = TestBrokerServer()
		self.q = LocalQueue(self.s.recv)
		self.c = BrokerClient(self.q.send)
		self.q.set_client_worker(self.c._recv)
		self.s.sender = self.q.notify
		super(Broker,self).setup()
	def stop(self):
		if self.q is not None:
			self.q.shutdown()
		super(Broker,self).stop()

	@property
	def cid(self):
		return self.q.cq.next_id

	def job(self,i):
		logger.debug("Get the root")
		res = self.c.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		assert res._meta.name == "rootMeta"
		cid=self.cid
		assert res._meta.name == "rootMeta" # again, to check caching
		assert cid==self.cid, (cid,self.cid)
		assert res.ops.rev("test123") == "321tset"
		assert res.ops.hell == "Oh?"
		self.c._send("update",1)
		assert res.ops.hell == "Yeah!"
		cid=self.cid
		assert res.ops.hell == "Yeah!"
		assert cid==self.cid, (cid,self.cid)

		# Now let's search for something
		Op = res.ops._meta

		o1 = Op.get(hell="Two")
		assert o1.hell == "Two", o1
		os = Op.find(hell="Two2")
		assert len(os) == 0, os

		cid=self.cid
		os = Op.find(hell="Two")
		assert len(os) == 1, os
		assert os[0] is o1, (os,o1)
		assert cid==self.cid

		# Now update some stuff.
		self.c._send("update",2)

		os = Op.find(hell="Two")
		assert len(os) == 0, os
		os = Op.find(hell="Two2")
		assert len(os) == 1, os

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
