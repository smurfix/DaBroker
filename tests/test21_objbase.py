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

# This test runs the test environment's local queue implementation.

from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
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

class TestBrokerServer(BrokerServer):
	def __init__(self,sender=None):
		super(TestBrokerServer,self).__init__(sender=sender)

		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Ref("ops"))
		self.loader.static.add(rootMeta,0,1)

		opsMeta = SearchBrokeredInfo("opsMeta")
		opsMeta.add(Callable("rev"))
		opsMeta.add(Field("hell"))
		self.loader.static.add(opsMeta,0,2)

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

		self.theRootObj = RootObj()
		self.loader.static.add(self.theRootObj,0,2,99)

		theOpsObj = OpsObj("Oh?")
		self.loader.static.add(theOpsObj,0,34)
		self.theRootObj.ops = theOpsObj

		for i,n in ((0,"Zero"),(1,"One"),(2,"Two"),(3,"Three")):
			o = OpsObj(n)
			self.loader.static.add(o,0,10,i)
			opsMeta.obj_add(o)

		self.opsMeta = opsMeta

	def do_root(self,msg):
		logger_s.debug("Get root %r",msg)
		return self.theRootObj
	do_root.include = True

	def do_trigger(self,msg):
		if msg == 1:
			self.theRootObj.ops.hell = "Yeah!"
			self.send("invalid",(self.theRootObj.ops._key,(3,4,5))) # the latter is unknown
		elif msg == 2:
			obj = self.opsMeta.objs[2]
			ov = obj.hell
			obj.hell = nv = "Two2"
			attrs = {'hell': (ov,nv)}
			self.send_updated(obj,attrs)
		else:
			raise RuntimeError(msg)
	
done=0

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

	def job(self):
		logger.debug("Get the root")
		res = self.c.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		assert res._meta.name == "rootMeta",(res,res._meta,res._meta.name)
		cid=self.cid
		assert res._meta.name == "rootMeta" # again, to check caching
		assert cid==self.cid, (cid,self.cid)
		assert res.ops.rev("test123") == "321tset"
		assert res.ops.hell == "Oh?"
		self.c.send("trigger",1)
		assert res.ops.hell == "Yeah!"
		cid=self.cid
		assert res.ops.hell == "Yeah!"
		assert cid==self.cid, (cid,self.cid)

		# Now let's search for something
		Op = res.ops._meta
		assert hasattr(Op,"calls")
		assert not hasattr(res,"calls"),(res,res.calls)
		assert not hasattr(res.ops,"calls")

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
		self.c.send("trigger",2)

		os = Op.find(hell="Two")
		assert len(os) == 0, os
		os = Op.find(hell="Two2")
		assert len(os) == 1, os

		global done
		done = 1

	def main(self):
		j = spawn(self.job)
		j.join()

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()
assert done==1, done

logger.debug("Exiting")
