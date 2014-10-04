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
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj,BaseRef
from dabroker.client.service import BrokerClient
from dabroker.util import cached_property,exported
from dabroker.util.thread import Event

from dabroker.util.tests import test_init,TestMain,TestClient,TestServer

logger = test_init("test.21.objbase")
logger_s = test_init("test.21.objbase.server")

class SearchBrokeredInfo(BrokeredInfo):
	objs = []
	_dab_cached = True

	def obj_add(self,obj):
		self.objs.append(obj)

	@exported
	def _dab_search(self,_limit=None,**kw):
		res = []
		for obj in self.objs:
			for k,v in kw.items():
				if getattr(obj,k,None) != v:
					break
			else:
				res.append(obj)
		return res
	_dab_search.include=True

class Test21_server(TestServer):
	@cached_property
	def root(self):
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Ref("ops"))
		self.add_static(rootMeta,0,1)

		opsMeta = SearchBrokeredInfo("opsMeta")
		opsMeta.add(Callable("rev"))
		opsMeta.add(Callable("revc", cached=True))
		opsMeta.add(Field("hell"))
		self.add_static(opsMeta,0,2)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"

		class OpsObj(BaseObj):
			_meta = opsMeta
			def __init__(self, h="Oh?"):
				self.hell = h
			@exported
			def rev(self,s):
				s = [c for c in s]
				s.reverse()
				return "".join(s)
			revc = rev
			def __str__(self):
				return "OpsObj:%r:%s"%(self._key,self.hell)
			def __repr__(self):
				return "<%s>"%self

		root = RootObj()
		self.add_static(root,0,2,21)

		theOpsObj = OpsObj("Oh?")
		self.add_static(theOpsObj,0,34)
		root.ops = theOpsObj

		for i,n in ((0,"Zero"),(1,"One"),(2,"Two"),(3,"Three")):
			o = OpsObj(n)
			self.add_static(o,0,10,i)
			opsMeta.obj_add(o)
		
		self._ops_meta = opsMeta
		return root

	def do_trigger(self,msg):
		if msg == 1:
			self.root.ops.hell = "Yeah!"
			self.send("invalid",self.root.ops._key,BaseRef(key=(3,4,5)), _include=None) # the latter is unknown
			self.send("go_on")
		elif msg == 2:
			obj = self._ops_meta.objs[2]
			ov = obj.hell
			obj.hell = nv = "Two2"
			attrs = {'hell': (ov,nv)}
			self.send_updated(obj,attrs)
			self.send("go_on")
		else:
			raise RuntimeError(msg)
	
done=0

class Test21_client(TestClient):
	@property
	def cid(self):
		return self.transport.next_id

	def do_go_on(self):
		self.go_on.set()

	def main(self):
		with self.env:
			self.go_on = Event()
			logger.debug("Get the root")
			root = self.root
			logger.debug("recv %r",root)
			assert root.hello == "Hello!"
			assert root._meta.name == "rootMeta",(root,root._meta,root._meta.name)
			cid=self.cid
			assert root._meta.name == "rootMeta" # again, to check caching
			assert cid==self.cid, (cid,self.cid)

			assert root.ops.rev("test123") == "321tset"
			assert cid!=self.cid
			cid=self.cid

			assert root.ops.revc("test123") == "321tset"
			assert cid!=self.cid
			cid=self.cid

			assert root.ops.rev("test123") == "321tset"
			assert cid!=self.cid
			cid=self.cid

			assert root.ops.revc("test123") == "321tset"
			assert cid==self.cid, (cid,self.cid)

			res = root.ops.revc("test1234")
			assert res == "4321tset", res
			assert cid!=self.cid

			assert root.ops.hell == "Oh?"
			self.send("trigger",1)
			self.go_on.wait()
			self.go_on.clear()

			assert root.ops.hell == "Yeah!",root.ops.hell
			cid=self.cid
			assert root.ops.hell == "Yeah!"
			assert cid==self.cid, (cid,self.cid)

			# Now let's search for something
			Op = root.ops._meta
			assert hasattr(Op,"calls")
			assert not hasattr(root,"calls"),(root,root.calls)
			assert not hasattr(root.ops,"calls")

			o1 = Op.get(hell="Two")
			assert o1.hell == "Two", (o1,o1.hell)
			os = list(Op.find(hell="Two2"))
			assert len(os) == 0, os

			# 'get' will use limit=2
			cid=self.cid
			os = list(Op.find(hell="Two"))
			assert len(os) == 1, os
			assert os[0] is o1, (os,o1)
			assert cid==self.cid

			# Now update some stuff.
			self.send("trigger",2)
			self.go_on.wait()

			os = list(Op.find(hell="Two"))
			assert len(os) == 0, os
			os = list(Op.find(hell="Two2"))
			assert len(os) == 1, os

			global done
			done = 1

class Tester(TestMain):
	client_factory = Test21_client
	server_factory = Test21_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

assert done==1, done

logger.debug("Exiting")
