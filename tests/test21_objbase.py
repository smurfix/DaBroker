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
from dabroker.server import export_class
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj,BaseRef
from dabroker.client.service import BrokerClient
from dabroker.util import cached_property,exported,exported_classmethod
from dabroker.util.thread import Event

from dabroker.util.tests import test_init,TestMain,TestClient,TestServer
from gevent.event import AsyncResult

logger = test_init("test.21.objbase")
logger_s = test_init("test.21.objbase.server")

class Test21_server(TestServer):
	@cached_property
	def root(self):
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Ref("ops"))
		self.add_static(rootMeta,0,1)

		someMeta = BrokeredInfo("rootMeta")
		someMeta.add(Field("hello"))
		self.add_static(someMeta,0,99)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"

		class SomeObj(BaseObj):
			_meta = someMeta
			foo="bar"

		class OpsObj(BaseObj):
			objs = []
			_dab_cached=True

			def __init__(self, h="Oh?"):
				self.hell = h

			@classmethod
			def obj_add(self,obj):
				self.objs.append(obj)

			@exported_classmethod
			def _dab_search(cls,_limit=None,**kw):
				res = []
				for obj in cls.objs:
					for k,v in kw.items():
						if getattr(obj,k,None) != v:
							break
					else:
						res.append(obj)
				return res

			@exported
			def trigger(self,arg):
				sig = SomeObj()
				t.server.add_static(sig,12,99)
				t.server.send_signal(self,sig, arg=arg)

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
		exp = self._ops_meta = export_class(OpsObj,self.loader, attrs="+")
		exp.add(Field('hell'))
		exp.calls['revc'].cached=True

		theOpsObj = OpsObj("Oh?")
		self.add_static(theOpsObj,0,34)
		root.ops = theOpsObj

		for i,n in ((0,"Zero"),(1,"One"),(2,"Two"),(3,"Three")):
			o = OpsObj(n)
			self.add_static(o,0,10,i)
			OpsObj.obj_add(o)
		
		return root

	def do_trigger(self,msg):
		if msg == 1:
			self.root.ops.hell = "Yeah!"
			self.send("invalid",self.root.ops._key,BaseRef(key=(3,4,5)), _include=None) # the latter is unknown
			self.send("go_on")
		elif msg == 2:
			obj = self.root.ops.__class__.objs[2]
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
		return self.transport.last_msgid

	def sigrecv(self,obj,sig, arg=None,**k):
		self.sig_arg = arg
		self.sig_obj = obj
		self.sig_sig = sig
		self.got_it.set(None)

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

			self.got_it = AsyncResult()
			root.ops._key.connect(self.sigrecv)
			root.ops.trigger("foobar")
			self.got_it.get(timeout=1)
			assert self.sig_arg == "foobar", self.sig_arg
			assert self.sig_obj is root.ops._key, self.sig_obj
			assert self.sig_sig.key == ('_s',12,99), self.sig_sig

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
			assert cid==self.cid,(cid,self.cid)

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
