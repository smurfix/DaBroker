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

# generic test setup

from pprint import pformat
from gevent.event import AsyncResult
from gevent import spawn,sleep,GreenletExit
from weakref import ref, WeakValueDictionary

import logging,sys,os
logger = logging.getLogger("tests")

def test_init(who):
	if os.environ.get("TRACE","0") == '1':
		level = logging.DEBUG
	else:
		level = logging.WARN

	logger = logging.getLogger(who)
	logging.basicConfig(stream=sys.stderr,level=level)

	return logger

# reduce cache sizes and timers

from dabroker.base import BaseObj,BrokeredInfo, Field,Ref,Callable
from dabroker.base.transport import BaseTransport
from dabroker.util.thread import Main
from dabroker.client import BrokerClient
from dabroker.client import service as cs
from dabroker.server import BrokerServer

cs.RETR_TIMEOUT=1 # except that we want 1000 when debugging
cs.CACHE_SIZE=5

# prettyprint

def _p_filter(m,mids):
	if isinstance(m,dict):
		if m.get('_oi',0) not in mids:
			del m['_oi']
		for v in m.values():
			_p_filter(v,mids)
	elif isinstance(m,(tuple,list)):
		for v in m:
			_p_filter(v,mids)
def _p_find(m,mids):
	if isinstance(m,dict):
		mids.add(m.get('_or',0))
		for v in m.values():
			_p_find(v,mids)
	elif isinstance(m,(tuple,list)):
		for v in m:
			_p_find(v,mids)
def pf(m):
	mids = set()
	_p_find(m,mids)
	_p_filter(m,mids)
	return pformat(m)
# local queue implementation

try:
	from queue import Queue
except ImportError:
	from Queue import Queue
from traceback import format_exc
from bson import BSON

test_cfg = dict(userid='test', password='test', virtual_host='test', codec="null") 

test_cfg_s = dict(transport="tests.ServerQueue")
test_cfg_c = dict(transport="tests.ClientQueue")

class RPCmessage(object):
	msgid = None
	def __init__(self,msg,q=None):
		self.msg = msg
		self.q = q

	def reply(self,msg):
		logger.debug("Reply to %s:\n%s", self.msgid,pf(msg))
		msg = type(self)(msg,None)
		msg.msgid = self.msgid
		self.q.put(msg)

class ServerQueue(BaseTransport):
	"""Server side of the LocalQueue transport"""
	def __init__(self,callbacks,cfg):
		self.callbacks = callbacks
		self.p = cfg['_p'] # the LocalQueue instance
		self.p.server = ref(self) # for clients to find me
		self.clients = WeakValueDictionary() # clients add themselves here
		self.next_id = -1

	def _process(self,msg):
		m = self.callbacks.recv(msg.msg)
		msg.reply(m)

	def run(self):
		logger.debug("Server: wait for messages")
		while True:
			try:
				msg = self.p.request_q.get()
			except GreenletExit:
				return
			else:
				spawn(self._process,msg)

	def send(self,msg):
		m = msg
		msg = RPCmessage(msg)
		msg.msgid = self.next_id
		self.next_id -= 1
		logger.debug("Server: send msg %s:\n%s",msg.msgid,pf(m))
		for c in self.clients.values():
			c.reply_q.put(msg)

global client_id
client_id = 0

class ClientQueue(BaseTransport):
	def __init__(self,callbacks,cfg):
		self.callbacks = callbacks
		self.p = cfg['_p']

		self.reply_q = Queue()
		self.q = {} # msgid => AsyncResult for the answer
		self.next_id = 1

		global client_id
		client_id += 1
		self.client_id = client_id

	def connect(self):
		self.p.server().clients[self.client_id] = self
		super(ClientQueue,self).connect()

	def disconnect(self):
		s = self.p.server
		if s:
			s = s()
			if s: 
				s.clients.pop(self.client_id,None)
		super(ClientQueue,self).disconnect()

	def run(self):
		try:
			while self.p.server is not None and self.p.server() is not None:
				msg = self.reply_q.get()
				if msg.msgid < 0:
					logger.debug("Client: get msg %s",msg.msgid)
					spawn(self.callbacks.recv,msg.msg)
				else:
					r = self.q.pop(msg.msgid,None)
					if r is not None:
						m = msg.msg
						logger.debug("Client: get msg %s",msg.msgid)
						r.set(m)
		except GreenletExit:
			pass
			
	def send(self,msg):
		m = msg
		msg = RPCmessage(msg,self.reply_q)
		res = AsyncResult()

		msg.msgid = self.next_id
		msg.q = self.reply_q
		self.q[self.next_id] = res
		self.next_id += 1

		logger.debug("Client: send msg %s:\n%s",msg.msgid,pf(m))
		self.p.request_q.put(msg)
		res = res.get()
		return res

class LocalQueue(object):
	"""\
		Queue manager for transferring data between a server and a couple
		of receivers within the same process.
	
		Passing this object to the client's and server's transport is achieved by 
		insertion into the confg dict, instead of copying it.

		You need to instantiate the server first.
		"""
	def __init__(self, cfg):
		self.request_q = Queue()
		self.server = None
		cfg['_p'] = self

def killer(x,t):
	sleep(t)
	x.killer = None
	x.stop()

rootMeta = BrokeredInfo("rootMeta")
rootMeta.add(Field("hello"))
rootMeta.add(Field("data"))
rootMeta.add(Ref("more"))
rootMeta.add(Callable("callme"))

class TestRoot(BaseObj):
	_meta = rootMeta
	hello = "Hello!"
	data = {}
	more = None
	def callme(self,msg):
		logger.debug("Called")
		return msg

class TestServer(BrokerServer):
	root_factory = TestRoot

	@property
	def root(self):
		return self.root_factory()
	def start(self):
		if self.root is None:
			self.root = self.make_root()
		return super(TestServer,self).start()

class TestClient(BrokerClient):
	def main(self):
		raise NotImplementedError("You need to override {}.main!".format(self.__class__.__name__))

class TestMain(Main):
	server_factory = TestServer
	client_factory = TestClient
	client = None
	server = None

	def __init__(self,cfg={}):
		in_cfg = cfg
		self.cfg = test_cfg.copy()
		self.cfg.update(test_cfg_s)
		self.cfg.update(in_cfg)
		self.q = LocalQueue(self.cfg)

		super(TestMain,self).__init__()

		self.client_cfg = test_cfg.copy()
		self.client_cfg.update(test_cfg_c)
		self.client_cfg.update(in_cfg)
		self.client_cfg['_p'] = self.cfg['_p']

	def setup(self):
		assert self.server is None
		assert self.client is None

		super(TestMain,self).setup()
		
		self.killer = spawn(killer,self,5)
		self.server = self.server_factory(cfg=self.cfg)
		self.server.start()

	def main(self):
		assert self.client is None
		try:
			self.client = self.client_factory(cfg=self.client_cfg)
			self.client.start()
			self.client.main()
		finally:
			c,self.client = self.client,None
			if c is not None:
				c.stop()

	def cleanup(self):
		s,self.server = self.server,None
		if s is not None:
			s.stop()

		super(TestMain,self).cleanup()
		k,self.killer = self.killer,None
		if k is not None:
			k.kill()

class TestBasicMain(Main):
	def setup(self):
		super(TestBasicMain,self).setup()
		self.killer = spawn(killer,self,5)
	def cleanup(self):
		super(TestBasicMain,self).cleanup()
		if self.killer is not None:
			self.killer.kill()

