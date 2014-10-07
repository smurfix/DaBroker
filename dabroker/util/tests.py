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
from dabroker.base.config import default_config
from dabroker.base.transport import BaseTransport
from dabroker.util.thread import Main, AsyncResult, spawned
from dabroker.client.service import BrokerClient
from dabroker.client import service as cs
from dabroker.server.service import BrokerServer

cs.RETR_TIMEOUT=1 # except that we want 1000 when debugging
cs.CACHE_SIZE=5

# local queue implementation

def cfg_merge(*cf, **kw):
	res = {}
	for d in cf:
		res.update(d)
	res.update(kw)
	return res

try:
	from queue import Queue
except ImportError:
	from Queue import Queue
from traceback import format_exc

test_cfg = dict(
                amqp_user='test', amqp_password='test', amqp_host=os.environ.get('AMQP_HOST','127.0.0.1'),
				amqp_virtual_host='test',
                codec=os.environ.get("DAB_CODEC","null"),
)

test_cfg_local = cfg_merge(test_cfg, transport="local")

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

	def __init__(self,cfg={}):
		#my_cfg = default_config.copy()
		#my_cfg.update(test_cfg_local)
		#my_cfg.update(cfg)
		my_cfg = cfg
		super(TestServer,self).__init__(my_cfg)

	@property
	def root(self):
		return self.root_factory()
	def start(self):
		if self.root is None:
			self.root = self.make_root()
		return super(TestServer,self).start()

class TestClient(BrokerClient):
	def __init__(self,cfg={}):
		#my_cfg = default_config.copy()
		#my_cfg.update(test_cfg_local)
		#my_cfg.update(cfg)
		my_cfg = cfg
		super(TestClient,self).__init__(my_cfg)

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
		self.cfg.update(test_cfg_local)
		self.cfg.update(in_cfg)
		if 'TRACE' in os.environ:
			self.cfg['trace']=int(os.environ['TRACE'])

		super(TestMain,self).__init__()

		#self.client_cfg = test_cfg.copy()
		#self.client_cfg.update(test_cfg_local)
		#self.client_cfg.update(in_cfg)
		#self.client_cfg['_LocalQueue'] = self.cfg['_LocalQueue']
		self.client_cfg = self.cfg

	def setup(self):
		assert self.server is None
		assert self.client is None

		super(TestMain,self).setup()
		
		self.killer = spawn(killer,self,15)
		logger.debug("SE %s",id(self.cfg))
		self.server = self.server_factory(cfg=self.cfg)
		self.server.start()

	def main(self):
		logger.debug("Main start")
		assert self.client is None
		try:
			logger.debug("CL %s",id(self.client_cfg))
			self.client = self.client_factory(cfg=self.client_cfg)
			self.client.start()
			self.client.main()
		finally:
			logger.debug("Main ending")
			c,self.client = self.client,None
			if c is not None:
				c.stop()
			logger.debug("Main ended")

	def cleanup(self):
		logger.debug("Cleaning up")
		s,self.server = self.server,None
		if s is not None:
			s.stop()

		super(TestMain,self).cleanup()
		k,self.killer = self.killer,None
		if k is not None:
			k.kill()
		logger.debug("Cleaned up")

class TestBasicMain(Main):
	def setup(self):
		super(TestBasicMain,self).setup()
		self.killer = spawn(killer,self,15)
	def cleanup(self):
		super(TestBasicMain,self).cleanup()
		if self.killer is not None:
			self.killer.kill()

