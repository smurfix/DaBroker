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

##
## Configuration: look up, in order:
## yaml_cfg.config
## etcd.specific.APP.config
## etcd.config
## 

import weakref
import etcd
import asyncio
from ..util import attrdict, import_string, uuidstr
from .msg import RequestMsg,PollMsg,AlertMsg
from .rpc import CC_MSG
from . import DEFAULT_CONFIG

import logging
logger = logging.getLogger(__name__)

class _NOTGIVEN:
	pass

# helper for recursive dict.[set]default()
def _r_setdefault(d,kv):
	for k,v in kv.items():
		try:
			dk = d[k]
		except KeyError:
			d[k] = v
		else:
			if isinstance(d[k],dict):
				_r_setdefault(dk,v)

class Unit(object):
	"""The basic DaBroker messenger. Singleton per app (normally)."""
	etcd = None # etcd client
	config = None # configuration data
	cfgtree = None
	conn = None # AMQP receiver
	uuid = None # my UUID

	def __init__(self, app, cfg, loop=None):
		self._loop = loop or asyncio.get_event_loop()
		self.app = app
		self._cfg = cfg

		self.rpc_endpoints = {}
		self.alert_endpoints = {}

		self.register_alert("dabroker.ping",self._alert_ping)

	@asyncio.coroutine
	def start(self):
		self.uuid = uuidstr()
		self.config = self._get_config(self._cfg)

		self.register_rpc("dabroker.ping."+self.uuid, self._reply_ping)

		yield from self._create_conn()
	
	@asyncio.coroutine
	def stop(self):
		self.rpc_endpoints.pop("dabroker.ping."+self.uuid, None)

		c,self.conn = self.conn,None
		if c:
			try:
				yield from c.close()
			except Exception: # pragma: no cover
				logger.exception("closing connection")
		self._kill()
	
	## client

	@asyncio.coroutine
	def rpc(self,name, _data=None, **data):
		"""Send a RPC request.
		Returns the response. 
		The (global) timeout is set in etcd.
		"""
		if _data is not None:
			assert not data
			data = _data
		msg = RequestMsg(name, self, data)
		res = yield from self.conn.call(msg)
		res.raise_if_error()
		return res.data

	@asyncio.coroutine
	def alert(self,name, _data=None, *, timeout=None,callback=None,call_conv=CC_MSG, **data):
		"""Send a broadcast alert.
		If @callback is not None, call on each response until the time runs out
		"""
		if _data is not None:
			assert not data
			data = _data
		if callback:
			msg = PollMsg(name, self, data=data, callback=callback,call_conv=call_conv)
		else:
			msg = AlertMsg(name, self, data=data)
		res = yield from self.conn.call(msg, timeout=timeout)
		return res
		
	## server

	def register_rpc(self, *a, async=False, alert=False, call_conv=CC_MSG):
		"""\
			Register a listener.
				
				conn.register_rpc(RPCservice(fn,name))
				conn.register_rpc(name,fn)
				conn.register_rpc(fn)
				@conn_register(name)
				def fn(…): pass
				@conn_register
				def fn(…): pass
			"""
		name = None
		@asyncio.coroutine
		def reg_async(fn,epl):
			if alert:
				yield from self.conn.register_alert(fn)
			else:
				yield from self.conn.register_rpc(fn)
			epl[name] = fn
		def reg(fn):
			nonlocal name
			from .rpc import RPCservice
			if not isinstance(fn,RPCservice):
				if name is None:
					name = fn.__name__
					name = name.replace('._','.')
					name = name.replace('_','.')
				fn = RPCservice(name=name,fn=fn, call_conv=call_conv)
			elif name is None:
				name = fn.name
			assert fn.is_alert is None
			if alert:
				epl = self.alert_endpoints
			else:
				epl = self.rpc_endpoints
			assert name not in epl, name
			fn.is_alert = alert
			if async and self.conn is not None:
				return reg_async(fn,epl)
			else:
				assert self.conn is None,"Use register_rpc_async when online"
			epl[name] = fn
			return fn.fn
		assert len(a) <= 2
		if len(a) == 0:
			return reg
		elif len(a) == 2:
			name = a[0]
			return reg(a[1])
		else:
			a = a[0]
			if isinstance(a,str):
				name = a
				return reg
			else:
				return reg(a)
	
	def register_alert(self, *a, async=False, call_conv=CC_MSG):
		"""Register a listener"""
		return self.register_rpc(*a, async=async, alert=True, call_conv=call_conv)

	def _alert_ping(self,msg):
		return dict(
			app=self.app,
			uuid=self.uuid,
			)

	def _reply_ping(self,msg):
		return dict(
			app=self.app,
			uuid=self.uuid,
			rpc_endpoints=list(self.rpc_endpoints.keys())
			)
		
	def _get_config(self, cfg):
		"""Read config data from cfg and etcd"""
		if not isinstance(cfg,dict): # pragma: no cover
			from etctree.util import from_yaml
			cfg = from_yaml(cfg)['config']

		_r_setdefault(cfg,DEFAULT_CONFIG)
		return cfg
		
	## The following code is the non-interesting cleanup part

	def __del__(self):
		self._kill()

	def _kill(self):
		self._kill_conn()
		c,self.cfgtree = self.cfgtree,None
		if c is not None:
			c._kill()

	def _kill_conn(self):
		c,self.conn = self.conn,None
		if c: # pragma: no cover
			try:
				c.close()
			except Exception:
				logger.exception("closing connection")

	@asyncio.coroutine
	def _create_conn(self):
		from .conn import Connection
		conn = Connection(self)
		yield from conn.connect()
		for d in self.rpc_endpoints.values():
			yield from conn.register_rpc(d)
		for d in self.alert_endpoints.values():
			yield from conn.register_alert(d)
		self.conn = conn

