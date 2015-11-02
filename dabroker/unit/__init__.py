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
from threading import Thread,Condition,Lock
import uuid
import etcd
import asyncio
from dabroker.util import attrdict
from etctree.node import mtValue

import logging
logger = logging.getLogger(__name__)

_d=attrdict
DEFAULT_CONFIG=_d(
	amqp=_d(
		server=_d(
			host='localhost',
			userid='guest',
			password='guest',
			virtual_host='/dabroker',
			ssl=False,
			connect_timeout=10,
		),
		exchanges=_d(      # all are persistent
			alert='alert', # topic: broadcast messages, may elicit multiple replies
			rpc='rpc',     # topic: RPC requests, will trigger exactly one reply
			reply='reply', # direct: all replies go here
			dead='dead',   # fanout: dead messages (TTL expires, reject, RPC/alert unrouteable, …)
		),
		queues=_d(
			alert='alert_',# plus the unit UUID. Nonpersistent.
			rpc='rpc_',    # plus the command name. Persistent.
			reply='reply_',# plus the unit UUID
			dead='dead',   # no add-on. Persistent. No TTL here!
		),
		ttl=_d(
			rpc=10,
		),
	))

class _NOTGIVEN:
	pass

_units = {}

# helper for recursive dict.[set]default()
def _r_setdefault(d,kv):
	for k,v in kv.items():
		if isinstance(v,mtValue):
			v = v.value
		try:
			dk = d[k]
		except KeyError:
			if isinstance(v,dict) and not isinstance(v,attrdict):
				v = attrdict(**v)
			d[k] = v
		else:
			if isinstance(d[k],dict):
				_r_setdefault(dk,v)

class Unit(object):
	"""The basic DaBroker messenger. Singleton per app (normally)."""
	etcd = None # etcd client
	config = None # configuration data
	conn = None # AMQP receiver
	recv_id = None # my UUID

	rpc_endpoints = None # RPC listeners
	alert_endpoints = None # 

	def __new__(cls, app, cfg, **kw):
		self = _units.get(app, None)
		if self is not None:
			return self
		return super(Unit,cls).__new__(cls)

	def __init__(self, app, cfg, **kw):
		if app in _units:
			return
		self.app = app

		self.config = self._get_config(cfg, **kw)
		self.conn_lock = Condition()

	@asyncio.coroutine
	def start(self):
		self.rpc_endpoints = {}
		self.alert_endpoints = {}
		self.register_alert(self._alert_ping,"dabroker.ping")
		self.register_rpc(self._reply_ping,"dabroker.ping")
		self.uuid = uuid.uuid1()

		yield from self._create_conn()
		_units[self.app] = self
	
	def stop(self):
		self._kill()
		
	def register_rpc(self, *a):
		"""\
			Register a listener.
				
				conn.register_rpc(RPCservice(fn,name))
				conn.register_rpc(fn,name)
				conn.register_rpc(fn)
				@conn_register(name)
				def fn(…): pass
				@conn_register
				def fn(…): pass
			"""
		name = None
		def reg(fn):
			nonlocal name
			from .rpc import RPCservice
			if not isinstance(fn,RPCservice):
				if name is None:
					name = fn.__name__
					name = name.replace('._','.')
					name = name.replace('_','.')
				fn = RPCservice(name=name,fn=fn)
			elif name is None:
				name = fn.name
			assert name not in self.rpc_endpoints, name
			assert fn.is_alert is None
			self.rpc_endpoints[name] = fn
			fn.is_alert = False
			return fn.fn
		assert len(a) <= 2
		if len(a) == 0:
			return reg
		elif len(a) == 2:
			name = a[1]
			return reg(a[0])
		else:
			a = a[0]
			if isinstance(a,str):
				name = a
				return reg
			else:
				return reg(a)
	
	def register_alert(self, fn, name=None):
		"""Register a listener"""
		from .rpc import RPCservice
		if not isinstance(fn,RPCservice):
			if name is None:
				name = fn.__name__
				name = name.replace('._','.')
				name = name.replace('_','.')
			fn = RPCservice(name=name,fn=fn)
		elif name is None:
			name = fn.name
		assert name not in self.alert_endpoints, name
		self.alert_endpoints[name] = fn
		fn.is_alert = True

	def _alert_ping(self, msg):
		msg.reply(dict(
			app=self.app,
			recv_id=self.uuid,
			))

	def _reply_ping(self,msg):
		msg.reply(dict(
			app=self.app,
			recv_id=self.uuid,
			rpc_endpoints=list(self.rpc_endpoints.keys())
			))
		
	def _get_config(self, cfg, **kw):
		"""Read config data from cfg and etcd"""
		if not isinstance(cfg,dict): # pragma: no branch
			from etctree.util import from_yaml
			cfg = from_yaml(cfg)
		_r_setdefault(kw,cfg)
		cfg=attrdict(**kw)

		if 'amqp' in cfg.config:
			self.config = cfg.config.amqp
		else:
			cfg.config.amqp = self.config = {}

		from etctree import client as etcd_client
		self.etcd = etcd_client(cfg)
		self.cfgtree = self.etcd.tree("/config", immediate=True)
		if 'specific' in self.cfgtree:
			specs = []
			try:
				spectree = self.cfgtree['specific']
				for part in self.app.split('.'):
					spectree = spectree[part]
					specs.append(spectree)
			except KeyError:
				pass
			for tree in specs:
				try:
					tree = tree.config
				except KeyError:
					pass
				else:
					_r_setdefault(cfg,tree)

		_r_setdefault(cfg.config,self.cfgtree)
		_r_setdefault(cfg.config,DEFAULT_CONFIG)

		# Also tell the user about our default settings
		_r_setdefault(self.cfgtree,DEFAULT_CONFIG)
		return cfg.config
		
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
		if c:
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

