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
from threading import Thread,Condition
import uuid
import etcd
from queue import Queue
from dabroker.util import attrdict

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
		dk = d.get(k,_NOTGIVEN)
		if dk is _NOTGIVEN:
			if isinstance(v,dict) and not isinstance(v,attrdict):
				v = attrdict(**v)
			d[k] = v
		elif isinstance(d[k],dict):
			_r_setdefault(dk,v)

class Unit(object):
	"""The basic DaBroker messenger. Singleton per app (normally)."""
	etcd = None # etcd client
	config = None # configuration data
	conn = None # AMQP receiver
	recv_id = None # my UUID
	queue = None
	reader = None
	writer = None

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

	def start(self):
		self.queue = Queue()
		self.reader = Thread(target=_reader, args=(weakref.proxy(self),))
		self.reader.start()
		self.writer = Thread(target=_writer, args=(weakref.proxy(self),))
		self.writer.start()
		self.rpc_endpoints = {}
		self.register_rpc("dabroker.info",self._server_info)
		self.recv_id = uuid.uuid1()

		_units[app] = self
	
	def register_rpc(self, fn, name=None):
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
		assert name not in self.rpc_endpoints, name
		self.rpc_endpoints = fn

	def _server_info(self, msg):
		msg.reply(dict(
			app=self.app,
			recv_id=self.recv_id,
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
		try:
			self.cfgtree = self.etcd.tree("/config", immediate=True)
		except etcd.EtcdKeyNotFound:
			logger.error("no /config subtree in etcd, using defaults")
			self.cfgtree = {}
		else:
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

			_r_setdefault(cfg,self.cfgtree)
			_r_setdefault(cfg,default_config)

			# Also tell the user about our default settings
			_r_setdefault(self.cfgtree,default_config)
		return cfg.config
		
	## The following code is the non-interesting cleanup part

	def __del__(self):
		self._kill()

	def _kill(self):
		q,self.queue = self.queue,None
		if q:
			try:
				q.put(None)
			except Exception:
				logger.exception("putting end token")
		self._kill_conn()
		r,self.reader=self.reader,None
		if r:
			try:
				r.join(timeout=10)
			except Exception:
				logger.exception("join reader")
		w,self.writer=self.writer,None
		if w:
			try:
				w.join(timeout=10)
			except Exception:
				logger.exception("join writer")

	def _kill_conn(self):
		c,self.conn = self.conn,None
		if c:
			try:
				c.close()
			except Exception:
				logger.exception("closing connection")

	def _create_conn(self):
		from .conn import Connection
		conn = Connection()
		for d in self.rpc_endpoints.values():
			conn.register_rpc(d)
		for d in self.alert_endpoints.values():
			conn.register_alert(d)
		conn.start()

		self.conn = conn

def _reader(self):
	"""\
		Read from messaging, dispatch"""
	backoff = 0.1
	while True:
		try:
			with self.conn_lock:
				self.conn = self._create_conn()
				self.conn_lock.notify()
			while self.conn is not None and self.conn.is_alive():
				x = self.read_conn()
				try:
					res = self.dispatch(x)
				except Exception as e:
					answer_with_errro()
				else:
					answer()
				ack(x)
				if backoff > 0.1:
					backoff /= 2
		except Exception as e:
			if not dir(self):
				# the proxy got tilted
				return
			logger.exception("_reader")
			if self.config.testing:
				raise
			sleep(backoff)
			backoff *= 1.5
		finally:
			self._kill_conn()

def _writer(self):
	"""\
		Read from the queue, publish to messaging
		"""
	backoff = 0.1
	while True:
		try:
			with self.conn_lock:
				while self.conn is None or not self.conn.is_alive():
					self.conn_lock.wait(10)
			while True:
				x = self.q.get()
				if x is None:
					return
				try:
					send_conn(x)
				except Exception as e:
					try:
						send_error_to(x,e)
					except Exception:
						logger.exception("_writer: nested error")
				if backoff > 0.1:
					backoff /= 2
		except Exception as e:
			if not dir(self):
				# the proxy got tilted
				return
			logger.exception("_writer")
			if self.config.testing:
				raise
			sleep(backoff)
			backoff *= 1.5
		finally:
			try:
				with self.conn_lock:
					c,self.conn = self.conn,None
			except ReferenceError:
				pass
			else:
				if c is not None:
					try:
						c.close()
					except Exception:
						logger.exception("_writer: closing the connection")

