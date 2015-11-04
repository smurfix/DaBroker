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

import weakref
import asyncio
import aioamqp

import logging
logger = logging.getLogger(__name__)

class _ch(object):
	"""Helper object"""
	channel = None
	exchange = None
	queue = None

class Connection(object):
	amqp = None # connection

	def __init__(self,unit):
		self.rpc = {}
		self.unit = weakref.ref(unit)
		cfg = unit.config['amqp']['server']
		if 'connect_timeout' in cfg:
			cfg['connect_timeout'] = float(cfg['connect_timeout'])
		if 'ssl' in cfg and isinstance(cfg['ssl'],str):
			cfg['ssl'] = cfg['ssl'].lower() == 'true'
		if 'port' in cfg:
			cfg['port'] = int(cfg['port'])
		self.cfg = cfg

	@asyncio.coroutine
	def connect(self):
		try:
			self.amqp_transport,self.amqp = yield from aioamqp.connect(**self.cfg)
		except Exception as e:
			logger.exception("Not connected to AMPQ: host=%s vhost=%s user=%s", self.cfg['host'],self.cfg['virtualhost'],self.cfg['login'])
			raise
		yield from self.setup_channels()

	@asyncio.coroutine
	def _setup_one(self,name,typ,callback=None, q=None, route_key=None, exclusive=False):
		"""\
			Register
			"""
		unit = self.unit()
		cfg = unit.config['amqp']
		ch = _ch()
		setattr(self,name,ch)
		logger.debug("setup RPC for %s",name)
		ch.channel = yield from self.amqp.channel()
		ch.exchange = cfg['exchanges'][name]
		yield from ch.channel.exchange_declare(cfg['exchanges'][name], typ, auto_delete=False, passive=False)

		if q is not None:
			assert callback is not None
			ch.queue = yield from ch.channel.queue_declare(cfg['queues'][name]+q, auto_delete=True, passive=False, exclusive=exclusive)
			yield from ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
			yield from ch.channel.basic_consume(cfg['queues'][name]+q, callback=callback)
			if route_key is not None:
				yield from ch.channel.queue_bind(ch.queue['queue'], cfg['exchanges'][name], routing_key=route_key)
		else:
			assert callback is None

		logger.debug("setup RPC for %s done",name)

	@asyncio.coroutine
	def setup_channels(self):
		"""Configure global channels"""
		u = self.unit()
		yield from self._setup_one("alert",'topic', self._on_alert, str(u.uuid))
		yield from self._setup_one("rpc",'topic')
		yield from self._setup_one("reply",'direct', self._on_reply, str(u.uuid), str(u.uuid))

	@asyncio.coroutine
	def _on_alert(self,*a,**k):
		import pdb;pdb.set_trace()
		pass

	@asyncio.coroutine
	def _on_reply(self,*a,**k):
		import pdb;pdb.set_trace()
		pass

	@asyncio.coroutine
	def register_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		assert rpc.queue is None
		rpc.channel = yield from self.amqp.channel()
		rpc.queue = yield from rpc.channel.queue_declare(cfg['queues']['rpc']+rpc.name.replace('.','_'), auto_delete=True, passive=False)
		yield from rpc.channel.queue_bind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)

		yield from rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
		yield from rpc.channel.basic_consume(rpc.queue['queue'], callback=rpc.run)

	@asyncio.coroutine
	def register_alert(self,rpc):
		ch = self.alert
		cfg = self.unit().config['amqp']
		yield from ch.channel.queue_bind(ch.queue['queue'], ch.exchange, routing_key=rpc.name)

	@asyncio.coroutine
	def call(self,fn,data):


		import pdb;pdb.set_trace()
		pass

	@asyncio.coroutine
	def alert(self,fn,*a,timeout=None,**k):
		import pdb;pdb.set_trace()
		pass

	@asyncio.coroutine
	def close(self):
		a,self.amqp = self.amqp,None
		if a is not None:
			try:
				yield from a.close(timeout=1)
			except Exception:
				logger.exception("closing the connection")
			self.amqp_transport = None

