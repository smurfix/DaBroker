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
import amqp
from threading import Thread

import logging
logger = logging.getLogger(__name__)

class _ch(object):
	"""Helper object"""
	channel = None
	exchange = None
	queue = None

class Connection(object):
	task = None # background reader
	amqp = None # connection

	def __init__(self,unit):
		self.unit = weakref.ref(unit)
		cfg = unit.config['amqp']['server']
		if 'connect_timeout' in cfg:
			cfg['connect_timeout'] = float(cfg['connect_timeout'])
		if 'ssl' in cfg and isinstance(cfg['ssl'],str):
			cfg['ssl'] = cfg['ssl'].lower() == 'true'
		if 'port' in cfg:
			cfg['port'] = int(cfg['port'])
		self.amqp = amqp.connection.Connection(**cfg)
		try:
			self.setup_channels()
		except Exception as e:
			logger.exception("Not connected to AMPQ: host=%s vhost=%s user=%s", cfg['host'],cfg['virtual_host'],cfg['userid'])
			a,self.amqp = self.amqp,None
			if a is not None:
					a.close()
			raise

	def _setup_one(self,name,typ,callback=None, q=None, route_key=None, exclusive=False):
		"""\
			Register
			"""
		unit = self.unit()
		cfg = unit.config['amqp']
		ch = _ch()
		setattr(self,name,ch)
		logger.debug("setup RPC for %s",name)
		ch.channel = self.amqp.channel()
		ch.exchange = cfg['exchanges'][name]
		ch.channel.exchange_declare(exchange=cfg['exchanges'][name], type=typ, auto_delete=False, passive=False)

		if q is not None:
			assert callback is not None
			ch.queue = ch.channel.queue_declare(queue=cfg['queues'][name]+q, auto_delete=True, passive=False, exclusive=exclusive)
			ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,a_global=False)
			ch.channel.basic_consume(callback=callback, queue=cfg['queues'][name]+q)
			if route_key is not None:
				ch.channel.queue_bind(ch.queue.queue, exchange=cfg['exchanges'][name], routing_key=route_key)
		else:
			assert callback is None

		logger.debug("setup RPC for %s done",name)

	def setup_channels(self):
		"""Configure global channels"""
		u = self.unit()
		self._setup_one("alert",'topic', self._on_alert, str(u.uuid))
		self._setup_one("rpc",'topic')
		self._setup_one("reply",'direct', self._on_reply, str(u.uuid), str(u.uuid))

	def _on_alert(self,*a,**k):
		import pdb;pdb.set_trace()
		pass

	def _on_reply(self,*a,**k):
		import pdb;pdb.set_trace()
		pass

	def register_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		assert rpc.queue is None
		rpc.channel = self.amqp.channel()
		rpc.queue = rpc.channel.queue_declare(queue=cfg['queues']['rpc']+rpc.name.replace('.','_'), auto_delete=True, passive=False)
		rpc.channel.queue_bind(rpc.queue.queue, exchange=cfg['exchanges']['rpc'], routing_key=rpc.name)

		rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,a_global=False)
		rpc.channel.basic_consume(callback=rpc.run, queue=rpc.queue.queue)

	def register_alert(self,rpc):
		ch = self.alert
		cfg = self.unit().config['amqp']
		ch.channel.queue_bind(ch.queue.queue, exchange=ch.exchange, routing_key=rpc.name)

	def call(self,fn,*a,**k):
		import pdb;pdb.set_trace()
		pass

	def run(self):
		self.task = Thread(target=_run, args=(self,))
		self.task.start()

	def close(self):
		a,self.amqp = self.amqp,None
		if a is not None and a.transport is not None:
			# This code deadlocks.
			try:
				a.close(nowait=True)
			except Exception:
				logger.exception("closing the connection")

		t,self.task = self.task,None
		if t is not None:
			try:
				t.join(10)
			except Exception:
				logger.exception("stopping the reader task")

		if a is not None and a.transport is not None:
			try:
				a._do_close() # layering violation
			except Exception:
				logger.exception("force-closing the connection 2")

	def __del__(self):
		a,self.amqp = self.amqp,None
		if a is not None and a.transport is not None:
			a._do_close() # layering violation

def _run(self):
	self = weakref.ref(self)
	logger.debug("Start conn thread")
	try:
		while True:
			amqp = None
			try:
				amqp = self().amqp
				if amqp.transport is None:
					return # got closed
				amqp.drain_events()
			except ReferenceError:
				return
			except Exception:
				if amqp is not None:
					logger.exception("reading from AMQP")
					try:
						amqp.close()
						while amqp.transport is not None:
							amqp.drain_events(timeout=10)
					except BrokenPipeError:
						pass
					except Exception:
						logger.exception("closing AMQP")
				return
	finally:
		logger.debug("Stop conn thread")

