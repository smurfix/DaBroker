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
import functools

from .msg import _RequestMsg,PollMsg,RequestMsg,BaseMsg,ResponseMsg
from .rpc import CC_DICT,CC_DATA
from ..util import import_string

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
		self.alerts = {}
		self.replies = {}
		self.unit = weakref.ref(unit)
		cfg = unit.config['amqp']['server']
		if 'connect_timeout' in cfg:
			cfg['connect_timeout'] = float(cfg['connect_timeout'])
		if 'ssl' in cfg and isinstance(cfg['ssl'],str):
			cfg['ssl'] = cfg['ssl'].lower() == 'true'
		if 'port' in cfg:
			cfg['port'] = int(cfg['port'])
		self.cfg = cfg

		codec_type = unit.config['amqp']['codec']
		if codec_type[0] == '_':
			codec_type = codec_type[1:]
			self.codec = import_string('dabroker.base.codec.%s.RawCodec' % (codec_type,))()
			self.mime_type = "application/"+codec_type
		else:
			self.codec = import_string('dabroker.base.codec.%s.Codec' % (codec_type,))()
			self.codec_type = codec_type
			self.mime_type = "application/"+codec_type+"+dabroker"


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
		yield from self._setup_one("alert",'topic', self._on_alert, u.uuid)
		yield from self._setup_one("rpc",'topic')
		yield from self._setup_one("reply",'direct', self._on_reply, u.uuid, u.uuid)

	@asyncio.coroutine
	def _on_alert(self, body,envelope,properties):
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg)
			rpc = self.alerts[msg.name]
			if rpc.call_conv == CC_DICT:
				a=(); k=msg.data
			elif rpc.call_conv == CC_DATA:
				a=(msg.data,); k={}
			else:
				a=(msg,); k={}

			reply_to = getattr(msg, 'reply_to',None)
			if reply_to:
				reply = ResponseMsg(msg)
				try:
					reply.data = yield from rpc.run(*a,**k)
				except Exception as exc:
					reply.set_error(exc, rpc.name,"reply")
				reply = reply.dump()
				reply = self.codec.encode(reply)
				yield from self.reply.channel.publish(reply, self.reply.exchange, reply_to)
			else:
				yield from rpc.run(*a,**k)
		except Exception as exc:
			logger.exception("problem receiving message: %s",body)
			yield from self.alert.channel.basic_reject(envelope.delivery_tag)
		else:
			yield from self.alert.channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def _on_rpc(self, rpc, body,envelope,properties):
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg)
			assert msg.name == rpc.name, (msg.name, rpc.name)
			reply = ResponseMsg(msg)
			try:
				if rpc.call_conv == CC_DICT:
					a=(); k=msg.data
				elif rpc.call_conv == CC_DATA:
					a=(msg.data,); k={}
				else:
					a=(msg,); k={}
				reply.data = yield from rpc.run(*a,**k)
			except Exception as exc:
				reply.set_error(exc, rpc.name,"reply")
			reply = reply.dump()
			reply = self.codec.encode(reply)
			yield from rpc.channel.publish(reply, self.reply.exchange, msg.reply_to)
		except Exception as exc:
			logger.exception("problem with message: %s",body)
			yield from rpc.channel.basic_reject(envelope.delivery_tag)
		else:
			yield from rpc.channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def _on_reply(self, body,envelope,properties):
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg)
			f,req = self.replies[msg.in_reply_to]
			try:
				yield from req.recv_reply(f,msg)
			except Exception as exc:
				if not f.done():
					f.set_exception(exc)
		except Exception as exc:
			yield from self.reply.channel.basic_reject(envelope.delivery_tag)
			logger.exception("problem receiving message: %s",body)
		else:
			yield from self.reply.channel.basic_client_ack(envelope.delivery_tag)

	@asyncio.coroutine
	def call(self,msg, timeout=None):
		cfg = self.unit().config['amqp']
		if timeout is None:
			tn = getattr(msg,'_timer',None)
			if tn is not None:
				timeout = self.unit().config['amqp']['timeout'].get(tn,None)
				if timeout is not None:
					timeout = float(timeout)
		assert isinstance(msg,_RequestMsg)
		data = msg.dump()
		data = self.codec.encode(data)
		if timeout is not None:
			f = asyncio.Future()
			id = msg.message_id
			self.replies[id] = (f,msg)
		yield from getattr(self,msg._exchange).channel.publish(data, cfg['exchanges'][msg._exchange], msg.name)
		if timeout is None:
			return
		try:
			yield from asyncio.wait_for(f,timeout)
		except asyncio.TimeoutError:
			if isinstance(msg,PollMsg):
				return msg.replies
			raise
		finally:
			del self.replies[id]
		return f.result()
		
	@asyncio.coroutine
	def register_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		assert rpc.queue is None
		rpc.channel = yield from self.amqp.channel()
		rpc.queue = yield from rpc.channel.queue_declare(cfg['queues']['rpc']+rpc.name.replace('.','_'), auto_delete=True, passive=False)
		yield from rpc.channel.queue_bind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)

		yield from rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
		yield from rpc.channel.basic_consume(rpc.queue['queue'], callback=functools.partial(self._on_rpc,rpc))

	@asyncio.coroutine
	def register_alert(self,rpc):
		ch = self.alert
		cfg = self.unit().config['amqp']
		yield from ch.channel.queue_bind(ch.queue['queue'], ch.exchange, routing_key=rpc.name)
		self.alerts[rpc.name] = rpc

	@asyncio.coroutine
	def close(self):
		a,self.amqp = self.amqp,None
		if a is not None:
			try:
				yield from a.close(timeout=1)
			except Exception:
				logger.exception("closing the connection")
			self.amqp_transport = None

