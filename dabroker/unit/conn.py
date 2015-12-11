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

from .msg import _RequestMsg,PollMsg,RequestMsg,BaseMsg
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
		self._loop = unit._loop
		self.rpcs = {}
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


	async def connect(self):
		try:
			self.amqp_transport,self.amqp = await aioamqp.connect(loop=self._loop, **self.cfg)
		except Exception as e:
			logger.exception("Not connected to AMPQ: host=%s vhost=%s user=%s", self.cfg['host'],self.cfg['virtualhost'],self.cfg['login'])
			raise
		await self.setup_channels()

	async def _setup_one(self,name,typ,callback=None, q=None, route_key=None, exclusive=None):
		"""\
			Register
			"""
		unit = self.unit()
		cfg = unit.config['amqp']
		ch = _ch()
		setattr(self,name,ch)
		logger.debug("setup RPC for %s",name)
		ch.channel = await self.amqp.channel()
		ch.exchange = cfg['exchanges'][name]
		logging.debug("Chan %s: exchange %s", ch.channel,cfg['exchanges'][name])
		if exclusive is None:
			exclusive = (q is not None)
		await ch.channel.exchange_declare(cfg['exchanges'][name], typ, auto_delete=False, passive=False)

		if q is not None:
			assert callback is not None
			ch.queue = await ch.channel.queue_declare(cfg['queues'][name]+q, auto_delete=True, passive=False, exclusive=exclusive)
			await ch.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
			logging.debug("Chan %s: read %s", ch.channel,cfg['queues'][name]+q)
			await ch.channel.basic_consume(cfg['queues'][name]+q, callback=callback)
			if route_key is not None:
				logging.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges'][name], route_key, ch.queue['queue'])
				await ch.channel.queue_bind(ch.queue['queue'], cfg['exchanges'][name], routing_key=route_key)
		else:
			assert callback is None

		logger.debug("setup RPC for %s done",name)

	async def setup_channels(self):
		"""Configure global channels"""
		u = self.unit()
		await self._setup_one("alert",'topic', self._on_alert, u.uuid)
		await self._setup_one("rpc",'topic')
		await self._setup_one("reply",'direct', self._on_reply, u.uuid, u.uuid)

	async def _on_alert(self, body,envelope,properties):
		logger.debug("read alert message %s",envelope.delivery_tag)
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg,properties)
			rpc = self.alerts[msg.name]
			if rpc.call_conv == CC_DICT:
				a=(); k=msg.data
			elif rpc.call_conv == CC_DATA:
				a=(msg.data,); k={}
			else:
				a=(msg,); k={}

			reply_to = getattr(msg, 'reply_to',None)
			if reply_to:
				reply = msg.make_response()
				try:
					reply.data = await rpc.run(*a,**k)
				except Exception as exc:
					reply.set_error(exc, rpc.name,"reply")
				reply,props = reply.dump(self)
				if reply == "":
					reply = "0"
				else:
					reply = self.codec.encode(reply)
				await self.reply.channel.publish(reply, self.reply.exchange, reply_to, properties=props)
			else:
				await rpc.run(*a,**k)
		except Exception as exc:
			logger.exception("problem with message %s: %s", envelope.delivery_tag, body)
			await self.alert.channel.basic_reject(envelope.delivery_tag)
		else:
			logger.debug("ack message %s",envelope.delivery_tag)
			await self.alert.channel.basic_client_ack(envelope.delivery_tag)

	async def _on_rpc(self, rpc, body,envelope,properties):
		logger.debug("read rpc message %s",envelope.delivery_tag)
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg,properties)
			assert msg.name == rpc.name, (msg.name, rpc.name)
			reply = msg.make_response()
			try:
				if rpc.call_conv == CC_DICT:
					a=(); k=msg.data
				elif rpc.call_conv == CC_DATA:
					a=(msg.data,); k={}
				else:
					a=(msg,); k={}
				reply.data = await rpc.run(*a,**k)
			except Exception as exc:
				reply.set_error(exc, rpc.name,"reply")
			reply,props = reply.dump(self)
			if reply == "":
				reply = "0"
			else:
				reply = self.codec.encode(reply)
			await rpc.channel.publish(reply, self.reply.exchange, msg.reply_to, properties=props)
		except Exception as exc:
			logger.exception("problem with message %s: %s", envelope.delivery_tag, body)
			await rpc.channel.basic_reject(envelope.delivery_tag)
		else:
			logger.debug("ack message %s",envelope.delivery_tag)
			await rpc.channel.basic_client_ack(envelope.delivery_tag)

	async def _on_reply(self, body,envelope,properties):
		logger.debug("read reply message %s",envelope.delivery_tag)
		try:
			msg = self.codec.decode(body)
			msg = BaseMsg.load(msg,properties)
			f,req = self.replies[msg.correlation_id]
			try:
				await req.recv_reply(f,msg)
			except Exception as exc: # pragma: no cover
				if not f.done():
					f.set_exception(exc)
		except Exception as exc:
			await self.reply.channel.basic_reject(envelope.delivery_tag)
			logger.exception("problem with message %s: %s", envelope.delivery_tag, body)
		else:
			logger.debug("ack message %s",envelope.delivery_tag)
			await self.reply.channel.basic_client_ack(envelope.delivery_tag)

	async def call(self,msg, timeout=None):
		cfg = self.unit().config['amqp']
		if timeout is None:
			tn = getattr(msg,'_timer',None)
			if tn is not None:
				timeout = self.unit().config['amqp']['timeout'].get(tn,None)
				if timeout is not None:
					timeout = float(timeout)
		assert isinstance(msg,_RequestMsg)
		data,props = msg.dump(self)
		if data == "":
			data = "0"
		else:
			data = self.codec.encode(data)
		if timeout is not None:
			f = asyncio.Future(loop=self._loop)
			id = msg.message_id
			self.replies[id] = (f,msg)
		logger.debug("Send %s to %s: %s", msg.name, cfg['exchanges'][msg._exchange], data)
		await getattr(self,msg._exchange).channel.publish(data, cfg['exchanges'][msg._exchange], msg.name, properties=props)
		if timeout is None:
			return
		try:
			await asyncio.wait_for(f,timeout, loop=self._loop)
		except asyncio.TimeoutError:
			if isinstance(msg,PollMsg):
				return msg.replies
			raise # pragma: no cover
		finally:
			del self.replies[id]
		return f.result()
		
	async def register_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		assert rpc.queue is None
		rpc.channel = await self.amqp.channel()
		rpc.queue = await rpc.channel.queue_declare(cfg['queues']['rpc']+rpc.name.replace('.','_'), auto_delete=True, passive=False)
		logging.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges']['rpc'], rpc.name, rpc.queue['queue'])
		await rpc.channel.queue_bind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)
		self.rpcs[rpc.uuid] = rpc

		await rpc.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
		logging.debug("Chan %s: read %s", rpc.channel,rpc.queue['queue'])
		callback=functools.partial(self._on_rpc,rpc)
		callback._is_coroutine = True
		await rpc.channel.basic_consume(rpc.queue['queue'], callback=callback, consumer_tag=rpc.uuid)

	async def unregister_rpc(self,rpc):
		ch = self.rpc
		cfg = self.unit().config['amqp']
		if isinstance(rpc,str):
			rpc = self.rpcs.pop(rpc)
		else:
			del self.rpcs[rpc.uuid]
		assert rpc.queue is not None
		logging.debug("Chan %s: unbind %s %s %s", ch.channel,cfg['exchanges']['rpc'], rpc.name, rpc.queue['queue'])
		await rpc.channel.queue_unbind(rpc.queue['queue'], cfg['exchanges']['rpc'], routing_key=rpc.name)
		logging.debug("Chan %s: noread %s", rpc.channel,rpc.queue['queue'])
		await rpc.channel.basic_cancel(consumer_tag=rpc.uuid)

	async def register_alert(self,rpc):
		ch = self.alert
		cfg = self.unit().config['amqp']
		logging.debug("Chan %s: bind %s %s %s", ch.channel,cfg['exchanges']['alert'], rpc.name, ch.exchange)
		await ch.channel.queue_bind(ch.queue['queue'], ch.exchange, routing_key=rpc.name)
		self.alerts[rpc.name] = rpc

	async def unregister_alert(self,rpc):
		if isinstance(rpc,str):
			rpc = self.alerts.pop(rpc)
		else:
			del self.alerts[rpc.name]
		ch = self.alert
		cfg = self.unit().config['amqp']
		logging.debug("Chan %s: unbind %s %s %s", ch.channel,cfg['exchanges']['alert'], rpc.name, ch.exchange)
		await ch.channel.queue_unbind(ch.queue['queue'], ch.exchange, routing_key=rpc.name)

	async def close(self):
		a,self.amqp = self.amqp,None
		if a is not None:
			try:
				await a.close(timeout=1)
			except Exception: # pragma: no cover
				logger.exception("closing the connection")
			self.amqp_transport = None

	def _kill(self):
		self.amqp = None
		a,self.amqp_transport = self.amqp_transport,None
		if a is not None:
			try:
				a.close()
			except Exception: # pragma: no cover
				logger.exception("killing the connection")

