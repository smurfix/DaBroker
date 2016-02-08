#!/usr/bin/python3
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

import asyncio
from dabroker.unit import Unit, CC_DATA
from dabroker.util.tests import load_cfg
import signal
import pprint
import json

import logging
import sys
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

u=Unit("dabroker.monitor", load_cfg("test.cfg")['config'])

@u.register_rpc("example.hello", call_conv=CC_DATA)
def hello(name="Joe"):
	return "Hello %s!" % name

channels = {'alert':'topic', 'rpc':'topic', 'reply':'direct'}

class mon:
	def __init__(self,u,typ,name):
		self.u = u
		self.typ = typ
		self.name = u.config['amqp']['exchanges'][name]

	async def start(self):
		self.channel = await u.conn.amqp.channel()
		await self.channel.exchange_declare(self.name, self.typ, auto_delete=False, passive=False)
		self.queue_name = 'mon_'+self.name+'_'+self.u.uuid
		self.queue = await self.channel.queue_declare(self.queue_name, auto_delete=True, passive=False, exclusive=True)
		await self.channel.basic_qos(prefetch_count=1,prefetch_size=0,connection_global=False)
		await self.channel.queue_bind(self.queue_name, self.name, routing_key='#')
		await self.channel.basic_consume(queue_name=self.queue_name, callback=self.callback)
	
	async def callback(self, channel,body,envelope,properties):
		if properties.content_type == 'application/json':
			body = json.loads(body.decode('utf-8'))

		if self.name == 'alert':
			if envelope.routing_key == 'dabroker.start':
				c = channels['reply']
				await jobs.put(BindMe(c, c.queue_name, c.name, routing_key=body['uuid']))
			elif envelope.routing_key == 'dabroker.stop':
				c = channels['reply']
				loop.call_later(10,jobs.put_nowait,UnBindMe(c, c.queue_name, c.name, routing_key=body['uuid']))

		m = {'body':body, 'prop':{}, 'env':{}}
		for p in dir(properties):
			if p.startswith('_'):
				continue
			v = getattr(properties,p)
			if v is not None:
				m['prop'][p] = v
		for p in dir(envelope):
			if p.startswith('_'):
				continue
			v = getattr(envelope,p)
			if v is not None:
				m['env'][p] = v
		pprint.pprint(m)
		await self.channel.basic_client_ack(delivery_tag = envelope.delivery_tag)

class BindMe:
	def __init__(self,c,*a,**k):
		self.c = c
		self.a = a
		self.k = k
	async def run(self):
		await self.c.channel.queue_bind(*self.a,**self.k)

class UnBindMe:
	def __init__(self,c,*a,**k):
		self.c = c
		self.a = a
		self.k = k
	async def run(self):
		await self.c.channel.queue_unbind(*self.a,**self.k)

##################### main loop

loop=None
jobs=None
quitting=False

class StopMe:
	async def run(self):
		global quitting
		quitting = True

async def mainloop():
	await u.start()
	for c,t in channels.items():
		channels[c] = m = mon(u,t,c)
		await m.start()
	while not quitting:
		j = await jobs.get()
		await j.run()
	await u.stop()

def _tilt():
	loop.remove_signal_handler(signal.SIGINT)
	loop.remove_signal_handler(signal.SIGTERM)
	jobs.put(StopMe())

def main():
	global loop
	global jobs
	jobs = asyncio.Queue()
	loop = asyncio.get_event_loop()
	loop.add_signal_handler(signal.SIGINT,_tilt)
	loop.add_signal_handler(signal.SIGTERM,_tilt)
	loop.run_until_complete(mainloop())

if __name__ == '__main__':
	main()

