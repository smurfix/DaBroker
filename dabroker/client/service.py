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

# This module implements the basic client server.

RETR_TIMEOUT = 10
CACHE_SIZE=10000

from ..base.serial import Codec
from .serial import adapters, client_broker_info_meta

import logging
logger = logging.getLogger("dabroker.client.service")

from weakref import WeakValueDictionary,KeyedRef,ref
from collections import deque
from gevent.event import AsyncResult
from heapq import heapify,heappop

def search_key(kw):
	"""Build a reproducible string from search keywords"""
	return ",".join("{}:{}".format(k, ".".join(v._key) if hasattr(v,'_key') else v) for k,v in sorted(kw.items()))

class KnownSearch(object):
	def __init__(self, kw, res):
		self.kw = kw
		self.res = res

class ServerError(Exception):
	"""An encapsulation for a server error (with traceback)"""
	def __init__(self,err,tb):
		self.err = err
		self.tb = tb

	def __repr__(self):
		return "ServerError({})".format(repr(self.err))

	def __str__(self):
		r = repr(self)
		if self.tb is None: return r
		return r+"\n"+self.tb

class ExtKeyedRef(KeyedRef):
	"""A KeyedRef which includes an access counter."""

	__slots__ = "key","counter"

	def __new__(type, ob, callback, key):
		self = ref.__new__(type, ob, callback)
		self.key = key
		self.counter = 0
		return self

	def __init__(self, ob, callback, key):
		super(ExtKeyedRef,  self).__init__(ob, callback, key)
	
	def __lt__(self,other):
		return self.counter > other.counter
		# Yes, this is backwards. That is intentional. See below.

class CountedCache(WeakValueDictionary,object):
	"""A WeakValueDictionary which counts accesses."""
	def __init__(self, *args, **kw):
		super(CountedCache,self).__init__(*args,**kw)
		def remove(wr, selfref=ref(self)):
			self = selfref()
			if self is not None:
				if self._iterating:
					self._pending_removals.append(wr.key)
				else:
					ref = self.data.pop(wr.key,None)
					if ref is not None:
						ref.counter = -1
		self._remove = remove

	def __getitem__(self, key):
		r = self.data[key]
		o = r()
		if o is None:
			raise KeyError(key)
		else:
			r.counter += 1
			return o

	def get_ref(self, key, default=None):
		return self.data.get(key, default)

	def get_counter(self, key):
		r = self.data.get(key,None)
		if r is None:
			return -1
		return r.count

	def __setitem__(self, key, value):
		if self._pending_removals:
			self._commit_removals()

		ref = self.data.get(key, None)
		if ref is not None:
			ref.counter = -1
		self.data[key] = ExtKeyedRef(value, self._remove, key)

	def setdefault(self, key, default=None):
		try:
			wr = self.data[key]
		except KeyError:
			if self._pending_removals:
				self._commit_removals()
			self.data[key] = ExtKeyedRef(default, self._remove, key)
			return default
		else:
			return wr()

	def update(self, dict=None, **kwargs):
		if self._pending_removals:
			self._commit_removals()
		d = self.data
		if dict is not None:
			if not hasattr(dict, "items"):
				dict = type({})(dict)
			for key, o in dict.items():
				ref = self.data.get(key, None)
				if ref is not None:
					ref.counter = -1
				d[key] = ExtKeyedRef(o, self._remove, key)
		if len(kwargs):
			self.update(kwargs)

class CacheDict(CountedCache):
	"""\
		This is a WeakValueDict which keeps the last CACHE_SIZE items pinned.

		.lru is a hash which acts as a FIFO (think collections.deque,
		except that a deque's length is not mutable).

		Items popping off the FIFO are added to a heap (sized CACHE_SIZE/10
		by default). The most-used half of these items are re-added to the FIFO, the rest is dropped.

		"""
	def __init__(self,*a,**k):
		self.lru = {}
		self.lru_next = 0
		self.lru_last = 0
		self.lru_size = CACHE_SIZE

		self.heap_min = CACHE_SIZE/20
		self.heap_max = CACHE_SIZE/10
		self.heap = []
		super(CacheDict,self).__init__(*a,**k)

	def set(self, key,value):
		"""Set an item, but bypass the cache"""
		super(CacheDict,self).__setitem__(key,value)
		return value

	def __setitem__(self, key, value):
		super(CacheDict,self).__setitem__(key,value)
		id = self.lru_next; self.lru_next += 1
		self.lru[id] = (key,value)

		# Move items from the queue to the heap
		min_id = id - self.lru_size
		while self.lru_last < min_id:
			id = self.lru_last; self.lru_last += 1
			if id not in self.lru: continue
			key,value = self.lru[id]
			ref = self.data.get(key,0)
			if ref is not None:
				self.heap.append((ref,key,value))

		# When enough items accumulate on the heap:
		# Move the most-used items back to the queue
		# This block should not schedule.
		if len(self.heap) > self.heap_max:
			self.heap = [(r,k,v) for r,k,v in self.heap if r.counter >= 0]
			heapify(self.heap)
			while len(self.heap) > self.heap_min:
				ref,key,value = heappop(self.heap)
				if ref.counter > 1:
					id = self.lru_next; self.lru_next += 1
					ref.counter = 0
					self.lru[id] = (key,value)

			self.heap = [] ## optional

class BrokerClient(object):
	"""\
		The basic client implementation. Singleton (for now).

		@server: a callable which sends a message to the server (and returns a reply)
		"""
	root_key = None

	def __init__(self, server):
		global client
		assert client is None

		self.server = server
		self._cache = CacheDict()
		self.codec = Codec(self)
		self.codec.register(adapters)

		self._add_to_cache(client_broker_info_meta)
		client = self

	def _add_to_cache(self, obj):
		key = getattr(obj,'_key',None)
		if key is None:
			old = None
		else:
			old = self._cache.get(key, None)
			if old is obj:
				return
		if obj is not None:
			self._cache[key] = obj

		if isinstance(old,AsyncResult):
			old.set(obj)

	def get(self, key):
		"""Get an object, from cache or from the server."""
		obj = self._cache.get(key,None)
		if obj is not None:
			if isinstance(obj,AsyncResult):
				obj = obj.get(timeout=RETR_TIMEOUT)
			return obj

		ar = self._cache.set(key, AsyncResult())
		try:
			obj = self._send("get",key)
		except Exception as e:
			# Remove the AsyncResult from cache and forward the excption to any waiters
			self._cache.pop(key).set_exception(e)
			del ar # otherwise the optimizer will drop this, and thus
				   # delete the weakref, _before_ the previous line!
			raise
		else:
			self._add_to_cache(obj) # sends to the AsyncResult as a side effect
			return obj
		
	def find(self, typ, _limit=None, **kw):
		"""Find objects by keyword"""
		kws = search_key(kw)
		res = typ.searches.get(kws,None)
		if res is not None:
			return res.res
		
		skw = {'k':kw}
		if _limit is not None:
			skw['lim'] = _limit
		res = self._send("find",typ._key, **skw)
		ks = KnownSearch(kw,res)
		typ.searches[kws] = ks
		self._cache[" ".join(str(x) for x in typ._key)+":"+kws] = ks
		return res

	def call(self, obj,name,a,k):
		return self._send("call",name, o=obj,a=a,k=k)
		
	def do_ping(self,msg):
		"""The server wants to know who's listening. So tell it."""
		logger.debug("ping %r",msg)
		self._send("pong")

	def do_pong(self,msg):
		# for completeness. The server doesn't send a broadcast on client request.
		raise RuntimeError("This can't happen")

	def do_invalid(self,msg):
		"""Directly invalidate these cache entries."""
		for k in msg:
			k = tuple(k)
			try:
				del self._cache[k]
			except KeyError:
				logger.debug("inval: not found: %r",k)
			else:
				logger.debug("inval: dropped: %r",k)

	def do_invalid_key(self,key, m=None,k={}):
		"""Invalidate an object, plus whatever might have been used to search for it.
		
			@key the object
			@m the object's metadata key (search results hang off metadata)
			@k: a key=>(value,…) dict. A search is obsoleted when one
			                           of the search keys matches one of the values.
			"""
		key = tuple(key)
		logger.debug("inval_key: %r: %r",key,k)
		obj = self._cache.pop(key,None)
		if obj is None:
			logger.debug("not in cache")

		if m is None:
			logger.warn("no metadata?")
			return
		m = tuple(m)
		obj = self._cache.get(m,None)
		if obj is None:
			logger.warn("metadata not found")
			return
		obsolete = set()

		# TODO: This loop is inefficient 
		for ks,s in obj.searches.items():
			if s.kw:
				for i,v in k.items():
					sv = s.kw.get(i,None)
					if sv is None or sv not in v:
						continue
					break
				else:
					continue
			obsolete.add(ks)
		for ks in obsolete:
			logger.debug("dropping %s",ks)
			obj.searches.pop(ks,None)

	@property
	def root(self):
		"""Get the object root. This may or may not be a cacheable object."""
		rk = self.root_key
		if rk is not None:
			if isinstance(rk,AsyncResult):
				return rk.get(timeout=RETR_TIMEOUT)
			return self.get(self.root_key)

		self.root_key = rk = AsyncResult()
		try:
			obj = self._send("root")
		except Exception as e:
			self.root_key = None
			rk.set_exception(e)
			raise
		else:
			self.root_key = getattr(obj,"_key",None)
			if self.root_key is not None:
				self._add_to_cache(obj)
			rk.set(obj)
			return obj

	def _send(self, action, msg=None, **kw):
		"""Low-level method for RPCing the server"""
		logger.debug("send dec %s %r",action,msg)
		kw['_m'] = msg
		kw['_a'] = action
		msg = self.codec.encode(kw)
		logger.debug("send raw %r",msg)

		msg = self.server(msg)

		logger.debug("recv raw %r",msg)
		msg = self.codec.decode(msg)
		logger.debug("recv dec %r",msg)

		if 'error' in msg:
			raise ServerError(msg['error'],msg.get('tb',None))
		return msg['res']

	def _recv(self, msg):
		"""Process incoming notifications from the server"""
		logger.debug("bcast raw %r",msg)
		msg = self.codec.decode(msg)
		logger.debug("bcast dec %r",msg)
		job = msg.pop('_a')
		m = msg.pop('_m',msg)

		try:
			proc = getattr(self,'do_'+job)
		except AttributeError:
			raise UnknownCommandError(job)
		proc(m,**msg)

client = None
