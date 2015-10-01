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

from weakref import ref,WeakValueDictionary
from functools import partial

from . import ClientBaseRef,ClientBaseObj
from ..base import BaseRef,BaseObj, BrokeredInfo, BrokeredInfoInfo, adapters as baseAdapters, common_BaseObj,common_BaseRef, NoData,ManyData
from ..base.service import current_service

import logging
logger = logging.getLogger("dabroker.client.serial")

class _NotGiven: pass

class CacheProxy(object):
	"""Can't weakref a string, so …"""
	def __init__(self,data):
		self.data = data

def kstr(v):
	k = getattr(v,'__dict__',None)
	if k is not None:
		k = k.get('_key',None)
	if k is not None:
		return '.'.join(str(x) for x in k.key)
	else:
		return str(v)

def search_key(a,**kw):
	"""Build a reproducible string from search keywords"""
	if a is None:
		a = ()
	return ','.join(kstr(v) for v in a) + '|' + ','.join('{}:{}'.format(k, kstr(v)) for k,v in sorted(kw.items()))

# This is the client's adapter storage.
adapters = baseAdapters[:]

def codec_adapter(cls):
	adapters.append(cls)
	return cls

# This is a list of special metaclasses, by key,
_registry = {}

def baseclass_for(*k):
	"""\
		Register a base class for a specific object type.
		@k is the meta object's key tuple.

		See test11 for an example which overrides the root object.

		If your client class duplicates an attribute, it takes
		precedence: the server's value of that attribute will not be
		accessible.

		Usage:

			@baseclass_for("static","root","meta")
			class MyRoot(ClientBaseObj):
				def check_me(self):
					return "This is a client-specific class"

		You can use `None` as the last value (only), which behaves like an
		any-single value placeholder.
		"""
	def proc(fn):
		_registry[k] = fn
		return fn
	return proc

class ClientBrokeredInfo(ClientBaseObj,BrokeredInfo):
	"""\
		This is the base class for client-side meta objects.
		"""
	def __init__(self,*a,**k):
		super(ClientBrokeredInfo,self).__init__(*a,**k)
		self.searches = WeakValueDictionary()
		self._class = None

	def __call__(self, _is_meta=False, *a,**kw):
		"""\
			Return the class to use for objects with this as metaclass
			"""
		cls = self._class
		if cls is None:
			k = self._key.key
			cls = _registry.get(k,None)
			if cls is None:
				# Allow a single wildcard at the end
				cls = _registry.get((k[:-1])+(None,),object)

			if _is_meta:
				class ClientInfo(ClientBrokeredInfo,cls):
					pass
			else:
				class ClientInfo(ClientBaseObj,cls):
					pass
			cls = ClientInfo

			for k in self.fields.keys():
				setattr(cls, '_dab_'+k if hasattr(cls,k) else k,FieldProperty(k))
			for k in self.refs.keys():
				if k != '_meta':
					setattr(cls, '_dab_'+k if hasattr(cls,k) else k,RefProperty(k))
			for k,v in self.backrefs.items():
				setattr(cls, '_dab_'+k if hasattr(cls,k) else k,BackRefProperty(k,v))

			for k,v in self.calls.items():
				if not hasattr(cls,k):
					setattr(cls,k,RpcProperty(v))

			self._class = cls
		return cls(*a,**kw)

	def find(self, **kw):
		if self._dab_cached is None:
			raise RuntimeError("You cannot search "+repr(self))
		for r in self.client.find(self, _cached=self._dab_cached, **kw):
			if not isinstance(r,BaseObj):
				r = r()
			yield r

	def get(self, **kw):
		if self._dab_cached is None:
			raise RuntimeError("You cannot search "+repr(self))
		res = list(self.client.find(self, _limit=2,_cached=self._dab_cached, **kw))
		if len(res) == 0:
			raise NoData(cls=self,**kw)
		elif len(res) == 2:
			raise ManyData(cls=self,**kw)
		else:
			res = res[0]
			if not isinstance(res,BaseObj):
				res = res()
			return res

	def count(self, **kw):
		if self._dab_cached is None:
			raise RuntimeError("You cannot search "+repr(self))
		return self.client.count(self, _cached=self._dab_cached, **kw)

	def __repr__(self):
		k=getattr(self,'_key',None)
		if not k or not hasattr(self,'name'):
			return super(ClientBrokeredInfo,self).__repr__()
		return '‹I:{}:{}›'.format(self.name, '¦'.join(str(x) for x in k))
	__str__=__unicode__=__repr__
		
class _ClientInfo(ClientBrokeredInfo):
	"""Mix-in class for meta objects"""
	_name = None
	def __init__(self,*a,**k):
		super(_ClientInfo,self).__init__(*a,**k)

class ClientBrokeredInfoInfo(ClientBrokeredInfo,BrokeredInfoInfo):
	"""\
		This is the client-side singleton meta object
		(the root of DaBroker's object system)
		"""
	pass
client_broker_info_meta = ClientBrokeredInfoInfo()

class FieldProperty(object):
	"""This property accessor handles updating non-referential attributes."""

	# Note that there is no `__get__` method. It is not necessary,
	# the value is stored in the object's `__dict__`;
	# Python will get it from there.

	def __init__(self, name):
		self.name = name

	def __set__(self, obj, val):
		ov = obj.__dict__.get(self.name,_NotGiven)
		obj.__dict__[self.name] = val
		if ov is _NotGiven:
			return
		if obj._meta is None:
			assert not ov or ov == val, (self.name,ov,val)
		else:
			import pdb;pdb.set_trace()
			obj._meta._dab.obj_change(obj, self.name, ov,val)

class RefProperty(object):
	"""This property accessor handles referred objects"""
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		k = obj._refs.get(self.name,None)
		if k is None:
			return None
		return obj._meta._dab.get(k)

	def __set__(self, obj, val):
		ov = obj._refs.get(self.name,_NotGiven)
		if val is not None:
			val = val._key
		obj._refs[self.name] = val
		if ov is _NotGiven:
			return
		obj._meta._dab.obj_change(obj, self.name, ov,val)

class BackRefProperty(object):
	"""This property accessor handles retrieving one-to-many relationships"""
	def __init__(self, name,refobj):
		self.name = name
		self.ref = ref(refobj)

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		k = obj._refs.get(self.name,None)
		if k is None:
			k = obj._refs[self.name] = k = BackRefHandler(obj, self.name,self.ref)
		return k

class BackRefHandler(object):
	"""Manage a specific back reference"""
	def __init__(self, obj, name,refobj):
		self.obj = ref(obj)
		self.name = name
		self.ref = refobj

	def _deref(self):
		obj = self.obj()
		ref = self.ref()
		if obj is None or ref is None:
			raise RuntimeError("weak ref: should not have been freed")
		return obj,ref

	def __getitem__(self,i):
		obj,ref = self._deref()
		res = obj._meta._dab.send("backref_idx",obj, self.name,i)
		if isinstance(res,BaseRef):
			res = res()
		return res

	def __len__(self):
		obj,ref = self._deref()
		return obj._meta._dab.send("backref_len",obj, self.name)

class RpcProperty(object):
	"""This property accessor returns a shim which executes a RPC to the server."""
	def __init__(self, proc):
		self.name = proc.name
		self.cached = getattr(proc,'cached',False)
		self.for_class = getattr(proc,'for_class',None)
		self.meta = getattr(proc,'meta',False)

	def _do_call(self,obj, *a,**k):
		with obj._dab.env:
			if self.cached and not obj._obsolete:
				kws = self.name+':'+search_key(a,**k)
				ckey = " ".join(str(x) for x in obj._key.key)+":"+kws

				res = obj._call_cache.get(kws,_NotGiven)
				if res is not _NotGiven:
					res = res.data
					current_service.top._cache[ckey] # Lookup to increase counter
					return res
			res = obj._meta._dab.call(obj,self.name, a,k, _meta=self.meta)
			if self.cached and not obj._obsolete:
				rc = CacheProxy(res)
				obj._call_cache[kws] = rc
				current_service.top._cache[ckey] = rc
			return res

	def __get__(self, obj, type=None):
		if self.for_class is None: # normal method
			if obj is None:
				return self
		else: # static- or classmethod
			obj=type

		c = partial(RpcProperty._do_call, self,obj)
		c.__name__ = str(self.name)
		return c

@codec_adapter
class client_BaseRef(common_BaseRef):
	cls = ClientBaseRef

	@staticmethod
	def decode(k,c=None):
		return ClientBaseRef(key=tuple(k),code=c)

@codec_adapter
class client_BaseObj(common_BaseObj):

	@classmethod
	def encode_ref(obj,k):
		"""\
			Encode a reference, without loading the actual object.
			(Since we can't load the object without encoding a reference for it, that'd be somewhat difficult.)
			"""
		ref = obj._refs[k]
		if ref is not None:
			import pdb;pdb.set_trace()
			ref = ClientBaseRef(meta=obj._meta, key=obj._key)
		return ref
	

	@classmethod
	def decode(cls, k,c=None,f=None,r=None, _is_meta=False):
		"""\
			Convert this object to a class
			"""

		k = ClientBaseRef(key=tuple(k),code=c)

		if not r or '_meta' not in r:
			raise RuntimeError("Object without meta data")

		m = r['_meta']
		if not isinstance(m,ClientBrokeredInfo):
			# assume it's a reference, so resolve it
			r['_meta'] = m = m()
		res = m(_is_meta)
		res._key = k

		# Got the class, now fill it with data
		if f:
			for k,v in f.items():
				res.__dict__[k] = v
				# do not use setattr here, it tries to record a change
		if r:
			for k,v in r.items():
				if k == '_meta':
					res._meta = v
				else:
					res._refs[k] = v

		return current_service.top._add_to_cache(res)
	
@codec_adapter
class client_InfoObj(client_BaseObj):
	cls = ClientBrokeredInfo
	clsname = "Info"
		
	@staticmethod
	def decode(k=None,c=None,f=None, **kw):
		if f is None:
			# We always need the data, but this is something like a ref,
			# so we need to go and get the real thing.
			# NOTE this assumes that the codec doesn't throw away empty lists.
			return ClientBaseRef(key=k,code=c)()
		res = client_BaseObj.decode(_is_meta=True, k=k,c=c,f=f,**kw)
		res.client = current_service.top
		return res

@codec_adapter
class client_InfoMeta(object):
    cls = ClientBrokeredInfoInfo
    clsname = "_ROOT"

    @staticmethod
    def encode(obj, include=False):
        return {}

    @staticmethod
    def decode(**attr):
        return client_broker_info_meta


