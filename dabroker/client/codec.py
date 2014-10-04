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
from ..base import BaseRef,BaseObj, BrokeredInfo, BrokeredInfoInfo, adapters as baseAdapters, common_BaseObj,common_BaseRef, NoData,ManyData
from ..base.service import current_service
from blinker._utilities import hashable_identity, reference, WeakTypes

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

def search_key(a,kw):
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

class ClientBaseObj(BaseObj):
	"""base for all DaBroker-controlled objects on the client."""
	_obsolete = False

	def __init__(self):
		super(ClientBaseObj,self).__init__()
		self._refs = {}
	
	def _attr_key(self,k):
		return self._refs[k]
	
	def _obsoleted(self):
		"""Called from the server to mark this object as changed or deleted.
			To determine which, try to fetch the new version via `self._key()`."""
		self._obsolete = True

_ref_cache = WeakValueDictionary()
class ClientBaseRef(BaseRef):
	"""DaBroker-controlled references to objects on the client."""
	def __new__(cls, key=None, meta=None, code=None):
		assert isinstance(key,tuple),key
		self = _ref_cache.get(key,None)
		if self is not None:
			return self
		return super(ClientBaseRef,cls).__new__(cls)
	def __init__(self, key=None, meta=None, code=None):
		if key in _ref_cache: return
		super(ClientBaseRef,self).__init__(key=key,meta=meta,code=code)
		self._dab = current_service.top
		_ref_cache[key] = self

	def __call__(self):
		return self._dab.get(self)

	### signalling stuff

	# inspired by blinker.base.signal(), except that I don't have a sender.
	# Instead I have a mandatory positional `signal` argument that's sent
	# to receivers

	def connect(self, receiver, weak=True):
		"""\
			Connect a signal receiver to me.

			`proc` will be called with two positional arguments: the
			destination object and the signal that's transmitted. Whatever
			keywords args the sender set in its .send() call are passed
			as-is.
			"""
		if not hasattr(self,'_receivers'):
			self._receivers = dict()

		receiver_id = hashable_identity(receiver)
		if weak:
			receiver_ref = reference(receiver, self._cleanup_receiver)
			receiver_ref.receiver_id = receiver_id
		else:
			receiver_ref = receiver
		self._receivers.setdefault(receiver_id, receiver_ref)
	
	def _send(self, sig, **k):
		"""Send a signal."""
		receivers = getattr(self,'_receivers',None)
		if receivers is None:
			return
		disc = []
		for id,r in receivers.items():
			if r is None:
				continue
			if isinstance(r, WeakTypes):
				r = r()
				if r is None:
					disc.append(id)
					continue
			try:
				r(self,sig,**k)
			except Exception:
				logger.exception("Signal error: %s %s",repr(sig),repr(k))
		for id in disc:
			self._disconnect(id)

	def disconnect(self, receiver):
		"""Disconnect *receiver* from this signal's events.

		:param receiver: a previously :meth:`connected<connect>` callable
		"""
		receiver_id = hashable_identity(receiver)
		self._disconnect(receiver_id)

	def _disconnect(self, receiver_id):
		self._receivers.pop(receiver_id, None)
		if not self._receivers:
			del self._receivers

	def _cleanup_receiver(self, receiver_ref):
		"""Disconnect a receiver from all senders."""
		self._disconnect(receiver_ref.receiver_id, ANY_ID)

class _ClientData(ClientBaseObj):
	"""Mix-in class for remote objects"""
	_key = None

	def __init__(self,*a,**k):
		self._call_cache = WeakValueDictionary()
		super(_ClientData,self).__init__(*a,**k)
	def __repr__(self):
		res = "<ClientData"
		n = self.__class__.__name__
		if n not in ("ClientObj","ClientData"):
			res += ":"+n
		n = self._key
		if n is not None:
			res += ":"+str(n)
		res += ">"
		return res
	__str__=__repr__

class ClientBrokeredInfo(BrokeredInfo):
	"""\
		This is the base class for client-side meta objects.
		"""
	def __init__(self,*a,**k):
		super(ClientBrokeredInfo,self).__init__(*a,**k)
		self.searches = WeakValueDictionary()
		self._class = [None,None]

	def class_(self,is_meta):
		"""\
			Determine which class to use for objects with this as metaclass
			"""
		cls = self._class[is_meta]
		if cls is not None:
			return cls
		k = self._key.key
		cls = _registry.get(k,None)
		if cls is None:
			# Allow a single wildcard at the end
			cls = _registry.get((k[:-1])+(None,),object)

		if is_meta:
			class ClientInfo(_ClientInfo,cls):
				pass
			cls = ClientInfo

			for k in self.refs.keys():
				if k != '_meta':
					setattr(cls, '_dab_'+k if hasattr(cls,k) else k,handle_related(k))
		else:
			class ClientData(_ClientData,cls):
				pass
			cls = ClientData

			for k in self.fields.keys():
				if not hasattr(cls,k):
					setattr(cls, '_dab_'+k if hasattr(cls,k) else k,handle_data(k))
			for k in self.refs.keys():
				if k != '_meta' and not hasattr(cls,k):
					setattr(cls, '_dab_'+k if hasattr(cls,k) else k,handle_ref(k))
			for k,v in self.backrefs.items():
				setattr(cls, '_dab_'+k if hasattr(cls,k) else k,handle_backref(k,v))

		for k,v in self.calls.items():
			if not hasattr(cls,k):
				setattr(cls,k,call_proc(v))

		self._class[is_meta] = cls
		return cls

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

class _ClientInfo(_ClientData,ClientBrokeredInfo):
	"""Mix-in class for meta objects"""
	_name = None
	def __init__(self,*a,**k):
		super(_ClientInfo,self).__init__(*a,**k)
	def __repr__(self):
		res = "<ClientInfo"
		n = self.name
		if n is not None:
			res += ":"+n
		else:
			n = self._key
			if n is not None:
				res += ":"+str(n)
		res += ">"
		return res
	__str__=__repr__

class ClientBrokeredInfoInfo(ClientBrokeredInfo,BrokeredInfoInfo):
	"""\
		This is the client-side singleton meta-metaclass
		(the root of DaBroker's object system)
		"""
	pass
client_broker_info_meta = ClientBrokeredInfoInfo()

class handle_data(object):
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
			obj._meta._dab.obj_change(obj, self.name, ov,val)

class handle_related(object):
	"""This property accessor handles retrieving referred objects from cache, or the server"""
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		k = obj._refs.get(self.name,None)
		if k is None:
			return None
		return obj._meta._dab.get(k)

class handle_ref(handle_related):
	"""This property accessor handles updating referential attributes"""
	def __set__(self, obj, val):
		ov = obj._refs.get(self.name,_NotGiven)
		if val is not None:
			val = val._key
		obj._refs[self.name] = val
		if ov is _NotGiven:
			return
		obj._meta._dab.obj_change(obj, self.name, ov,val)

class handle_backref(object):
	"""This property accessor handles retrieving one-to-many relationships"""
	def __init__(self, name,refobj):
		self.name = name
		self.ref = ref(refobj)

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		k = obj._refs.get(self.name,None)
		if k is None:
			k = obj._refs[self.name] = k = backref_handler(obj, self.name,self.ref)
		return k

class backref_handler(object):
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

class call_proc(object):
	"""This property accessor returns a shim which executes a RPC to the server."""
	def __init__(self, proc):
		self.name = proc.name
		self.cached = getattr(proc,'cached',False)
		self.meta = getattr(proc,'meta',False)

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		def c(*a,**k):
			with obj._dab.env:
				if self.cached and not obj._obsolete:
					kws = self.name+':'+search_key(a,k)
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
			Encode a reference, without loading the actual object – which
			would be a Bad Idea.
			"""
		ref = obj._refs[k]
		if ref is not None:
			ref = ClientBaseRef(obj._meta,obj._key)
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
		res = m.class_(_is_meta)()
		res._key = k

		# Got the class, now fill it with data
		if f:
			for k,v in f.items():
				setattr(res,k,v)
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


