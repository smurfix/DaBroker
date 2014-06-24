# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including an optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

from weakref import ref
from ..base import BaseRef,BaseObj, BrokeredInfo, BrokeredInfoInfo, adapters as baseAdapters, common_BaseObj,common_BaseRef
import logging
logger = logging.getLogger("dabroker.client.serial")

# This is the client's storage side.

adapters = baseAdapters[:]

def serial_adapter(cls):
	adapters.append(cls)
	return cls

class ClientBrokeredInfo(BrokeredInfo):
	_class = None
	@property
	def class_(self):
		if self._class is not None:
			return self._class

		class loadedObj(ClientBaseObj,ClientBrokeredInfo):
			name = None
			def __init__(self):
				ClientBaseObj.__init__(self)
				ClientBrokeredInfo.__init__(self)

			def __repr__(self):
				if self.name:
					return "<ClientBrokeredInfo:%s>"%(self.name,)
				else:
					return "<ClientBrokeredInfo>"
			__str__=__repr__

		self._class = loadedObj
		for k in self.refs.keys():
			setattr(loadedObj,k,load_related(k))
		loadedObj.__name__ = str(self.name or "unknownClientType")
		return loadedObj

class ClientBrokeredInfoInfo(ClientBrokeredInfo,BrokeredInfoInfo):
	pass
client_broker_info_meta = ClientBrokeredInfoInfo()

class load_related(object):
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		kv = obj._refs.get(self.name,None)
		if kv is None:
			 obj._refs[self.name] = kv = [None,None]
		k,v = kv
		if k is None:
			return None
		if v is not None:
			v = v()
		if v is None:
			from . import service as s
			v = s.client.get(k)
			kv[1] = ref(v)
		return v

class ClientBaseObj(BaseObj):
	def __init__(self):
		self._refs = {}

@serial_adapter
class client_baseRef(common_BaseRef):
	@staticmethod
	def decode(k):
		return BaseRef(key=tuple(k))

@serial_adapter
class client_BaseObj(common_BaseObj):

	@classmethod
	def encode_ref(obj,k):
		"""\
			Encode a reference, without loading the actual object – which
			would be a Bad Idea.
			"""
		ref = obj._refs[k]
		ref = getattr(obj,k)
		if ref is not None:
			ref = BaseRef(obj._meta,obj._key)
		return ref
	

	@staticmethod
	def decode(k,f=None,r=None,meta=None):
		"""\
			Decode a reference.
			"""
		k = tuple(k)
		if meta is not None:
			res = meta.class_
		elif r and '_meta' in r and hasattr(r['_meta'],'_key'):
			from . import service as s
			res = s.client.get(r['_meta']._key).class_
		else:
			res = ClientBaseObj

		res = res()
		res._key = tuple(k)

		if f:
			for k,v in f.items():
				setattr(res,k,v)
		if r:
			for k,v in r.items():
				res._refs[k] = [v._key,None]

		return res

@serial_adapter
class client_InfoObj(client_BaseObj):
	cls = ClientBrokeredInfo
	clsname = "Info"
		
	@staticmethod
	def decode(k=None,f=None):
		res = client_BaseObj.decode(k=k,f=f,meta=client_broker_info_meta)
		return res

