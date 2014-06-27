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
	def __init__(self,*a,**k):
		super(ClientBrokeredInfo,self).__init__(*a,**k)
		self.searches = WeakValueDictionary()

	@property
	def class_(self):
		if self._class is not None:
			return self._class

		class ClientObj(ClientBaseObj):
			name = None
			def __repr__(self):
				res = "<ClientObj"
				n = self.__class__.__name__
				if n != "ClientObj":
					res += ":"+n
				n = getattr(self,'_key',None)
				if n is not None:
					res += ":"+" ".join(str(x) for x in n)
				res += ">"
				return res
			__str__=__repr__

		if not self._key:
			# singleton
			_ClientObj = ClientObj
			class ClientObj(_ClientObj,ClientBrokeredInfo):
				def __init__(self):
					_ClientObj.__init__(self)
					ClientBrokeredInfo.__init__(self)
				def __repr__(self):
					res = "<ClientInfo"
					n = getattr(self,'name',None)
					if n is not None:
						res += ":"+n
					res += ">"
					return res
				__str__=__repr__

		self._class = ClientObj
		for k in self.refs.keys():
			setattr(ClientObj,k,load_related(k))
		for k in self.calls.keys():
			setattr(ClientObj,k,call_proc(k))
		ClientObj.__name__ = str(self.name or "unknownClientType")
		return ClientObj

	def find(self, **kw):
		from . import service as s
		return s.client.find(self,**kw)
		
	def get(self, **kw):
		res = self.find(_limit = 2, **kw)
		if len(res) == 0:
			raise NoData
		elif len(res) == 2:
			raise ManyData
		else:
			return res[0]

class ClientBrokeredInfoInfo(ClientBrokeredInfo,BrokeredInfoInfo):
	pass
client_broker_info_meta = ClientBrokeredInfoInfo()

class load_related(object):
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		k = obj._refs.get(self.name,None)
		if k is None:
			return None
		from . import service as s
		return s.client.get(k)

class call_proc(object):
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		def c(*a,**k):
			from . import service as s
			return s.client.call(obj,self.name, a,k)
		c.__name__ = str(self.name)
		return c

class ClientBaseObj(BaseObj):
	def __init__(self):
		self._refs = {}
	
	def _attr_key(self,k):
		return self._refs[k]

@serial_adapter
class client_baseRef(common_BaseRef):
	@staticmethod
	def decode(loader, k):
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
		if ref is not None:
			ref = BaseRef(obj._meta,obj._key)
		return ref
	

	@staticmethod
	def decode(loader, k,f=None,r=None,meta=None):
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
				res._refs[k] = v._key

		return res

@serial_adapter
class client_InfoObj(client_BaseObj):
	cls = ClientBrokeredInfo
	clsname = "Info"
		
	@staticmethod
	def decode(loader, k=None,f=None):
		res = client_BaseObj.decode(loader, k=k,f=f,meta=client_broker_info_meta)
		return res

