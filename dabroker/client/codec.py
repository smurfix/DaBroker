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

class _NotGiven: pass

# This is the client's storage side.

adapters = baseAdapters[:]

def codec_adapter(cls):
	adapters.append(cls)
	return cls

class ClientBrokeredInfo(BrokeredInfo):
	def __init__(self,*a,**k):
		super(ClientBrokeredInfo,self).__init__(*a,**k)
		self.searches = WeakValueDictionary()
		self._class = [None,None]

	def class_(self,is_meta):
		cls = self._class[is_meta]
		if cls is not None:
			return cls
		class ClientObj(ClientBaseObj):
			name = None
			def __repr__(self):
				res = "<ClientObj"
				n = self.__class__.__name__
				if n != "ClientObj":
					res += ":"+n
				n = getattr(self,'_key',None)
				if n is not None:
					res += ":"+str(n)
				res += ">"
				return res
			__str__=__repr__

		if is_meta:
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
					else:
						n = getattr(self,'_key',None)
						if n is not None:
							res += ":"+str(n)
					res += ">"
					return res
				__str__=__repr__

		self._class[is_meta] = ClientObj
		if is_meta:
			for k in self.refs.keys():
				if k != '_meta':
					setattr(ClientObj,k,handle_related(k))
		else:
			for k in self.fields.keys():
				setattr(ClientObj,k,handle_data(k))
			for k in self.refs.keys():
				if k != '_meta':
					setattr(ClientObj,k,handle_ref(k))
		for k in self.calls.keys():
			setattr(ClientObj,k,call_proc(k))
		return ClientObj

	def find(self, **kw):
		return self.client.find(self,**kw)
		
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

class handle_data(object):
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
	def __set__(self, obj, val):
		ov = obj._refs.get(self.name,_NotGiven)
		if val is not None:
			val = val._key
		obj._refs[self.name] = val
		if ov is _NotGiven:
			return
		obj._meta._dab.obj_change(obj, self.name, ov,val)

class call_proc(object):
	def __init__(self, name):
		self.name = name

	def __get__(self, obj, type=None):
		if obj is None:
			return self

		def c(*a,**k):
			return obj._meta._dab.call(obj,self.name, a,k)
		c.__name__ = str(self.name)
		return c

class ClientBaseObj(BaseObj):
	def __init__(self):
		self._refs = {}
	
	def _attr_key(self,k):
		return self._refs[k]

@codec_adapter
class client_baseRef(common_BaseRef):
	@staticmethod
	def decode(loader, k,c=None):
		return BaseRef(key=tuple(k),code=c)

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
			ref = BaseRef(obj._meta,obj._key)
		return ref
	

	@classmethod
	def decode(cls, loader, k,c=None,f=None,r=None,meta=None, _is_meta=None):
		"""\
			Decode a reference.
			"""
		k = BaseRef(key=k,code=c)
		if meta is not None:
			res = meta.class_(_is_meta if _is_meta is not None else issubclass(cls.cls,BrokeredInfo))
		elif r and '_meta' in r:
			r['_meta'] = meta = loader.get(r['_meta'])
			res = meta.class_(_is_meta if _is_meta is not None else issubclass(cls.cls,BrokeredInfo))
		else:
			#raise RuntimeError("no meta info: untested")
			import pdb;pdb.set_trace()
			res = ClientBaseObj

		res = res()
		res._key = k

		if f:
			for k,v in f.items():
				setattr(res,k,v)
		if r:
			for k,v in r.items():
				if k == '_meta':
					res._meta = v
				else:
					res._refs[k] = v

		return loader._add_to_cache(res)

@codec_adapter
class client_InfoObj(client_BaseObj):
	cls = ClientBrokeredInfo
	clsname = "Info"
		
	@staticmethod
	def decode(loader, k=None,c=None,f=None, **kw):
		if f is None:
			# We always need the data, but this is something like a ref
			# so we need to go and get the real thing.
			# NOTE this assumes that the codec doesn't throw away empty lists.
			return loader.get(BaseRef(key=k,code=c))
		res = client_BaseObj.decode(loader, _is_meta=True, k=k,c=c,f=f,**kw)
		res.client = loader
		return res

