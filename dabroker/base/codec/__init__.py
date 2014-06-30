#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import
import sys
from time import mktime
from ...util import TZ,UTC, format_dt
from ..config import default_config
import datetime as dt

from traceback import format_tb

class _notGiven: pass

class SupD(dict):
	"""A dictionary which finds classes"""
	def get(self,k,default=_notGiven):
		"""Look up type K according to the name of its class, or its closest constituent"""
		if hasattr(k,"__mro__"):
			for x in k.__mro__:
				try:
					return self.__getitem__(x.__module__+"."+x.__name__)
				except KeyError:
					pass
		if default is _notGiven:
			raise KeyError(k)
		return default

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
		return r+"\n"+"".join(self.tb)

_basics = []
def codec_adapter(cls):
	"""A decorator for an adapter class which translates serializer to whatever."""
	_basics.append(cls)
	return cls

@codec_adapter
class _datetime(object):
	cls = dt.datetime
	clsname = "datetime"

	@staticmethod
	def encode(obj, include=False):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":mktime(obj.timetuple()),"s":format_dt(obj)}

	@staticmethod
	def decode(loader, t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).replace(tzinfo=UTC).astimezone(TZ)
		else: ## historic
			assert a
			return dt.datetime(*a).replace(tzinfo=TZ)

@codec_adapter
class _timedelta(object):
	cls = dt.timedelta
	clsname = "timedelta"

	@staticmethod
	def encode(obj, include=False):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":obj.total_seconds(),"s":str(obj)}

	@staticmethod
	def decode(loader, t,s=None,**_):
		return dt.timedelta(0,t)

@codec_adapter
class _date(object):
	cls = dt.date
	clsname = "date"

	@staticmethod
	def encode(obj, include=False):
		return {"d":obj.toordinal(), "s":obj.strftime("%Y-%m-%d")}

	@staticmethod
	def decode(loader, d=None,s=None,a=None,**_):
		if d:
			return dt.date.fromordinal(d)
		## historic
		return dt.date(*a)

@codec_adapter
class _time(object):
	cls = dt.time
	clsname = "time"

	@staticmethod
	def encode(obj, include=False):
		ou = obj.replace(tzinfo=UTC)
		secs = ou.hour*3600+ou.minute*60+ou.second
		return {"t":secs,"s":"%02d:%02d:%02d" % (ou.hour,ou.minute,ou.second)}

	@staticmethod
	def decode(loader, t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).time()
		return dt.time(*a)

scalar_types = {type(None),float}
from six import string_types,integer_types
for s in string_types+integer_types: scalar_types.add(s)
scalar_types = tuple(scalar_types)

class BaseCodec(object):
	"""\
		Serialize my object structure to something dict/list-based and
		non-self-referential, suitable for JSON/BSON/XML/whatever-ization.

		@loader is something with a .get method. The resolving code will,
		call that with an object key if it needs to refer to an object.
		"""
	def __init__(self,loader,adapters=(), cfg={}):
		super(BaseCodec,self).__init__()
		self.loader = loader
		self.cfg = default_config.copy()
		self.cfg.update(cfg)
		self.type2cls = SupD() # encoder
		self.name2cls = {} # decoder 
		self.register(_basics)
		self.register(adapters)
	
	def register(self,cls):
		if isinstance(cls,(list,tuple)):
			for c in cls:
				self.register(c)
			return
		if cls.cls is not None:
			self.type2cls[cls.cls.__module__+"."+cls.cls.__name__] = cls
		if cls.clsname is not None:
			self.name2cls[cls.clsname] = cls
		
	def _encode(self, data, objcache, include=False):
		if isinstance(data,scalar_types):
			return data

		if isinstance(data,(list,tuple)):
			# A toplevel list will keep its "include" state
			return type(data)(self._encode(x,objcache,include) for x in data)

		oid = objcache.get(id(data),None)
		if oid is not None:
			return {'_or':oid}
		oid = 1+len(objcache)
		objcache[id(data)] = oid
		
		if isinstance(data,dict):
			assert '_o' not in data, data
			assert '_oi' not in data, data
		else:
			obj = self.type2cls.get(type(data),None)
			if obj is None:
				raise NotImplementedError("I don't know how to encode %r"%(data,))
			data = obj.encode(data, include=include)
			data['_o'] = obj.clsname

		res = type(data)()
		for k,v in data.items():
			res[k] = self._encode(v,objcache)
		if res:
			res['_oi'] = oid
		return res

	def encode(self, data, include=False):
		objcache = {}
		return self._encode(data, objcache, include=include)
	
	def encode_error(self, err, tb=None):
		if not hasattr(err,'swapcase'): # it's a string
			err = str(err)
		res = {'_error':err }

		if tb is not None:
			if hasattr(tb,'tb_frame'):
				tb = format_tb(tb)
			res['tb'] = tb
		return res

	def _decode(self,data, objcache,objtodo, p=None,off=None):
		if isinstance(data, scalar_types):
			return data

		if isinstance(data,(list,tuple)):
			k = 0
			res = []
			for v in data:
				res.append(self._decode(v,objcache,objtodo, res,k))
				k += 1
			return res

		if isinstance(data,dict):
			res = {}
			for k,v in data.items():
				res[k] = self._decode(v,objcache,objtodo, res,k)
			
			oid = res.pop('_oi',None)
			obj = res.pop('_o',None)
			if obj is not None:
				res = self.name2cls[obj].decode(self.loader, **res)
			if oid is not None:
				objcache[oid] = res
			if obj is None and len(data) == 1:
				oref = data.pop('_or',None)
				if oref is not None:
					assert p is not None
					objtodo.append((oref,p,off))
			return res

		raise NotImplementedError("Don't know how to decode %r"%data)
	
	def _cleanup(self, objcache,objtodo):
		for d,p,k in objtodo:
			p[k] = objcache[d]
		
	def decode(self, data):
		objcache = {}
		objtodo = []
		res = self._decode(data,objcache,objtodo)
		self._cleanup(objcache,objtodo)
		if isinstance(res,dict) and '_error' in res:
			raise ServerError(res['_error'],res.get('tb',None))
		return res

