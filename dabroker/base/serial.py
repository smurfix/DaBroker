#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function,absolute_import
import sys
from time import mktime
from ..util import TZ,UTC, format_dt
import datetime as dt

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

type2cls = SupD() # 
name2cls = {} # decoder: 
def serial_adapter(cls):
	"""A decorator for an adapter class which translates serializer to whatever."""
	type2cls[cls.cls.__module__+"."+cls.cls.__name__] = cls
	name2cls[cls.clsname] = cls
	return cls

@serial_adapter
class _datetime(object):
	cls = dt.datetime
	clsname = "datetime"

	@staticmethod
	def encode(obj):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":mktime(obj.timetuple()),"s":format_dt(obj)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).replace(tzinfo=UTC).astimezone(TZ)
		else: ## historic
			assert a
			return dt.datetime(*a).replace(tzinfo=TZ)

@serial_adapter
class _timedelta(object):
	cls = dt.timedelta
	clsname = "timedelta"

	@staticmethod
	def encode(obj):
		## the string is purely for human consumption and therefore does not have a time zone
		return {"t":obj.total_seconds(),"s":str(obj)}

	@staticmethod
	def decode(t,s=None,**_):
		return dt.timedelta(0,t)

@serial_adapter
class _date(object):
	cls = dt.date
	clsname = "date"

	@staticmethod
	def encode(obj):
		return {"d":obj.toordinal(), "s":obj.strftime("%Y-%m-%d")}

	@staticmethod
	def decode(d=None,s=None,a=None,**_):
		if d:
			return dt.date.fromordinal(d)
		## historic
		return dt.date(*a)

@serial_adapter
class _time(object):
	cls = dt.time
	clsname = "time"

	@staticmethod
	def encode(obj):
		ou = obj.replace(tzinfo=UTC)
		secs = ou.hour*3600+ou.minute*60+ou.second
		return {"t":secs,"s":"%02d:%02d:%02d" % (ou.hour,ou.minute,ou.second)}

	@staticmethod
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).time()
		return dt.time(*a)

scalar_types = {type(None),float}
from six import string_types,integer_types
for s in string_types+integer_types: scalar_types.add(s)
scalar_types = tuple(scalar_types)

class Encoder(object):
	def __init__(self):
		self.objcache = {}

	def encode(self, data):
		if isinstance(data,scalar_types):
			return data

		oid = self.objcache.get(id(data),None)
		if oid is not None:
			return {'_or':oid}
		oid = 1+len(self.objcache)
		self.objcache[id(data)] = oid
		
		if isinstance(data,(list,tuple)):
			return type(data)(self.encode(x) for x in data)
		if isinstance(data,dict):
			res = type(data)()
			for k,v in data.items():
				res[k] = self.encode(v)
			assert '_o' not in res, res
			assert '_oi' not in res, res
			if res:
				res['_oi'] = oid
			return res

		obj = type2cls.get(type(data),None)
		if obj is not None:
			data = obj.encode(data)
			data['_o'] = obj.clsname
			data['_oi'] = oid
			return data
		raise NotImplementedError("I don't know how to encode %r"%(data,))

def encode(data):
	return Encoder().encode(data)

class Decoder(object):
	def __init__(self):
		self.objcache = {}
		self.objtodo = []

	def _decode(self,data, p=None,off=None):
		if isinstance(data, scalar_types):
			return data

		if isinstance(data,(list,tuple)):
			k = 0
			res = []
			for v in data:
				v = self._decode(v,res,k)
				k += 1
			return res

		if isinstance(data,dict):
			res = {}
			for k,v in data.items():
				res[k] = self._decode(v,res,k)
			
			oid = res.pop('_oi',None)
			obj = res.pop('_o',None)
			if obj is not None:
				res = name2cls[obj].decode(**res)
			if oid is not None:
				self.objcache[oid] = res
			if obj is None and len(data) == 1:
				oref = data.pop('_or',None)
				if oref is not None:
					assert p is not None
					self.objtodo.append((oref,p,off))
			return res

		raise NotImplementedError("Don't know how to decode %r"%data)
	
	def _cleanup(self):
		for d,p,k in self.objtodo:
			p[k] = self.objcache[d]
		
	def decode(self, data):
		res = self._decode(data)
		self._cleanup()
		return res

	
def decode(data):
	d = Decoder()
	return d.decode(data)

