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

import sys
from time import mktime
from ...util import TZ,UTC, format_dt,pformat
from ..config import default_config
from .. import NoData,ManyData
import datetime as dt
from collections import namedtuple
from dabroker.util import attrdict,pformat

from traceback import format_tb
import logging
logger = logging.getLogger("dabroker.base.codec")

class _notGiven: pass
class ComplexObjectError(Exception): pass

DecodeRef = namedtuple('DecodeRef',('oid','parent','offset', 'cache'))
# This is used to store references to to-be-amended objects.
# The idea is that if a newly-decoded object encounters this, it can
# replace the offending reference by looking up the result in the cache.
# 
# Currently, this type only provides a hint about the problem's origin if
# it is ever encountered outside the decoding process. The problem does not
# occur in actual code because most objects are just transmitted as
# references, to be evaluated later (when an attribute referring to the
# object is accessed).

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
	def decode(t=None,s=None,a=None,k=None,**_):
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
	def decode(t,s=None,**_):
		return dt.timedelta(0,t)

@codec_adapter
class _date(object):
	cls = dt.date
	clsname = "date"

	@staticmethod
	def encode(obj, include=False):
		return {"d":obj.toordinal(), "s":obj.strftime("%Y-%m-%d")}

	@staticmethod
	def decode(d=None,s=None,a=None,**_):
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
	def decode(t=None,s=None,a=None,k=None,**_):
		if t:
			return dt.datetime.utcfromtimestamp(t).time()
		return dt.time(*a)

@codec_adapter
class _attrdict(object):
	cls = attrdict
	clsname = "adict"
	include = True

	@staticmethod
	def encode(obj, include=False):
		return dict(**obj)

	@staticmethod
	def decode(**k):
		return attrdict(**k)

### Error handling / forwarding

class ServerError(Exception):
	name = "ServerError"
	_traceback = None
	_repr = None

	def __repr__(self):
		return self._repr

	def __str__(self):
		r = self.__repr__()
		if self._traceback is None: return r
		return r+"\n"+"".join(self._traceback)

known_errors = {
	"NoData":NoData,
	"ManyData":ManyData,
	"IndexError":IndexError, # required for backref_idx call
}

@codec_adapter
class _exc(object):
	cls = Exception
	clsname = "exc"

	@staticmethod
	def encode(obj, include=False):
		res = dict((k,repr(v)) for k,v in obj.__dict__.items())
		res['_t'] = obj.__class__.__name__
		res['_r'] = repr(obj)
		return res

	@staticmethod
	def decode(**k):
		t = k.pop('_t',"Exception")
		r = k.pop('_r',"??")

		e = known_errors.get(t,None)
		if e is None:
			if t.endswith("Error") or t.endswith("Exception") or t.endswith("Warning"):
				e = getattr(__builtins__,t,None)
		if e is None:
			e = Exception

		class _Error(ServerError,e):
			pass
		_Error.__name__ = str(t)

		if e is not None:	
			t = None

		err = _Error()
		err.__dict__.update(k)
		err.name = t
		err._repr = r
		return err

scalar_types = {type(None),float,bytes}
from six import string_types,integer_types
for s in string_types+integer_types: scalar_types.add(s)
scalar_types = tuple(scalar_types)

class BaseCodec(object):
	"""\
		Serialize my object structure to something dict/list-based and
		non-self-referential, suitable for JSON/BSON/XML/whatever-ization.

		@loader is something with a .get method. The resolving code will
		call that with a key if it needs to refer to an object.

		@adapters is a list of additional adapters which are to be
		registered.
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
		"""\
			Register more adapters.
			"""
		if isinstance(cls,(list,tuple)):
			for c in cls:
				self.register(c)
			return
		if cls.cls is not None:
			self.type2cls[cls.cls.__module__+"."+cls.cls.__name__] = cls
		if cls.clsname is not None:
			self.name2cls[cls.clsname] = cls
		
	def _encode(self, data, objcache,objref, include=False, p=None,off=None):
		# @objcache: dict: id(obj) => (seqnum,encoded,selfref,data)
		#            `encoded` will be set to the encoded object so that
		#            the seqnum can be removed later if it turns out not to
		#            be needed.
		#            `selfref` is None (incomplete), False (recursive),
		#            or an (id,parent,position) tuple used to fix-up a prior
		#            reference to this object when decoding.
		#            `data` is the original data. We need to keep it around
		#            because if there is no other reference to it, it'll be
		#            freed, causing the id to be re-used. Not good.
		# 
		# @objref: dict: seqnum => oid: objects which are actually required for proper
		#          encoding.

		# @p,@off: p[off] == data. Required for fixing cyclic references.
		
		# If this is a Werkzeug localproxy, dereference it
		ac = getattr(data,'_get_current_object',None)
		if ac is not None:
			data = ac()

		# Scalars (integers, strings) do not refer to other objects and
		# thus are never encoded.
		if isinstance(data,scalar_types):
			return data

		# Have I seen this object before?
		did = id(data)
		oid = objcache.get(did,None)
		if oid is not None:
			# Yes.
			if oid[1] is None: # it's incomplete: mark as recursive structure.
				oid[2] = False

			# Point to it.
			oid = oid[0]
			objref[oid] = did
			return {'_or':oid}

		# No, this is a new object: Generate a new ID for it.
		oid = 1+len(objcache)
		objcache[did] = [oid,None,None,data]
		# we need to keep the data around, see above
		
		if isinstance(data,(list,tuple)):
			# A list will keep its "include" state
			res = []
			i = 0
			for x in data:
				res.append(self._encode(x,objcache,objref,include, p=res,off=i))
				i += 1

			res = { '_o':'LIST','_d':res }

		else:
			odata = data
			inc = 2
			# Marker to use the field key: include 'f' values (data fields)

			if type(data) is not dict:
				obj = self.type2cls.get(type(data),None)
				if obj is None:
					raise NotImplementedError("I don't know how to encode %s: %r"%(repr(data.__class__),data,))
				inc = getattr(obj,"include",inc)
				data = obj.encode(data, include=include)
				if isinstance(data,tuple):
					obj,data = data
				else:
					obj = obj.clsname
			else:
				obj = None
				inc = True
			if not include:
				# override with existing None/False values
				inc = include

			res = type(data)()
			for k,v in data.items():
				# Transparent encoding: _ofoo => _o_foo, undone in the decoder
				# so that our _o and _oi values don't clash with whatever
				nk = '_o_'+k[2:] if k.startswith('_o') else k

				# inc==2: include regular data values, but not refs or whatever
				res[nk] = self._encode(v,objcache,objref, include=((k == "f") if (inc == 2) else inc), p=res,off=k)

			if obj is not None:
				res['_o'] = obj

		did = objcache[did]
		did[1] = res
		if did[2] is None:
			# order non-recursive objects by completion time.
			# Need to mangle the offset
			d = objcache['done']
			did[2] = (d,p,('_o_'+off[2:] if isinstance(off,string_types) and off.startswith('_o') else off))
			objcache['done'] = d+1
		return res

	def encode(self, data, _include=False, _raw=False, **kw):
		"""\
			Encode this data structure. Recursive structures or
			multiply-used objects are handled mostly-correctly.

			@_include: a flag telling the system to encode an object's data,
			           not just a reference. Used server>client. If None,
			           send object keys without retrieval info. This is used
			           e.g. when broadcasting, so as to not leak data access.
			@_raw: if true, skip bundling up the result. Used for encodding
			       errors.
			"""
		# No, not yet / did not work: slower path
		objcache = {"done":1}
		objref = {}
		res = self._encode(data, objcache,objref, include=_include)
		del objcache['done']
		cache = []

		if objref:
			# At least one reference was required.
			def _sorter(k):
				c,d,e,x = objcache[k]
				if type(e) is not tuple: return 9999999999
				return e[0]

			for d in sorted(objref.values(), key=_sorter):
				oid,v,f,x = objcache[d]
				# add object IDs only to those objects which need it
				v['_oi']=oid
				if isinstance(f,tuple):
					# Referenced. Add to cache and replace with fix-up.
					f[1][f[2]] = {'_or':oid}
					cache.append(v)
		if _raw:
			assert not kw, kw
			return res,cache

		res = {'data':res}
		res.update(kw)
		if cache:
			res['cache'] = cache
		return res
	
	def encode_error(self, err, tb=None):
		"""\
			Special method for encoding an error, with optional traceback.

			Note that this will not pass through the normal encoder, so the
			data should be strings (or, in case of the traceback, a list of
			strings).
			"""
		res = {}

		if isinstance(err,string_types):
			err = Exception(err)
		# don't use the normal 
		res['error'],cache = BaseCodec.encode(self,err, _raw=True)
		if cache:
			res['cache'] = cache

		if tb is not None:
			if hasattr(tb,'tb_frame'):
				tb = format_tb(tb)
			res['tb'] = tb
		return res

	def _decode(self,data, objcache,objtodo, p=None,off=None):
		# Decode the data recursively.
		#
		# @objcache: dict seqnum=>result
		# 
		# @objtodo: Fixup data, list of (seqnum,parent,index). See below.
		#
		# @p, @off: parent object and index which refer to this object.
		#
		# During decoding, information to recover an object may not be
		# available, i.e. we encounter an object reference while decoding
		# the data it refers to.
		# The @objtodo array records where the actual result is supposed to
		# be stored, as soon as we have it.
		# 
		# This process does not work with recursive object references
		# within other objects. That'd require a more expensive/intrusive
		# decoding framework. TODO: Detect this case.

		if isinstance(data, scalar_types):
			return data

		# "Unmolested" lists are passed through.
		if isinstance(data,(list,tuple)):
			return type(data)(self._decode(v,objcache,objtodo) for v in data)

		if type(data) is dict:
			oid = data.pop('_oi',None)
			obj = data.pop('_o',None)
			objref = data.pop('_or',None)
			if objref is not None:
				res = objcache.get(objref,None)
				if res is None:
					# Save fixing the problem for later
					res = DecodeRef(objref,p,off, objcache)
					objtodo.append(res)
				return res

			if obj == 'LIST':
				res = []
				if oid is not None:
					objcache[oid] = res
				k = 0
				for v in data['_d']:
					res.append(self._decode(v,objcache,objtodo, res,k))
					k += 1
				return res
			
			res = {}
			for k,v in data.items():
				if k.startswith("_o"):
					assert k[2] == '_',nk # unknown meta key?
					nk = '_o'+k[3:]
				else:
					nk = k
				res[nk] = self._decode(v,objcache,objtodo, res,k)

			if obj is not None:
				try:
					res = self.name2cls[obj].decode(**res)
				except Exception:
					logger.error("Decoding: %s:\n%s\n%s",obj,pformat(data), pformat(res))
					logger.error("Decoding:: %r",self.name2cls[obj])
					raise
			if oid is not None:
				objcache[oid] = res
			return res

		raise NotImplementedError("Don't know how to decode %r"%data)
	
	def _cleanup(self, objcache,objtodo):
		# resolve the "todo" stuff
		for d,p,k,_ in objtodo:
			p[k] = objcache[d]
		
	def decode(self, data):
		"""\
			Decode the data:
			Reverse everything the encoder does as cleanly as possible.

			Step 1: unpack and return a top-level object.
			"""

		assert type(data) is dict
		if 'error' in data:
			real_error = self.decode2(data['error'], _cache=data.get('cache',()))
			tb = data.get('tb',None)
			if tb:
				real_error._traceback = tb
			raise real_error
		return attrdict(data)

	def decode2(self,data,_cache=None):
		"""\
			Decode the data:
			Reverse everything the encoder does as cleanly as possible.

			Step 2: re-object-ify the message contents.
			"""
		objcache = {}
		objtodo = []

		if _cache is None:
			cache = data.pop('cache',())
			data = data.data
		else:
			cache = _cache

		for obj in cache:
			self._decode(obj, objcache,objtodo)
			# side effect: populate objcache

		res = self._decode(data, objcache,objtodo)
		self._cleanup(objcache,objtodo)
		if isinstance(res,DecodeRef):
			res = objcache[res.oid]
		return res

