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

class UnknownCommandError(Exception):
	def __init__(self, cmd):
		self.cmd = cmd
	def __repr__(self):
		return "{}({})".format(self.__class__.__name__,repr(self.cmd))
	def __str__(self):
		return "Unknown command: {}".format(repr(self.cmd))

def get_attrs(obj, meta=None):
	"""Return a dict with my attributes"""
	if meta is None:
		meta = obj._meta

	res = {}
	for k in meta.fields:
		res[k] = getattr(obj,k,None)
	for k in meta.refs:
		if k == "_meta": continue
		res[k] = obj._attr_key(k)
	return res

class BaseRef(object):
	"""\
		A basic (reference to an) object.
		"""
	def __init__(self, meta=None,key=None):
		if meta is not None:
			self._meta = meta
		if key is not None:
			self._key = key

class BaseObj(BaseRef):
	"""\
		This is the base object for our object storage.

		You need:
		@_meta the BrokeredInfo which describes this element's class
		@_key the data required to load this element
		"""
	def _attr_key(self,k):
		res = getattr(self,k,None)
		if res is not None:
			res = res._key
		return res

	@property
	def _attrs(self):
		return get_attrs(self)

class common_BaseRef(object):
	cls = BaseRef
	clsname = "Ref"
	
	@staticmethod
	def encode(obj, include=False):
		assert not include
		res = {"k":obj._key}
		return res

class common_BaseObj(object):
	"""Common base class for object coding; overridden in client and server"""
	cls = BaseObj
	clsname = "Obj"

	@staticmethod
	def encode_ref(obj,k):
		"""\
			Fetch a reference.

			This is overrideable so that an implementation can generate the
			key from the reference, without loading the actual object.
			"""
		ref = getattr(obj,k)
		if ref is not None:
			ref = BaseRef(ref._meta,ref._key)
		return ref

	@classmethod
	def encode(cls, obj, include=False):
		if not include:
			return common_BaseRef.encode(obj, include=False)
			
		res = {"k":obj._key}
		res['f'] = f = dict()
		for k in obj._meta.fields.keys():
			f[k] = getattr(obj,k)
		res['r'] = r = dict()
		for k in obj._meta.refs.keys():
			r[k] = cls.encode_ref(obj,k)
		return res

	# the decoder is client/server specific

class BrokeredInfo(BaseObj):
	"""\
		This class is used for metadata about Brokered objects.
		It is immutable on the client.
		"""
	name = None
	_meta = None

	def __init__(self,name=None):
		super(BrokeredInfo,self).__init__()
		self.name = name
		self.fields = dict()
		self.refs = dict()
		self.backrefs = dict()
		self.calls = dict()

		self.add(Ref("_meta"))

	def add(self, f):
		if isinstance(f,Field):
			self.fields[f.name] = f
		elif isinstance(f,Ref):
			self.refs[f.name] = f
		elif isinstance(f,BackRef):
			self.backrefs[f.name] = f
		elif isinstance(f,Callable):
			self.calls[f.name] = f
		else:
			raise RuntimeError("I don't know how to add this")

	def obj_find(self, _limit=None, **kw):
		raise NotImplementedError("Searching these objects is not implemented.")

	def __repr__(self):
		return "{}({})".format(self.__class__.__name__,repr(self.name))

class _attr(object):
	def __init__(self,name, **kw):
		self.name = name
		for k,v in kw.items():
			setattr(self,k,v)

class Field(_attr):
	"""A standard data field; may be a dict/list"""
	pass

class Ref(_attr):
	"""A reference to another BrokeredBase object"""
	pass

class BackRef(_attr):
	"""A reference from another BrokeredBase object type"""
	pass

class Callable(_attr):
	"""A procedure that will be called on the server."""
	pass

adapters = []

def codec_adapter(cls):
	adapters.append(cls)
	return cls

class AttrAdapter(object):
	@staticmethod
	def encode(obj, include=False):
		return obj.__dict__.copy()
	@classmethod
	def decode(cls,loader,**attr):
		return cls.cls(**attr)

@codec_adapter
class FieldAdapter(AttrAdapter):
	cls = Field
	clsname = "_F"

@codec_adapter
class RefAdapter(AttrAdapter):
	cls = Ref
	clsname = "_R"

@codec_adapter
class BackrefAdapter(AttrAdapter):
	cls = BackRef
	clsname = "_B"

@codec_adapter
class CallableAdapter(AttrAdapter):
	cls = Callable
	clsname = "_C"

class BrokeredMeta(BrokeredInfo):
	class_ = BrokeredInfo
	def __init__(self,name):
		super(BrokeredMeta,self).__init__(name)
		self.add(Field("name"))
		self.add(Field("fields"))
		self.add(Field("refs"))
		self.add(Field("backrefs"))
		self.add(Field("calls"))

class BrokeredInfoInfo(BrokeredMeta):
	"""This singleton is used for metadata about BrokeredInfo objects."""
	def __init__(self):
		super(BrokeredInfoInfo,self).__init__("BrokeredInfoInfo Singleton")
		self._key = ()

broker_info_meta = BrokeredInfoInfo()
BrokeredInfo._meta = broker_info_meta

