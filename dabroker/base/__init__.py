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

from six import string_types,integer_types

class UnknownCommandError(Exception):
	def __init__(self, cmd):
		self.cmd = cmd
	def __repr__(self):
		return "{}({})".format(self.__class__.__name__,repr(self.cmd))
	def __str__(self):
		return "Unknown command: {}".format(repr(self.cmd))

class DataError(RuntimeError):
	def __init__(self,**kw):
		self.__dict__.update(kw)
	def __repr__(self):
		return "{}({})".format(self.__class__.__name__, ",".join("{}={}".format(k,v) for k,v in self.__dict__.items() if not k.startswith("_") and v is not None))
class NoData(DataError):
    pass
class ManyData(DataError):
    pass

def get_attrs(obj, meta=None):
	"""Return a dict with an object's DaBroker-exported attributes"""
	if meta is None:
		meta = obj._meta

	res = {}
	for k in meta.fields:
		res[k] = getattr(obj,k,None)
	for k in meta.refs:
		if k == "_meta": continue
		res[k] = obj._attr_key(k)
	return res

class BadKeyComponentError(RuntimeError):
	pass
scalar_types=integer_types+string_types

class BaseRef(object):
	"""\
		A basic (reference to an) object.

		@key: The actual data required to retrieve the object.
		@code: a secret to ensure that the client got the code from the server
		       This prevents malicious clients from trying to randomly
		       guessing keys for objects they won't usually have access to.
		"""
	def __init__(self, key=None, meta=None, code=None):
		# if this is a hybrid BaseObj+BaseRef thing (like BrokeredInfoInfo),
		# some arguments have been eaten
		if key is None:
			key=self._key
		else:
			for k in key:
				if not isinstance(k,scalar_types):
					raise BadKeyComponentError(k,key)
		if meta is None:
			meta=getattr(self,'_meta',None)

		self.meta = meta
		self.key = tuple(key)
		self.code = code
	
	# BaseRef objects behave like their keys
	# when indexing / comparing.

	def __len__(self):
		return len(self.key)

	def __getitem__(self,k):
		return self.key[k]

	def __hash__(self):
		return self.key.__hash__()
	
	def __eq__(self,other):
		other = getattr(other,'_key',other)
		other = getattr(other,'key',other)
		return self.key.__eq__(other)

	def __ne__(self,other):
		other = getattr(other,'_key',other)
		other = getattr(other,'key',other)
		return self.key.__ne__(other)

	def __repr__(self):
		res = "‹R:"+'¦'.join(str(x) for x in self.key)
		if self.code is not None:
			res += ':'+self.code
		return res+'›'
	__str__ = __repr__

class BaseObj(object):
	"""\
		This is the basic class for our storage system.

		You need:
		@meta (BrokeredInfo): describes this element's class
		@key (BaseRef): required to (re)load this element
		"""
	def __init__(self, meta=None,key=None, **k):
		if meta is not None:
			self._meta = meta
		if key is not None:
			self._key = key
		super(BaseObj,self).__init__(**k)

	# Objects compare like their key
	def __hash__(self):
		self = getattr(self,'_key',self)
		self = getattr(self,'key',self)
		return self.__hash__()
	def __eq__(self,other):
		return self._key.__eq__(other)
	def __ne__(self,other):
		return self._key.__ne__(other)

	def _attr_key(self,k):
		"""Like getattr(), but returns the value's key."""
		res = getattr(self,k,None)
		if res is not None:
			res = res._key
		return res

	@property
	def _attrs(self):
		return get_attrs(self)

class common_BaseRef(object):
	"""Common base for reference coding; the decoder is overridden in client and server"""
	cls = BaseRef
	clsname = "Ref"
	
	@classmethod
	def encode(cls,ref, include=False, meta=None):
		assert not include
		res = {}
		res['k'] = ref.key
		if ref.code is not None and include is not None:
			if ref.code == "ROOT":
				raise RuntimeError("The root object is never referenced")
			res['c'] = ref.code
		return cls.clsname,res

class common_BaseObj(object):
	"""Common base for object coding; the decoder is overridden in client and server"""
	cls = BaseObj
	clsname = "Obj"

	@staticmethod
	def encode_ref(obj,k):
		"""\
			Fetch a reference.

			This is overrideable so that the server-side can generate
			a reference's key without loading the actual object.
			"""
		ref = getattr(obj,k)
		if ref is not None:
			try:
				ref = ref._key
			except Exception:
				raise
		return ref

	@classmethod
	def encode(cls, obj, include=False):
		if not include:
			return common_BaseRef.encode(obj._key, include=False)
			
		rx = common_BaseRef.encode(obj._key)
		if isinstance(rx,tuple):
			res = rx[1]
		else:
			res = rx

		res['f'] = f = dict()
		for k in obj._meta.fields.keys():
			f[k] = getattr(obj,k)
		res['r'] = r = dict()
		for k in obj._meta.refs.keys():
			r[k] = cls.encode_ref(obj,k)
		return res

class BrokeredInfo(BaseObj):
	"""\
		This class is used for metadata about Brokered objects.
		It is immutable on the client.

		The `cached` attribute specifies how client-side search works
		(i.e. the `get` and `find` methods).

		None: No search, explicitly exported methods only.
		False: Searches should not be cached.
		True: Searches are cached normally.
		"""
	name = None
	_meta = None
	cached = None

	def __init__(self,name=None, **k):
		super(BrokeredInfo,self).__init__(**k)
		if name is not None:
			self.name = name
		elif self.name is None:
			self.name = self.__class__.__name__
		self.fields = dict()
		self.refs = dict()
		self.backrefs = dict()
		self.calls = dict()
		self._fields = set()

		self.add(Ref("_meta"))

	def add(self, f, cls=None):
		"""Add a field named 'f' to me."""
		if isinstance(f,string_types):
			f = cls(f)
		elif cls is not None:
			assert isinstance(f,cls),(f,cls)
		if f in self._fields:
			raise RuntimeError("Field already exists",(f.name,f))
		self._fields.add(f)
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

class _Attribute(object):
	"""Base class for attributes of DaBroker-managed types"""
	defer = False

	def __init__(self,name, **kw):
		self.name = name
		for k,v in kw.items():
			setattr(self,k,v)

	# fields should compare with their names
	def __hash__(self):
		return self.name.__hash__()
	def __eq__(self,other):
		return self.name.__eq__(getattr(other,'name',other))
	def __ne__(self,other):
		return self.name.__ne__(getattr(other,'name',other))

	def __repr__(self):
		return "{}({},{})".format(self.__class__.__name__,repr(self.name),
			','.join("{}={}".format(k,repr(v)) for k,v in self.__dict__.items() if k != "name" and not k.startswith('_')))

class Field(_Attribute):
	"""A standard data field; may be a dict/list.
		Set the "hidden" attribute to True if you don't want this value broadcast."""
	pass

class Ref(_Attribute):
	"""A reference to another BrokeredBase object (many-to-one).
		Set the "hidden" attribute to True if you don't want this value broadcast."""
	pass

class BackRef(_Attribute):
	"""A reference from another BrokeredBase object type (one-to-many).
		If you want client-side caching, TODO."""
	pass

class Callable(_Attribute):
	"""A procedure that will be called on the server.
		The 'for_class' attribute, if true, says that this is a classmethod:
		True=yes, None/missing=No, False=it's a staticmethod."""
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
	def decode(cls,**attr):
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
	"""This class describes the fields which BrokeredInfo exports."""
	class_ = BrokeredInfo
	def __init__(self,name, **k):
		super(BrokeredMeta,self).__init__(name=name, **k)
		self.add(Field("name"))
		self.add(Field("cached"))

		self.add(Field("fields"))
		self.add(Field("refs"))
		self.add(Field("backrefs"))
		self.add(Field("calls"))

class BrokeredInfoInfo(BrokeredMeta,BaseRef):
	"""This well-known singleton is used for metadata about BrokeredInfo objects.
		It is its own metadata implementation, its own key, and isn't (in fact cannot be) sent to clients."""
	def __init__(self):
		super(BrokeredInfoInfo,self).__init__(name="BrokeredInfoInfo Singleton", meta=self,key=(),code="ROOT")
		self._key = self
		# This is a singleton that's never destructed, thus the cycles are not a problem.

broker_info_meta = BrokeredInfoInfo()
BrokeredInfo._meta = broker_info_meta

