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

# Utility code

from importlib import import_module
from pprint import pformat
from six import PY2,PY3, string_types,text_type
from functools import wraps

from .thread import AsyncResult

import pytz
UTC = pytz.UTC
with open("/etc/localtime", 'rb') as tzfile:
	TZ = pytz.tzfile.build_tzinfo(str('local'), tzfile)

# a prettyprinter which skips these pesky u'' prefixes (PY2)
# and which emits UTF8 instead of escaping everything under the sun
# and which ignores OrderedDict
from pprint import PrettyPrinter,_safe_repr
import datetime as _dt
from io import StringIO as _StringIO
from base64 import b64encode

def uuidstr(u=None):
	if u is None:
		import uuid
		u=uuid.uuid1()
	return b64encode(u.bytes, altchars=b'-_').decode('ascii')

class UTFPrinter(PrettyPrinter,object):
	def _format(self, object, *a,**k):
		typ = type(object)
		if hasattr(object,"values"):
			object = dict(object.items())
		return super(UTFPrinter,self)._format(object, *a,**k)

	def format(self, object, *a,**k):
		typ = type(object)
		if isinstance(object, string_types):
			if PY2 and isinstance(typ,str):
				object = object.decode("utf-8")
		elif typ is _dt.datetime:
			return "DT( %s )"%(format_dt(object),),True,False
		else:
			return super(UTFPrinter,self).format(object,*a,**k)

		s = repr(object)
		if '\\' not in s:
			if s[0] in ('"',"'"):
				return s,True,False
			else:
				return s[1:],True,False

		# more work here
		if "'" in s[2:-1] and '"' not in s[2:-1]:
			closure = '"'
			quotes = {'"': '\\"'}
		else:
			closure = "'"
			quotes = {"'": "\\'"}
		qget = quotes.get
		sio = _StringIO()
		write = sio.write
		for char in object:
			if not char.isalpha():
				char = qget(char, text_type(repr(char)))
				if char[0] == 'u':
					char = char[2:-1]
				else:
					char = char[1:-1]
			else:
				char = text_type(char)
			write(char)
		return ("%s%s%s" % (closure, sio.getvalue(), closure)), True, False

def pprint(object, stream=None, indent=1, width=80, depth=None):
	"""Pretty-print a Python object to a stream [default is sys.stdout]."""
	UTFPrinter(stream=stream, indent=indent, width=width, depth=depth).pprint(object)

def pformat(object, indent=1, width=80, depth=None):
	"""Format a Python object into a pretty-printed representation."""
	return UTFPrinter(indent=indent, width=width, depth=depth).pformat(object)

# Default timeout for the cache.
def format_dt(value, format='%Y-%m-%d %H:%M:%S'):
	try:
		return value.astimezone(TZ).strftime(format)
	except ValueError: ## naïve time: assume UTC
		return value.replace(tzinfo=UTC).astimezone(TZ).strftime(format)

def _p_filter(m,mids):
	if isinstance(m,dict):
		if m.get('_oi',0) not in mids:
			del m['_oi']
		for v in m.values():
			_p_filter(v,mids)
	elif isinstance(m,(tuple,list)):
		for v in m:
			_p_filter(v,mids)
def _p_find(m,mids):
	if isinstance(m,dict):
		mids.add(m.get('_or',0))
		for v in m.values():
			_p_find(v,mids)
	elif isinstance(m,(tuple,list)):
		for v in m:
			_p_find(v,mids)

def format_msg(m):
	mids = set()
	_p_find(m,mids)
	_p_filter(m,mids)
	return pformat(m)

def import_string(name):
	"""Import a module, or resolve an attribute of a module."""
	name = str(name)
	try:
		return import_module(name)
	except ImportError:
		if '.' not in name:
			raise
		module, obj = name.rsplit('.', 1)
		try:
			return getattr(import_string(module),obj)
		except AttributeError:
			raise AttributeError(name)

class _missing: pass

class attrdict(dict):
	"""A dictionary which can be accessed via attributes, for convenience"""
	def __init__(self,*a,**k):
		super(attrdict,self).__init__(*a,**k)
		self._done = set()

	def __getattr__(self,a):
		if a.startswith('_'):
			return super(attrdict,self).__getattr__(a)
		try:
			return self[a]
		except KeyError:
			raise AttributeError(a)
	def __setattr__(self,a,b):
		if a.startswith("_"):
			super(attrdict,self).__setattr__(a,b)
		else:
			self[a]=b
	def __delattr__(self,a):
		del self[a]

class cached_property(object):
	"""A decorator that converts a function into a lazy property.

	Copied from werkzeug, extended to deal with async calls.
	"""

	def __init__(self, func, name=None, doc=None):
		self.__name__ = name or func.__name__
		self.__module__ = func.__module__
		self.__doc__ = doc or func.__doc__
		self.func = func
		self.value = _missing
		self.in_progress = dict()

	def __get__(self, obj, type=None):
		if obj is None:
			return self
		value = obj.__dict__.get(self.__name__, _missing)
		if value is _missing:
			value = self.in_progress.get(id(obj),_missing)
			if value is _missing:
				self.in_progress[id(obj)] = ar = AsyncResult()
				value = self.func(obj)
				ar.set(value)
				obj.__dict__[self.__name__] = value
				del self.in_progress[id(obj)]
			elif isinstance(value, AsyncResult):
				value = value.get()
		return value

default_attrs = dict(callable=True, include=True)
def _dab_(d):
	"""\
		Called with a dict. Returns a dict with every key with '_dab_',
		unless there is at least one key which starts with an underscore.
		Also adds the default attributes _dab_*, unless overridden or
		explicitly turned off by passing a true-valued item named
		_dab_no_*, for * in (callable,include)
		"""
	under=False
	for k in d:
		if k[0]=='_':
			under=True
			break
	if not under:
		d = dict(('_dab_'+k,v) for k,v in d.items())
	for k,v in default_attrs.items():
		if '_dab_'+k not in d and not d.get('_dab_no_'+k,False):
			d['_dab_'+k]=v
	return d

def exported(_fn=None,**attrs):
	"""\
		Decorator to mark a method as exported to the client
		"""
	if _fn is None:
		def xfn(_fn):
			return exported(_fn,**attrs)
		return xfn
	# Functions allow arbitrary attributes, so this is easy
	attrs = _dab_(attrs)
	for k,v in attrs.items():
		setattr(_fn,k,v)
	return _fn

# Python doesn't support setting an attribute on MethodType,
# and the thing is not subclass-able either,
# so I need to implement my own.
# Fortunately, this is reasonably easy.
from types import MethodType 
class _ClassMethodType(object):
	def __init__(self,_func,_self,_cls=None, **attrs):
		self.im_func = self.__func__ = _func
		self.im_self = self.__self__ = _self
		self.im_class = _cls
		self.__name__ = _func.__name__
		for k,v in attrs.items():
			setattr(self,k,v)
	def __call__(self,*a,**k):
		return self.__func__(self.im_self,*a,**k)

class _StaticMethodType(_ClassMethodType):
	def __call__(self,*a,**k):
		return self.__func__(*a,**k)

def exported_classmethod(_fn=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_classmethod(bar=42)
				def foo(cls, …):
					pass
			assert test.foo.bar == 42
		"""
	if _fn is None:
		def xfn(_fn):
			return exported_classmethod(_fn,**attrs)
		return xfn
	res = _ClassMethodAttr(_fn)
	attrs = _dab_(attrs)
	for k,v in attrs.items():
		setattr(res,k,v)
	res._attrs = attrs
	return res

def exported_staticmethod(_fn=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_staticmethod(bar=42)
				def foo(…):
					pass
			assert test.foo.bar == 42
		"""
	if _fn is None:
		def xfn(_fn):
			return exported_staticmethod(_fn,**attrs)
		return xfn
	res = _StaticMethodAttr(_fn)
	attrs = _dab_(attrs)
	for k,v in attrs.items():
		setattr(res,k,v)
	res._attrs = attrs
	return res

def exported_property(fget=None,fset=None,fdel=None,doc=None,**attrs):
	"""\
		A classmethod thing which supports attributed methods.

		Usage:
			class test(object):
				@exported_staticmethod(bar=42)
				def foo(…):
					pass
			assert test.foo.bar == 42
		"""
	if fget is None and attrs is not None:
		def xfn(fget=None,fset=None,fdel=None,doc=None):
			return exported_property(fget=fget,fset=fset,fdel=fdel,doc=doc,**attrs)
		return xfn
	attrs = dict(_dab_(attrs))
	res = _PropertyAttr(fget=fget,fset=fset,fdel=fdel,doc=doc,**attrs)
	return res

class _PropertyAttr(property):
	"""Yet another rewrite in Python."""
	# The problem here is that
	#	@exported_property
	#	def x(self): …
	#	@x.setter
	#	def x(self,val): …
	# will call exported_property() TWICE. (Or thrice if you also want a deleter.)
	# Therefore we stash the attributes in the function.
	# TODO: check if one of these has attributes which are not "ours"?

	def __init__(self, fget=None,fset=None,fdel=None,doc=None,**attrs):
		property.__init__(self,fget=fget,fset=fset,fdel=fdel,doc=doc)
		f = fget
		if not attrs:
			if not f or not f.__dict__:
				f = fset
				if not f or not f.__dict__:
					f = fdel
			if f:
				attrs = f.__dict__
		for f in (fget,fset,fdel):
			if f is None:
				continue
			for k,v in attrs.items():
				setattr(f,k,v)
		for k,v in attrs.items():
			setattr(self,k,v)
		self.__doc__=doc

class _ClassMethodAttr(classmethod):
	def __get__(self,i,t=None):
		if i is not None:
			# Ignore calls on objects
			return classmethod.__get__(self,i,t)

		# Otherwise we need to build this thing ourselves.
		# TODO: use our data directly, rather than calling up.
		res = classmethod.__get__(self,i,t)
		if PY3:
			return _ClassMethodType(res.__func__,res.__self__, **self._attrs)
		else:
			return _ClassMethodType(res.im_func,res.im_self,res.im_class, **self._attrs)

class _StaticMethodAttr(classmethod):
	def __get__(self,i,t=None):
		if i is not None:
			# Ignore calls on objects
			return classmethod.__get__(self,i,t)

		# same as classmethod (almost)
		res = classmethod.__get__(self,i,t)
		if PY3:
			return _StaticMethodType(res.__func__,res.__self__, **self._attrs)
		else:
			return _StaticMethodType(res.im_func,res.im_self,res.im_class, **self._attrs)

#class exported_staticmethod(staticmethod):
#	_dab_callable = True
#	def __new__(self,*a,**k):
#		res = staticmethod.__new__(self,*a,**k)
#	def __init__(self,*a,**k):
#		res = staticmethod.__init__(self,*a,**k)
#	def __get__(self,*a,**k):
#		res = staticmethod.__get__(self,*a,**k)
#		res._dab_callable = True
#		return res


