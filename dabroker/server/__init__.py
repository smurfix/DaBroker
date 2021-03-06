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

from ..base import BrokeredInfo,Callable,Ref,BackRef,Field,BrokeredInfoInfo,BrokeredMeta,_Attribute
from types import FunctionType
from itertools import chain
from six import string_types
from inspect import isfunction,ismethod
from ..util import _ClassMethodType

class ServerBrokeredInfo(BrokeredInfo):
	"""Describe an exported object (or rather, class)"""

	def add_class_attrs(self, cls,attrs='+'):
		"""\
			Analyze a class to show which attributes to export.

			@attrs: a list of field names to export, or a couple of magic values:
			+  all exported functions
			*  all expored functions plus any class variables

			Attributes whose names start with '_' are not exported.
			"""

		known = set()
		for f in chain(self.fields,self.refs,self.backrefs,self.calls):
			known.add(f)

		if isinstance(attrs,string_types):
			if attrs == '*': # fields, too
				attrs = (x for x in dir(cls) if not x.startswith('_') or x.startswith('_dab_'))
			elif attrs == '+': # methods only
				attrs = (x for x in dir(cls) if x.startswith('_dab_') or (not x.startswith('_') and getattr(getattr(cls,x),'_dab_callable',False)))
			else:
				attrs = attrs.split(' ')

		for a in attrs:
			if a in known: # also contains 'hidden'
				continue
			if isinstance(a,_Attribute):
				self.add(a)
				continue

			m = getattr(cls,a)
			d = getattr(m,'_dab_defer',None)
			if d: d={'defer':d}
			else: d = {}

			if isfunction(m): # PY3 unbound method, or maybe a static function
				if not getattr(m,'_dab_callable',False): continue
				self.add(Callable(a,**d))
			elif hasattr(m,'_meta'): # another DaBroker object
				self.add(Ref(a,**d))
			elif isinstance(m,property): # possibly-exported property
				if not getattr(m,'_dab_callable',False): continue
				ref = getattr(m,'_dab_ref',None)
				if ref is None:
					self.add(Field(a,**d))
				elif ref:
					self.add(BackRef(a,**d))
				else:
					self.add(Ref(a,**d))
			elif isinstance(m,_ClassMethodType):
				if not getattr(m,'_dab_callable',False): continue
				self.add(Callable(a,for_class=True,**d))
			elif not ismethod(m): # data, probably
				if a.startswith('_dab_'):
					self.add(Field(a,for_class=True,**d))
				else:
					self.add(Field(a,**d))
			elif getattr(m,'__self__',None) is cls: # classmethod, py2+py3
				if not getattr(m,'_dab_callable',False): continue
				self.add(Callable(a,for_class=True,**d))
			else: # must be a PY2 unbound method
				if not getattr(m,'_dab_callable',False): continue
				self.add(Callable(a,**d))
		

class ServerBrokeredMeta(ServerBrokeredInfo,BrokeredMeta):
	pass

from .service import BrokerServer

############# Convenience methods for exporting stuff

def export_object(obj, loader=None, attrs=None, name=None, key=None, metacls=ServerBrokeredInfo):
	"""\
		Convenience method to export a single object via DaBroker.

		This works by calling export_class on the object's class, but
		adds the _meta attribute to the object instead. Thus, you need
		to explicitly add object-level attributes.

		The loader needs to be passed in to 
		"""
	cls = type(obj)

	meta = self.export_class(obj.__class__, loader, _set=obj, **kw)
	meta.add_class_attrs(meta,meta, attrs='+')
	meta.add_class_attrs(meta,cls, attrs=attrs)
	obj._meta = meta
	if getattr(obj,'_dab_cached',False):
		meta._dab_cached = obj._dab_cached

	if loader:
		if key is None:
			key = (loader.static.id, '_ec',meta.name)
		loader.add(obj,*key)
	return meta

def export_class(cls, loader=None, attrs=None, name=None, key=None, metacls=ServerBrokeredInfo):
	"""\
		Convenience method to export a class via DaBroker.

		@cls: the class to export.
		@attrs: A list of attributes to exports. They will be looked up in the class to determine their type.
		@name: The client-visible name of this class. Defaults to the Python classname.
		@loader,@key: the loader which shall serve the class info, and the well-known key
					  for the class (autogenerated if not passed in). If no loader is given,
		              assigning a key to the new info object is the caller's responsibility.
		@vars @refs @backrefs @funcs: Attributes to export, will not be looked up in the class.
		@classfuncs @classvars: Class attributes to export, will not be seen in instances.

		Note that auto-detecting attributes works on the class, not a
		class object. You need to add attributes manually if they are
		added only when the object is created.

		The return value is the info object for the class, to be assigned
		to instances' _meta attribute.
		"""

	m = getattr(cls,'_meta',None)
	assert m is None or m is broker_info_meta, (cls,m)

	meta = _build_info(cls, name=name,metacls=metacls)
	meta.add_class_attrs(meta, attrs='+')
	meta.add_class_attrs(cls, attrs=attrs)
	cls._meta = meta

	if loader:
		if key is None:
			key = (loader.static.id, '_ec',meta.name)
		loader.add(meta,*key)
	return meta

_export_seq = 0

def _build_info(cls, metacls=ServerBrokeredInfo, name=None, _add_properties=False):
	"""Build a 'meta' object which collects information about a class,
		to be sent to clients.
		"""

	if name:
		clsname = name
	else:
		clsname = cls.__name__
		global _export_seq
		_export_seq += 1
		name = '_'+str(_export_seq)

	# We may need to add properties to this object/class, but that's
	# not how Python works: properties can only be added to a class.
	# 
	# Thus, create a private class and extend that.
	if isinstance(metacls,ServerBrokeredInfo):
		if _add_properties:
			# It's an object. Patch the object's class.
			class metacls_(type(metacls)):
				pass
			meta.__class__ = metacls_
		meta = metacls
	else:
		# It's a class. Create a private subclass to make sure we don't
		# overwrite anything.
		assert issubclass(metacls,ServerBrokeredInfo),metacls.__mro__
		if _add_properties:
			class metacls_(metacls):
				pass
			metacls = metacls_

		meta = metacls(name)
	if _add_properties:
		metacls_.__name__ = str('mcls_'+cls.__name__)

	meta.model = cls
	meta.name = name
	return meta

#class classattr(object):
#	def __init__(self,cls,attr):
#		self.cls = cls
#		self.attr = attr
#	def __get__(self,*a):
#		return getattr(self.cls,self.attr)
#	def __set__(self,i,v):
#		setattr(self.cls,self.attr,v)
