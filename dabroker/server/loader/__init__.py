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

# Object loaders. The static loader is defined here.

from ...base import broker_info_meta, BaseObj,BaseRef

class Loaders(object):
	"""\
		The keeper of all of a server's objects.

		Object keys are tuples; their members can be integes or strings.
		Nested tuples are not supported.

		There is no automatic key assignment anywhere in the
		system. This is no accident. You do not want your keys to change
		when the server needs to be restarted for any reason.
		"""

	def __init__(self,server=None,**k):
		super(Loaders,self).__init__(**k)
		self.server = server # cyclic ref, but long-lived, so it doesn't matter

		self.loaders = {} # first key component => actual loader

		self.static = StaticLoader(self)
		self.static.add(broker_info_meta)

	def add_loader(self,loader):
		"""Register a loader."""
		id = loader.id
		assert id not in self.loaders
		self.loaders[id] = loader

	def get(self,key):
		"""\
			Get an object by key.
			"""
		assert isinstance(key,BaseRef),key
		_key = key.key
		try:
			obj = self.loaders[_key[0]]
		except KeyError:
			raise KeyError("Object type '%s' not known" % (_key[0],))
		obj = obj.get(*_key[1:])
		k = getattr(obj,'_key',None)
		if k is None:
			obj._key = key
		else:
			assert obj._key == key, (obj._key,key)
		return obj

	def delete(self,*key):
		"""\
			Remove an object.
			"""

		if len(key) == 1 and isinstance(key[0],BaseObj):
			key = key[0]._key.key
		self.loaders[key[0]].delete(*key[1:])

	def add(self, obj, *key):
		"""\
			Add an object. Gets the correct loader from the object's class.
			"""
		if key:
			k = key[0]
		else:
			k = getattr(obj,'_meta',obj.__class__._meta)._key.key[0]

		try:
			self.loaders[k].add(obj, *(key[1:] if key else ()))
		except KeyError:
			import pdb;pdb.set_trace()
			raise

class BaseLoader(object):
	id=None
	def __init__(self, parent,id=None):
		if id is not None:
			self.id = id
		assert self.id is not None
		parent.add_loader(self)

	def get(self,*key):
		raise NotImplementedError("You need to override {}.get()".format(self.__class__.__name__))

	def update(self, obj, **kv):
		"""Update an object. You might want to override this."""
		for k,v in kv.items():
			setattr(obj,k,v)

	def delete(self,*key):
		raise NotImplementedError("You need to override {}.delete()".format(self.__class__.__name__))

	def add(self, obj, *key):
		raise NotImplementedError("You need to override {}.new()".format(self.__class__.__name__))
		
	def set_key(self, obj, *key):
		"""sets an object's lookup key. Returns the key object for convenience."""
		if obj is broker_info_meta:
			return broker_info_meta._key

		if len(key) == 1 and isinstance(key[0],BaseRef):
			kx = key[0]
			assert obj.__dict__.get('_key',kx) is kx, (key,kx)
			key = None
		else:
			# if the object has a ._key property which generates the key, getattr() would recurse back here :-/
			kx = obj.__dict__.get('_key',None)

		if kx is not None:
			k = kx.key
			assert k[0] == self.id, (k,self.id)
			if key:
				assert k[1:] == key
		else:
			obj.__dict__['_key'] = kx = BaseRef(key=(self.id,)+key)
		return kx

class StaticLoader(BaseLoader):
	"""A simple 'loader' which serves static objects"""
	id="_s"

	def __init__(self, parent, id=None):
		self.objects = {}
		super(StaticLoader,self).__init__(parent, id=id)

	def get(self,*key):
		return self.objects[key]

	def delete(self,*key):
		del self.objects[key]

	def add(self,obj, *key):
		assert key not in self.objects or self.objects[key] is obj,(key, obj, self.objects[key])
		self.set_key(obj,*key)
		self.objects[key] = obj


