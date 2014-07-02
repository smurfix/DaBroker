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

from ...base import broker_info_meta

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

		self.static = StaticLoader(0)
		self.add_loader(self.static)
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
		assert isinstance(key,tuple),key
		obj = self.loaders[key[0]].get(*key[1:])
		k = getattr(obj,'_key',None)
		if k is None:
			obj._key = key
		else:
			assert obj._key == key, (obj._key,key)
		return obj

class BaseLoader(object):
	def __init__(self, loaders=None, id=0):
		self.id = id

	def get(self,*key):
		raise NotImplementedError("You need to override {}.get()".format(self.__class__.__name__))

	def set_key(self, obj, *key):
		"""sets an object's lookup key."""
		if obj is broker_info_meta:
			return
		k = getattr(obj,'_key',None)
		if k:
			assert k[0] == self.id, (k,self.id)
			if key:
				assert k[1:] == key
		else:
			obj._key = (self.id,)+key

class StaticLoader(BaseLoader):
	"""A simple 'loader' which serves static objects"""
	def __init__(self, loaders=None, id='static'):
		self.objects = {}
		super(StaticLoader,self).__init__(id=id, loaders=loaders)

	def get(self,*key):
		return self.objects[key]

	def add(self,obj,*key):
		assert key not in self.objects or self.objects[key] is obj,(key, obj, self.objects[key])
		self.set_key(obj,*key)
		self.objects[obj._key[1:]] = obj


