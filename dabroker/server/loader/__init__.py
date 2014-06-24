# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including an optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

# Object loaders. The static loader is defined here

from ...base import broker_info_meta

loaders = {}
def get(key):
	assert isinstance(key,tuple),key
	obj = loaders[key[0]].get(*key[1:])
	k = getattr(obj,'_key',None)
	if k is None:
		obj._key = key
	else:
		assert obj._key == key, (obj._key,key)
	return obj

def add(obj, *key):
	"""Add an object to the store and set its lookup key."""
	if not key:
		key = obj._key
	elif len(key) == 1:
		key = key+obj._key
	loaders[key[0]].add(obj, *key[1:])
	obj._key = key
	

class BaseLoader(object):
	def __init__(self, id):
		assert id not in loaders
		self.id = id
		loaders[id] = self

	def get(self,*key):
		raise NotImplementedError("You need to override {}.get()".format(self.__class__.__name__))

class StaticLoader(BaseLoader):
	"""A simple 'loader' which serves static objects"""
	def __init__(self, id=0):
		self.objects = {}
		super(StaticLoader,self).__init__(id=id)

	def get(self,*key):
		return self.objects[key]

	def add(self,obj,*key):
		assert key not in self.objects
		self.objects[key] = obj

# Static loader, used for metadata
# The info_meta must have the same key on the client!

static = StaticLoader(0)
static.add(broker_info_meta)

