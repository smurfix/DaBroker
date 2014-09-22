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

from .thread import AsyncResult

import pytz
UTC = pytz.UTC
with open("/etc/localtime", 'rb') as tzfile:
	TZ = pytz.tzfile.build_tzinfo(str('local'), tzfile)

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
		return self[a]
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

