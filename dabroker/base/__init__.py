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

class BaseRef(object):
	"""\
		A basic referenced object.
		"""
	def __init__(self,data):
		self.data = data

class BaseStore(object):
	"""\
		This is the base for our object storage.

		The ID needs to be persistent.
		ID zero is reserved for persistent Info objects.
		"""
	_store = {}

	def __init__(self, id):
		self.id = id
		self._store[id] = self

class BrokeredBase(object):
	"""\
		This is the base class for all brokered objects.

		This object behaves quite differently in the client vs. the server.
		"""
	pass

class BrokeredInfo(BrokeredBase):
	"""This class is used for metadata about Brokered objects."""
	def __init__(self):
		self.fields = dict()
		self.refs = dict()
		self.backrefs = dict()
		self.calls = dict()

	@property
	def _meta(self):
		return broker_info_meta

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
	def __init__(self,name,ref):
		"""@ref: a BrokeredInfo object"""
		super(Ref,self).__init__(name=name,ref=ref)
	pass

class BackRef(_attr):
	"""A reference from another BrokeredBase object type"""
	pass

class Callable(_attr):
	"""A procedure that will be called on the server."""
	pass

class BrokeredInfoInfo(BrokeredInfo):
	"""This class is used for metadata about BrokeredInfo objects."""
	def __init__(self):
		BrokeredInfo.__init__(self)
		self.add(Field("fields"))
		self.add(Field("refs"))
		self.add(Field("backrefs"))
		self.add(Field("calls"))

broker_info_meta = None
broker_info_meta = BrokeredInfoInfo()

