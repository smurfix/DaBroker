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

# The sqlalchemy object loader

from ...base import BrokeredInfo,BrokeredMeta, Field,Ref,BackRef,Callable, get_attrs
from ...util import cached_property
from ...util.thread import local_object
from ...util.sqlalchemy import with_session
from . import BaseLoader
from ..codec import server_BaseObj
from sqlalchemy.inspection import inspect

import logging
logger = logging.getLogger("dabroker.server.loader.sqlalchemy")

class server_SQLobject(server_BaseObj):
	"""An encoder which auto-sets the _key attribute"""
	cls = None # override me
	#clsname = None # ignore me

	@staticmethod
	def encode(obj, include=False):
		if not hasattr(obj,'_key'):
			obj.__class__._dab.fixup(obj)
		return server_BaseObj.encode(obj, include=include)

	def encode_ref(obj, include=False):
		if not hasattr(obj,'_key'):
			obj.__class__._dab.fixup(obj)
		return server_BaseObj.encode_ref(obj, include=include)

def _get_attrs(obj):
	return get_attrs(obj, obj._dab)

def keyfix(self,*a,**k):
	"""
		SQLalchemy objects don't get a key on their own.
		Attach it here.
		"""
	self.__class__._dab.fixup(self)
	return self.__dict__.get('_key')

class SQLInfo(BrokeredInfo):
	"""This class represents a single SQL table"""
	def __new__(cls, server, meta, model, loader, rw=False, hide=()):
		if hasattr(model,'_dab'):
			return model._dab
		return object.__new__(cls)
	def __init__(self, server, meta, model, loader, rw=False, hide=()):
		if hasattr(model,'_dab'):
			return
		super(SQLInfo,self).__init__()

		i = inspect(model)
		for k in i.column_attrs:
			if k not in hide:
				self.add(Field(k.key))
		for k in i.relationships:
			if k not in hide:
				if k.uselist:
					### TODO add info to generate the list on the client
					self.add(BackRef(k.key))
				else:
					self.add(Ref(k.key))

		self.rw = rw
		if rw:
			self.add(Callable("update"))
			self.add(Callable("delete"))
		self.model = model
		self.server = server
		self.loader = loader
		self.name = i.class_.__name__
		self._meta = meta
		self._dab = self
		model._dab = self
		model._key = cached_property(keyfix)
		model._attrs = property(_get_attrs)
		class load_me(server_SQLobject):
			cls = model
		load_me.__name__ = str("codec_sql_"+self.name)

		server.codec.register(load_me)

	def __call__(self, **kw):
		return self.new(**kw)
		
	@with_session
	def backref_idx(self,session, obj,name,idx):
		return getattr(obj,name)[idx]

	@with_session
	def backref_len(self,session, obj,name):
		return len(getattr(obj,name))

	@with_session
	def find(self,session,_limit=None, **kw):
		res = session.query(self.model).filter_by(**kw)
		if _limit is not None:
			res = res[:_limit]
		res = list(res)
		for r in res:
			self.fixup(r)
		return res
	find.include = True

	@with_session
	def get(self, session,*key, **kw):
		assert len(key) == 1 or kw and not key
		if key:
			kw['id'] = key[0]
		res = session.query(self.model).filter_by(**kw).one()
		self.fixup(res)
		return res
	get.include = True

	@with_session
	def new(self, session, obj=None, *key, **kw):
		if obj is None:
			# called to make a new object
			assert kw and not key
			obj = self.model(**kw)
		else:
			# called with an existing object
			assert not kw
		session.add(obj)
		session.flush()
		self.fixup(obj)
		return obj
	new.include = True

	@with_session
	def update(self, session, obj, **kw):
		assert obj._meta.rw
		obj = session.merge(obj, load=False)
		for k,on in kw.items():
			assert k in self.fields or k in self.refs
			ov,nv = on
			assert getattr(obj,k) == ov, (k,getattr(obj,k),nv)
			setattr(obj,k,nv)
		session.flush()
		self.fixup(obj)

	@with_session
	def local_update(self, session, obj, **kw):
		obj = session.merge(obj, load=False)
		for k,v in kw.items():
			setattr(obj,k,v)
		session.flush()
		self.fixup(obj)

	def delete(self, obj):
		assert obj._meta.rw
		self.server.obj_delete(obj)

	@with_session
	def local_delete(self, session, *key):
		if len(key) == 1 and isinstance(key[0],self.model):
			obj = key[0]
		else:
			obj = self.get(*key)
		session.delete(obj)
		session.flush()

	def fixup(self,obj):
		"""Set _meta and _key attributes"""
		obj._meta = self
		obj._dab = self._dab
		i=inspect(obj)
		return self.loader.set_key(obj,i.class_.__name__,obj.id)

class SQLLoader(BaseLoader):
	"""A loader which reads from SQL"""
	id="sql"
	def __init__(self, session, server,id=None):
		self.tables = {}
		super(SQLLoader,self).__init__(id=id)

		self.model_meta = []
		for rw in range(3):
			m = BrokeredMeta("sql")
			self.model_meta.append(m)
			if rw:
				m.add(Callable("get"))
				m.add(Callable("find"))
				m.session = session
				if rw > 1:
					m.add(Callable("new"))
					m.add(Callable("delete"))
			server.loader.static.new(m,"_sql",self.id,rw)
		self.session = session
		self.loader = server.loader
		self.server = server

	def add_model(self, model, root=None, cls=SQLInfo, rw=False, hide=()):
		r = cls(meta=self.model_meta[0 if rw is None else rw+1], server=self.server, model=model, loader=self, rw=rw, hide=hide)
		if root is not None:
			root[r.name] = r

		self.tables[r.name] = r
		self.set_key(r,r.name)
		return r

	def get(self,*key):
		m = self.tables[key[0]]
		if len(key) == 1:
			return m
		return m.get(*key[1:])

	def new(self, obj, *key):
		if key:
			k = key[0]
		else:
			key = getattr(obj,'_meta',obj.__class__._meta)._key.key[1]
		self.tables[key].new(obj, key[1:] if key else ())
	
