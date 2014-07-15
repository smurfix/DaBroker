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
from ...util.thread import local_object
from . import BaseLoader
from ..codec import server_BaseObj
from sqlalchemy.inspection import inspect
from functools import wraps

import logging
logger = logging.getLogger("dabroker.server.loader.sqlalchemy")

_session = local_object()

def with_session(fn):
	@wraps(fn)
	def wrapper(self,*a,**k):
		s = getattr(_session,'s',None)
		if s is None:
			logger.debug("new session")
			_session.s = s = self._meta.session()
		else:
			logger.debug("old session")
		try:
			return fn(self,s, *a,**k)
		except:
			logger.debug("rollback")
			s.rollback()
			s = None
			raise
		finally:
			if s is not None:
				logger.debug("commit")
				s.commit()
	return wrapper

class server_SQLobject(server_BaseObj):
	"""An encoder which auto-sets the _key attribute"""
	cls = None # override me
	#clsname = None # ignore me

	@staticmethod
	def encode(obj, include=False):
		if not hasattr(obj,'_key'):
			obj.__class__._dab.fixup(obj)
		return server_BaseObj.encode(obj, include=include)

def _get_attrs(obj):
	return get_attrs(obj, obj._dab)

class SQLInfo(BrokeredInfo):
	"""This class represents a single SQL table"""
	def __new__(cls, server, meta, model, loader):
		if hasattr(model,'_dab'):
			return model._dab
		return object.__new__(cls)
	def __init__(self, server, meta, model, loader):
		if hasattr(model,'_dab'):
			return
		super(SQLInfo,self).__init__()

		i = inspect(model)
		for k in i.column_attrs:
			self.add(Field(k.key))
		for k in i.relationships:
			if k.uselist:
				self.add(BackRef(k.key))
			else:
				self.add(Ref(k.key))
		self.model = model
		self.loader = loader
		self.name = i.class_.__name__
		self._meta = meta
		model._dab = self
		model._attrs = property(_get_attrs)
		class load_me(server_SQLobject):
			cls = model
		load_me.__name__ = str("codec_sql_"+self.name)

		server.codec.register(load_me)

	def __call__(self, **kw):
		return self.new(**kw)
		
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
		return self.fixup(res)
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

	@with_session
	def delete(self, session, *key):
		if len(key) == 1 and isinstance(key[0],self.model):
			obj = key[0]
		else:
			obj = self.get(*key)
		session.delete(obj)
		session.flush()

	def fixup(self,obj):
		"""Set _meta and _key attributes"""
		obj._meta = self
		i=inspect(obj)
		self.loader.set_key(obj,i.class_.__name__,obj.id)
		return obj

class SQLLoader(BaseLoader):
	"""A loader which reads from SQL"""
	id="sql"
	def __init__(self, session, server,id=None):
		self.tables = {}
		super(SQLLoader,self).__init__(id=id)

		self.model_meta = BrokeredMeta("sql")
		self.model_meta.add(Callable("new"))
		self.model_meta.add(Callable("get"))
		self.model_meta.add(Callable("find"))
		self.model_meta.add(Callable("delete"))
		self.model_meta.session = session
		self.session = session
		self.loaders = server.loader
		self.loaders.static.new(self.model_meta,"_sql")
		self.server = server

	def add_model(self, model, root=None, cls=SQLInfo):
		r = cls(meta=self.model_meta, server=self.server, model=model, loader=self)
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
	
