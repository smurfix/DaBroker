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

from ...base import common_BaseRef, BrokeredInfo,BrokeredMeta, Field,Ref,BackRef,Callable
from . import BaseLoader
from ..serial import server_BaseObj
from sqlalchemy.inspection import inspect
from functools import wraps

def with_session(fn):
	@wraps(fn)
	def wrapper(self,*a,**k):
		s = self.loader.session()
		try:
			return fn(self,s, *a,**k)
		except:
			s.rollback()
			s = None
			raise
		finally:
			if s is not None:
				s.commit()
	return wrapper

class server_SQLobject(server_BaseObj):
	cls = None # override me
	#clsname = None # ignore me

	@staticmethod
	def encode(obj, include=False):
		if not hasattr(obj,'_key'):
			obj.__class__._dab.fixup(obj)
		if not include:
			return common_BaseRef.encode(obj)
		return server_BaseObj.encode(obj)

class SQLInfo(BrokeredInfo):
	def __new__(cls, loader, model):
		if hasattr(model,'_dab'):
			return model._dab
		return object.__new__(cls)
	def __init__(self, loader, model):
		if hasattr(model,'_dab'):
			return model._dab
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
		self._meta = self.loader.model_meta
		model._dab = self
		class load_me(server_SQLobject):
			cls = model
		load_me.__name__ = str("codec_sql_"+self.name)
		loader.loaders.server.codec.register(load_me)

	@with_session
	def find(self,session,_limit=None,**kw):
		res = session.query(self.model).filter_by(**kw)
		if _limit is not None:
			res = res[:_limit]
		res = list(res)
		for r in res:
			self.fixup(r)
		return res

	@with_session
	def new(self, session,**kw):
		res = self.model(**kw)
		session.add(res)
		session.flush()
		return self.fixup(res)

	@with_session
	def get(self, session,*key,**kw):
		assert len(key) == 1 or kw and not key
		if key:
			kw['id'] = key[0]
		res = session.query(self.model).filter_by(**kw).one()
		return self.fixup(res)

	def fixup(self,res):
		res._meta = self
		self.loader.set_key(res,res.id)
		return res

class SQLLoader(BaseLoader):
	"""A loader which reads from SQL"""
	def __init__(self, session, loaders,id='sql'):
		self.tables = {}
		super(SQLLoader,self).__init__(id=id,loaders=loaders)

		self.model_meta = BrokeredMeta("sql")
		self.model_meta.add(Callable("new"))
		self.model_meta.add(Callable("get"))
		self.model_meta.add(Callable("find"))
		self.session = session
		self.loaders = loaders
		loaders.static.add(self.model_meta,"sql")

	def add_model(self, model, root=None):
		r = SQLInfo(loader=self, model=model)
		if root is not None:
			root[r.name] = r

		self.tables[r.name] = r
		self.set_key(r,r.name)
		return r

	def get(self,*key):
		m = self.tables[key[0]]
		if len(key) == 1:
			return m
		return m.obj_get(*key[1:])


