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

from .. import ServerBrokeredInfo, export_class
from ...base import BaseRef, Field,Ref,BackRef,Callable, get_attrs,NoData
from ...util import cached_property,exported
from ...util.thread import local_object
from ...util.sqlalchemy import with_session
from . import BaseLoader
from .. import ServerBrokeredMeta
from ..codec import server_BaseObj
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.exc import NoResultFound
from inspect import isfunction,ismethod

import logging
logger = logging.getLogger("dabroker.server.loader.sqlalchemy")

class server_SQLobject(server_BaseObj):
	"""An encoder which auto-sets the _key attribute"""
	cls = None # override me
	#clsname = None # ignore me

	@staticmethod
	def encode(obj, include=False):
		if '_key' not in obj.__dict__:
			obj.__class__._dab.fixup(obj)
		return server_BaseObj.encode(obj, include=include)

	def encode_ref(obj, include=False):
		if '_key' not in obj.__dict__:
			obj.__class__._dab.fixup(obj)
		return server_BaseObj.encode_ref(obj, include=include)

def _get_attrs(obj):
	return get_attrs(obj, obj._dab)

def keyfix(self,*a,**k):
	"""
		SQLalchemy objects don't magically get a key on their own.
		Attach it with this property function.
		"""
	self.__class__._dab.fixup(self)
	return self.__dict__.get('_key')

class SQLInfo(ServerBrokeredInfo):
	"""This class represents a single SQL table"""
	_dab_cached = True

	def __new__(cls, id, server, model, loader, rw=False, hide=()):
		if hasattr(model,'_dab'):
			return model._dab
		return object.__new__(cls)
	def __init__(self, id, server, model, loader, rw=False, hide=()):
		if hasattr(model,'_dab'):
			assert model._dab is self
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
		self._dab_cached = getattr(self,'_dab_cached',getattr(model,'_dab_cached',None))
		model._dab = self
		model._key = cached_property(keyfix)
		model._attrs = property(_get_attrs)
		class load_me(server_SQLobject):
			cls = model
		load_me.__name__ = str("codec_sql_"+self.name)

		# now do the rest
		export_class(model, server.loader, attrs='+', name=self.name, metacls=self, key=(loader.id,self.name))
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

	@exported
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

	@exported
	@with_session
	def _dab_search(self,session,_limit=None, **kw):
		res = session.query(self.model).filter_by(**kw)
		if _limit is not None:
			res = res[:_limit]
		res = list(res)
		for r in res:
			self.fixup(r)
		return res

	@exported
	@with_session
	def _dab_count(self,session, **kw):
		res = session.query(self.model).filter_by(**kw).count()
		return res

	@with_session
	def get(self, session,*key, **kw):
		assert len(key) == 1 or kw and not key
		if key:
			kw['id'] = key[0]
		try:
			res = session.query(self.model).filter_by(**kw).one()
		except NoResultFound:
			raise NoData(table=self.name,key=kw)
			
		self.fixup(res)
		return res
	get._dab_include = True

	def new_setup(self,obj,**kw):
		"""Method to override, to add interesting things to an object"""
		pass

	@exported
	@with_session
	def new(self, session, obj=None, *key, **kw):
		assert self.rw is not None
		if obj is None:
			# called to make a new object
			assert kw and not key
			obj = self.model(**kw)
		else:
			# called with an existing object
			assert not kw
		self.new_setup(obj,**kw)
		session.add(obj)
		session.flush()
		self.fixup(obj)
		self.server.send_created(obj,kw)
		return obj

class SQLLoader(BaseLoader):
	"""A loader which reads from SQL"""
	id="sql"
	def __init__(self, session, server,id=None):
		self.tables = {}
		self.meta = {}
		if id is None: id = self.id
		else: self.id = id
		super(SQLLoader,self).__init__(server.loader,id=id)

		self.session = session
		self.server = server

	def add_model(self, model, root=None, cls=SQLInfo, rw=False, hide=()):
		r = cls(id=self.id, server=self.server, model=model, loader=self, rw=rw, hide=hide)
		self.meta[r.name]=r._meta

		if root is not None:
			root[r.name] = r

		self.tables[r.name] = r
		return r

	def get(self,*key):
		if key[0] == "_meta":
			return self.meta[key[1]]
		m = self.tables[key[0]]
		if len(key) == 1:
			return m
		return m.get(*key[1:])

	def add(self, obj, *key):
		# dummy, .get already knows me
		assert len(key) == 1 and key[0] == obj.name, (key,obj,obj.name)
		return self.set_key(obj, obj.name)

	def new(self, obj, *key):
		import pdb;pdb.set_trace()
		if key:
			k = key[0]
		else:
			key = getattr(obj,'_meta',obj.__class__._meta)._key.key[1]
		self.tables[key].new(obj, key[1:] if key else ())

