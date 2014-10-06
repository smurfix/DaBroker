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

# This implements the main broker server.

from .loader import Loaders
from . import ServerBrokeredInfo
from ..base import UnknownCommandError,BaseRef, Field,Ref,BackRef,Callable, broker_info_meta,_Attribute
from ..util import import_string,_ClassMethodType
from ..base.config import default_config
from ..base.transport import BaseCallbacks
from ..base.service import BrokerEnv
from .codec import adapters as default_adapters

import sys
from traceback import format_exc
from itertools import chain
from six import string_types
from inspect import ismethod,isfunction

import logging
logger = logging.getLogger("dabroker.server.service")

class _NotGiven: pass

_export_seq = 0

class BrokerServer(BrokerEnv, BaseCallbacks):
	"""\
		The main server.

		"""
	root = None
	transport = None
	last_msgid = 0

	def __init__(self, cfg={}, loader=None, adapters=()):
		# the sender might be set later
		logger.debug("Setting up")
		self.cfg = cfg
		for k,v in default_config.items():
			self.cfg.setdefault(k,v)

		if loader is None:
			loader = Loaders(server=self)
		self.loader = loader
		self.codec = self.make_codec(default_adapters)
		self.register_codec(adapters)
		super(BrokerServer,self).__init__()

	def make_loader(self):
		from .loader import Loaders
		return Loaders()

	def make_transport(self):
		name = self.cfg['transport']
		if '.' not in name:
			name = "dabroker.server.transport."+name+".Transport"
		return import_string(name)(cfg=self.cfg, callbacks=self)

	def make_codec(self, adapters):
		name = self.cfg['codec']
		if '.' not in name:
			name = "dabroker.base.codec."+name+".Codec"
		return import_string(name)(loader=self.loader, adapters=adapters, cfg=self.cfg)

	def register_codec(self,adapter):
		self.codec.register(adapter)

	def start(self,purge=False):
		if self.transport is None:
			self.transport = self.make_transport()
		self.transport.connect(purge=purge)

	def stop(self):
		self.transport.disconnect()

	# server-side convenience methods

	def obj_new(self, cls, *key, **kw):
		obj = cls(**kw)
		if getattr(obj,'_key',None) is None:
			self.loader.new(obj, *key)
			# otherwise the class already did that

		attrs = {}
		for k in chain(obj._meta.fields.keys(), obj._meta.refs.keys()):
			if k != '_meta':
				attrs[k] = getattr(obj,k,None)
		self.send_created(obj, attrs)
		return obj

	def obj_update(self, obj, **kw):
		attrs = {}
		for k,v in kw.items():
			if k in obj._meta.fields or k in obj._meta.refs:
				if k != '_meta':
					attrs[k] = (getattr(obj,k,None),v)
		obj._meta.local_update(obj, **kw)
		self.send_updated(obj, attrs)

	def obj_delete(self, obj):
		attrs = {}
		for k in chain(obj._meta.fields.keys(), obj._meta.refs.keys()):
			if k != '_meta':
				attrs[k] = (getattr(obj,k,None),)
		obj._meta.local_delete(obj)
		self.send_deleted(obj, attrs)

	def add_static(self, obj, *key):
		self.loader.static.add(obj, *key)

	def del_static(self, obj, *key):
		self.loader.static.delete(obj, *key)

	def get(self,*a,**k):
		return self.loader.get(*a,**k)

	# remote calls

	def do_root(self):
		#logger.debug("Get root")
		res = self.root
		if '_key' not in res.__dict__:
			self.add_static(res,'root')
		if '_key' not in res._meta.__dict__:
			self.add_static(res._meta,'root','meta')
		return res
	do_root.include = True

	def do_echo(self,msg):
		logger.debug("echo %r",msg)
		return msg

	def do_ping(self):
		logger.debug("ping")

	def do_pong(self):
		logger.debug("pong")

	def do_get(self, obj):
		"""Fetch an object by its key"""
		# … which the loader, most likely, has already taken care of
		#logger.debug("get %r",obj)
		if isinstance(obj,BaseRef):
			raise RuntimeError("Not without code")
		return obj
	do_get.include = True

	def do_update(self,obj,k={}):
		"""Update an object.
		
			@k: A map of key => (old_value,new_value)
			"""
		logger.debug("update %r %r",obj,k)
		attrs = obj._meta.update(obj,**k)
		if not attrs:
			attrs = k
		self.send_updated(obj, attrs)

	def do_backref_idx(self, obj, name,idx):
		"""Get an item from the backref list. This is severely suboptimal."""
		return obj._meta.backref_idx(obj,name,idx)

	def do_backref_len(self, obj, name):
		"""Get the length of the backref list."""
		return obj._meta.backref_len(obj,name)

	# Broadcast messages to clients

	def send_ping(self, msg):
		self.send("ping",msg)
	
	def send_signal(self, obj, sig, **msg):
		self.send("signal", _sig=sig, _obj=obj, **msg)
		
	# The next three broadcast messages are used for broadcasting object
	# changes. They will invalidate possibly-matching search results.
	def send_created(self, obj, attrs={}):
		"""This object has been created."""
		attrs = dict((k,(v,)) for k,v in attrs.items())
		self.send("invalid_key", _meta=obj._meta._key, _include=None, **attrs)

	def send_deleted(self, obj, attrs={}):
		"""This object has been deleted."""
		attrs = dict((k,(v,)) for k,v in attrs.items())
		self.send("invalid_key", _key=obj._key, _meta=obj._meta._key, _include=None, **attrs)

	def send_updated(self, obj, attrs={}):
		"""\
			An object has been updated.
		
			@attrs is the object change map.
			"""
		key = obj._key
		mkey = obj._meta._key
		fields = obj._meta.refs
		refs = obj._meta.refs
		for k,on in attrs.items():
			ov,nv = on
			if k in refs:
				if getattr(refs[k],'_dab_hidden'):
					continue
				if ov is not None: ov = ov._key
				if nv is not None: nv = nv._key
			elif k in fields:
				if getattr(fields[k],'_dab_hidden'):
					continue
			if ov != nv:
				attrs[k] = (ov,nv)
		self.send("invalid_key", _key=key, _meta=mkey, _include=None, **attrs)
		
	# Basic transport handling

	def recv(self, msg):
		"""Receive a message. Usually called as a separate thread."""
		incl = False
		with self.env:
			#logger.debug("recv raw %r",msg)

			rmsg=msg
			try:
				msg = self.codec.decode(msg)
				msg = self.codec.decode2(msg)

				#logger.debug("recv %r",msg)
				m = msg.pop('_m')
				o = msg.pop('_o',None)
				a = msg.pop('_a',())
				mt = msg.pop('_mt',False)

				try:
					if o is not None:
						if m == "_dab_search" and getattr(o,'_dab_cached',None) is not None:
							incl = msg.get('_limit',99) < 10
						else:
							assert m in o._meta.calls,"You cannot call method {} of {}".format(m,o)
						if not mt and m[0] != '_' and isinstance(o,ServerBrokeredInfo):
							do = o.model
						else:
							do = o
						proc = getattr(do,m)
						if not getattr(proc,'_dab_callable',False):
							import pdb;pdb.set_trace()
							raise UnknownCommandError((m,o,a))
					else:
						proc = getattr(self,'do_'+m)
				except AttributeError:
					import pdb;pdb.set_trace()
					raise UnknownCommandError((m,o,a))
				msg = proc(*a,**msg)
				#logger.debug("reply %r",msg)
				try:
					msg = self.codec.encode(msg, _include = getattr(proc,'include',incl), msgid=self.last_msgid)
				except Exception:
					print("RAW was",rmsg,file=sys.stderr)
					print("MSG is",msg,file=sys.stderr)
					raise
				return msg

			except BaseException as e:
				return self.codec.encode_error(e, sys.exc_info()[2])

	def send(self, action, *a, **k):
		"""Broadcast a message to all clients"""
		with self.env:
			#logger.debug("bcast %s %r %r",action,a,k)
			msg = k
			include = k.pop('_include',False)
			msg['_m'] = action
			if a:
				msg['_a'] = a

			msgid = self.last_msgid+1
			self.last_msgid = msgid
			msg = self.codec.encode(msg, include=include, msgid=msgid)
			self.transport.send(msg)

	############# Convenience methods for exporting stuff

	def export_object(self, obj, **kw):
		"""\
			Convenience method to export a single object via DaBroker.
			The object's class must not already be exported.

			This works by calling export_class on the object's class, but
			adds the _meta attribute to the object instead. Thus, you need
			to explicitly add object-level attributes.
			"""
		kw.setdefault('metacls', ServerBrokeredInfo)
		meta = self.export_class(obj.__class__, _set=obj, **kw)
		return meta

	def export_class(self, cls, attrs=None, metaattrs='+', vars=None, refs=None, backrefs=None, funcs=None, hidden=None, classfuncs=None, name=None, key=None, classkey=None, metacls=ServerBrokeredInfo, metametacls=ServerBrokeredInfo, _set=None):
		"""\
			Convenience method to export a class via DaBroker.
			@cls: the class to export.
			@attrs: A list of attributes to exports. They will be looked up in the class to determine their type.
			@name: The client-visible name of this class. Defaults to the Python classname.
			@key, @classkey: well-known keys for the class. Will otherwise be allocated.
			@vars @refs @backrefs @funcs @classfuncs: Attributes to export, will not be looked up in the class.

			Note that auto-detecting attributes works on the class, not a
			class object. You need to add attributes manually if they are
			added only when the object is created.
			"""

		m = getattr(cls,'_meta',None)
		assert m is None or m is broker_info_meta, (cls,m)

		# Build
		known = set()
		classknown = set()
		def make_set(s,k=known):
			if s is NullSet or isinstance(s,NullSet):
				return NullSet()
			res = set()
			if s is None:
				return res

			if isinstance(s,string_types):
				s = s.split(' ')
			for m in s:
				if m:
					res.add(m)
					k.add(m)
			return res

		# known types
		vars = make_set(vars)
		vars2 = make_set(())
		refs = make_set(refs)
		backrefs = make_set(backrefs)
		funcs = make_set(funcs)
		if classfuncs is not _NotGiven:
			classfuncs = make_set(classfuncs)
		make_set(hidden)

		if attrs is not None:
			if isinstance(attrs,string_types):
				if attrs == '*': # fields, too
					attrs = (x for x in dir(cls) if not x.startswith('_'))
				elif attrs == '+': # methods only
					attrs = (x for x in dir(cls) if not x.startswith('_') and getattr(getattr(cls,x),'_dab_callable',False))
				else:
					attrs = attrs.split(' ')
			for a in attrs:
				if a in known: # also contains 'hidden'
					continue
				if isinstance(a,_Attribute):
					vars2.add(a) # doesn't matter which kind
					continue

				m = getattr(cls,a)
				d = getattr(m,'_dab_defer',None)
				if d: d={'defer':d}
				else: d = {}

				if isfunction(m): # PY3 unbound method, or maybe a static function
					if not getattr(m,'_dab_callable',False): continue
					funcs.add(Callable(a,**d))
				elif hasattr(m,'_meta'): # another DaBroker object
					refs.add(Ref(a,**d))
				elif isinstance(m,property): # possibly-exported property
					if not getattr(m,'_dab_callable',False): continue
					ref = getattr(m,'_dab_ref',None)
					if ref is None:
						vars.add(Field(a,**d))
					elif ref:
						vars.add(BackRef(a,**d))
					else:
						vars.add(Ref(a,**d))
				elif isinstance(m,_ClassMethodType):
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
				elif not ismethod(m): # data, probably
					vars.add(Field(a,**d))
				elif getattr(m,'__self__',None) is cls: # classmethod, py2+py3
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
				else: # must be a PY2 unbound method
					if not getattr(m,'_dab_callable',False): continue
					funcs.add(Callable(a,**d))
		
		if name:
			clsname = name
		else:
			clsname = cls.__name__
			global _export_seq
			_export_seq += 1
			name = '_'+str(_export_seq)

		if isinstance(metacls,ServerBrokeredInfo):
			meta = metacls
		else:
			meta = metacls(name)

		if metaattrs is not None:
			if isinstance(metaattrs,string_types):
				if metaattrs == '*': # fields, too
					metaattrs = (x for x in dir(metacls) if not x.startswith('_'))
				elif metaattrs == '+': # methods only
					metaattrs = (x for x in dir(metacls) if not x.startswith('_') and getattr(getattr(metacls,x),'_dab_callable',False))
				else:
					metaattrs = metaattrs.split(' ')

			for a in metaattrs:
				if a in known: # also contains 'hidden'
					continue
				if isinstance(a,_Attribute):
					vars2.add(a) # doesn't matter which kind
					continue

				m = getattr(metacls,a)
				d={'meta':True}
				if  getattr(m,'_dab_defer',None):
					d['defer']=d

				if isfunction(m): # PY3 unbound method, or maybe a static function
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
				elif hasattr(m,'_meta'): # another DaBroker object
					#classrefs.add(Ref(a,**d))
					pass
				elif isinstance(m,property): # possibly-exported property
					pass
					#if not getattr(m,'_dab_callable',False): continue
					#ref = getattr(m,'_dab_ref',None)
					#if ref is None:
					#	classvars.add(Field(a,**d))
					#elif ref:
					#	classvars.add(BackRef(a,**d))
					#else:
					#	classvars.add(Ref(a,**d))
				elif isinstance(m,_ClassMethodType):
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
				elif not ismethod(m): # data, probably
					pass
					#classvars.add(Field(a,**d))
				elif getattr(m,'__self__',None) is cls: # classmethod, py2+py3
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
				else: # must be a PY2 unbound method
					if not getattr(m,'_dab_callable',False): continue
					classfuncs.add(Callable(a,**d))
		
		if metametacls is not ServerBrokeredInfo or classfuncs is not _NotGiven and classfuncs:
			if isinstance(metametacls,ServerBrokeredInfo):
				mmeta = metametacls
			else:
				mmeta = metametacls("meta_"+name)
			for f in classfuncs:
				if not isinstance(f,_Attribute):
					f = Callable(f)
				mmeta.add(f)
			if classkey is None:
				classkey = (self.loader.static.id, '_ecm',name)
			if getattr(mmeta,'_key',None) is None:
				self.loader.add(mmeta,*classkey)
			meta._meta = mmeta

		for f in vars: meta.add(f)
		for f in vars2: meta.add(f,Field)
		for f in refs: meta.add(f,Ref)
		for f in backrefs: meta.add(f,BackRef)
		for f in funcs: meta.add(f,Callable)
		cls._meta = meta
		setattr(cls if _set is None else _set, '_meta', meta)
		if key is None:
			key = (self.loader.static.id, '_ec',name)
		self.loader.add(meta,*key)
		return meta

class NullSet(set):
	"""An empty set that stays empty."""
	def add(self,x):
		pass

