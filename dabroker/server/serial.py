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

from ..base import BaseRef,BaseObj,BrokeredInfo, adapters as baseAdapters, common_BaseObj,common_BaseRef

# This is the server's storage side.

adapters = baseAdapters[:]

def serial_adapter(cls):
	adapters.append(cls)
	return cls

@serial_adapter
class server_BaseObj(common_BaseObj):
	@staticmethod
	def decode(k=None,f=None,r=None):
		from .loader import get
		res = get(k)
		if f:
			for k,v in f.items():
				if getattr(res,k) != v:
					raise NotImplementedError("Update: {} {} {} {}".format(res,k,getattr(res,k),v))
		if r:
			for k,v in r.items():
				if getattr(res,k) != v:
					raise NotImplementedError("Update: {} {} {} {}".format(res,k,getattr(res,k),v))
		return res

@serial_adapter
class server_BaseRef(common_BaseRef):
	cls = BaseRef
	clsname = "Ref"

	@staticmethod
	def encode(obj, include=False):
		if include:
			from .loader import get
			obj = get(obj._key)
			return server_BaseObj.encode(obj,include)
		return common_BaseRef.encode(obj)

	@staticmethod
	def decode(k=None,m=None):
		from .loader import get
		res = get(k)
		if m:
			assert m is res._meta,(m,res._meta)
		return res

@serial_adapter
class server_InfoObj(server_BaseObj):
	cls = BrokeredInfo
	clsname = "Info"

	@staticmethod
	def encode(obj, include=False):
		res = {"k":obj._key}
		if include:
			res['f'] = f = dict()
			for k in obj._meta.fields.keys():
				f[k] = getattr(obj,k)
		return res

	@staticmethod
	def decode(k=None,f=None,m=None):
		k = tuple(k)
		from .loader import get
		assert f is None
		return get(k)

