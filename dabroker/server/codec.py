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

from ..base import BaseRef,BaseObj,BrokeredInfo,BrokeredInfoInfo, adapters as baseAdapters, common_BaseObj,common_BaseRef
from ..base.config import default_config
from ..base.service import current_service
from hashlib import sha1 as mac
from base64 import b64encode
from six import integer_types

# This is the server's storage side.

adapters = baseAdapters[:]

secret = None

def make_secret(key):
	"""Generate a salted hash of the key, so that a client is unable to simply enumerate objects."""
	assert key

	global secret
	if secret is None:
		secret = mac(default_config['SECRET_KEY'].encode('utf-8'))
	m = secret.copy()
	for k in key:
		if isinstance(k,integer_types):
			k = str(k)
		if not isinstance(k,bytes):
			k = k.encode('utf-8')
		m.update(b'\0'+k)
	return b64encode(m.digest()[0:6]).decode('ascii')

def codec_adapter(cls):
	adapters.append(cls)
	return cls

@codec_adapter
class server_BaseObj(common_BaseObj):
	@staticmethod
	def encode(obj, include=False):
		if obj._key.code is None:
			obj._key.code = make_secret(obj._key.key)
		if not include:
			return common_BaseRef.encode(obj._key, meta=obj._meta)
		return common_BaseObj.encode(obj, include=include)

	@staticmethod
	def decode(k=None,c=None,f=None,r=None):
		res = server_BaseRef.decode(k=k,c=c)
		if f:
			for k,v in f.items():
				if getattr(res,k) != v:
					raise NotImplementedError("Update: {} {} {} {}".format(res,k,getattr(res,k),v))
		if r:
			for k,v in r.items():
				if getattr(res,k) != v:
					raise NotImplementedError("Update: {} {} {} {}".format(res,k,getattr(res,k),v))
		return res

@codec_adapter
class server_BaseRef(common_BaseRef):
	cls = BaseRef
	clsname = "Ref"

	@staticmethod
	def encode(obj, include=False):
		if include:
			raise RuntimeError("Cannot deref here")
		if obj.code is None:
			obj.code = make_secret(obj.key)
		return common_BaseRef.encode(obj, include=include)

	@staticmethod
	def decode(k=None,c=None):
		res = BaseRef(key=k,code=c)
		if c is None:
			return res
		assert c == make_secret(k)
		res = current_service.top.get(res)
		return res

@codec_adapter
class server_InfoObj(server_BaseObj):
	cls = BrokeredInfo
	clsname = "Info"

	@staticmethod
	def decode(f=None,**kw):
		assert f is None
		return server_BaseObj.decode(**kw)

@codec_adapter
class server_InfoMeta(object):
    cls = BrokeredInfoInfo
    clsname = "_ROOT"

    @staticmethod
    def encode(obj, include=False):
        return {}

    @staticmethod
    def decode(**attr):
        return broker_info_meta

