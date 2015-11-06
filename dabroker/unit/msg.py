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

import asyncio

from . import CC_MSG,CC_DICT,CC_DATA
from ..util import uuidstr

class _NOTGIVEN:
	pass

_types = {}
_fmap = {}

class FieldCollect(type):
	def __new__(meta, name, bases, dct):
		# Grab all known field names
		s = set()
		for b in bases:
			s.update(getattr(b,'fields',()))
		b = dct.get('fields',"")
		if b:
			if isinstance(b,str):
				b = b.split(" ")
			assert not s.intersection(b), (s,b)
			s.update(b)
		dct['fields'] = s

		# map them, so that obj.field_name goes to field-name
		for n in s:
			_fmap[n] = n.replace('-','_')

		res = super(FieldCollect, meta).__new__(meta, name, bases, dct)
		t = dct.get('type',None)
		if t is not None:
			_types[t] = res
		return res

class _MsgPart(object, metaclass=FieldCollect):

	def dump(self):
		obj = {}
		for f in self.fields:
			try:
				obj[f] = getattr(self, _fmap[f])
			except AttributeError:
				pass
		return obj
	
	def _load(self, data):
		for f in self.fields:
			v = data.get(f,_NOTGIVEN)
			if v is not _NOTGIVEN:
				setattr(self, _fmap[f], v)

class ReturnedError(RuntimeError):
	def __init__(self,err=None,msg=None):
		self.error = err
		self.message = msg
	
	def __str__(self):
		return self.error.message

class MsgError(_MsgPart):
	fields = "status id part message"

	def __init__(self, data=None):
		if data is not None:
			self._load(data)

	@property
	def failed(self):
		if self.status in ('ok','warn'):
			return False
		if self.status in ('error','fail'):
			return True
		raise RuntimeError("Unknown error status: "+str(self.status))
	
	@classmethod
	def build(cls, exc, eid,part, fail=False):
		obj = cls()
		obj.status = "fail" if fail else "error"
		obj.eid = eid
		obj.part = part
		obj.message = str(exc)
		return obj

	def returned_error(self, msg=None):
		assert not msg or msg.error == self
		return ReturnedError(self,msg)

class BaseMsg(_MsgPart):
	version = 1
	debug = False
	# type: needs to be overridden
	fields = "type version debug message-id"

	data = None
	error = None

	def __init__(self, data=None, hdr=None):
		if hdr:
			super(BaseMsg,self)._load(hdr)
		if not hasattr(self,'message_id'):
			self.message_id = uuidstr()

	def dump(self):
		obj = { 'header': super().dump() }
		if self.data is not None:
			obj['data'] = self.data
		if self.error is not None:
			obj['error'] = self.error.dump()
		return obj

	def set_error(self, *a, **k):
		self.error = MsgError.build(*a,**k)

	@staticmethod
	def load(data):
		t = data['header']['type']
		return _types[t]._load(data)

	@classmethod
	def _load(cls, msg):
		obj = cls()
		super(BaseMsg,obj)._load(msg['header'])
		if 'data' in msg:
			obj.data = msg['data']
		if 'error' in msg:
			obj.error = MsgError(msg['error'])
		return obj

	@property
	def failed(self):
		return self.error is not None and self.error.failed

	def raise_if_error(self):
		if self.error and self.error.failed:
			raise self.error.returned_error()

class _RequestMsg(BaseMsg):
	"""A request packet. The remaining fields are data elements."""
	fields = "name reply-to"

	def __init__(self, _name=None, data=None):
		super().__init__()
		self.name = _name
		self.data = data
		self.message_id = uuidstr()

	def make_response(self, **data):
		return ResponseMsg(self, **data)

	def make_error_response(self, exc, eid,part, fail=False):
		res = ResponseMsg(self)
		res.error = MsgError.build(exc, eid,part)
		return error

class RequestMsg(_RequestMsg):
	type = "request"
	_exchange = "rpc" # lookup key for the exchange name
	_timer = "rpc" # lookup key for the timeout

	def __init__(self, _name=None, _unit=None, data=None):
		super().__init__(_name, data)
		if _unit is not None:
			self.reply_to = _unit.uuid

	@asyncio.coroutine
	def recv_reply(self, f,reply):
		"""Client side: Incoming reply. @f is the future to trigger when complete."""
		f.set_result(reply)

class AlertMsg(_RequestMsg):
	"""An alert which is not replied to"""
	type = "alert"
	_exchange = "alert" # lookup key for the exchange name

	def __init__(self, _name=None, _unit=None, data=None):
		super().__init__(_name=_name, data=data)
		# do not set reply_to

class PollMsg(AlertMsg):
	"""An alert which requests replies"""
	_timer = "poll" # lookup key for the timeout

	def __init__(self, _name=None, _unit=None, callback=None,call_conv=CC_MSG, data=None):
		super().__init__(_name=_name, _unit=_name, data=data)
		if _unit is not None:
			self.reply_to = _unit.uuid
		self.callback = callback
		self.call_conv = call_conv
		self.replies = 0

	@asyncio.coroutine
	def recv_reply(self, f,msg):
		"""Incoming reply. @f is the future to trigger when complete."""
		try:
			if self.call_conv == CC_DICT:
				a=(); k=msg.data
			elif self.call_conv == CC_DATA:
				a=(msg.data,); k={}
			else:
				a=(msg,); k={}
			if asyncio.iscoroutinefunction(self.callback):
				yield from self.callback(*a,**k)
			else:
				self.callback(*a,**k)
		except StopIteration:
			f.set_result(self.replies+1)
		except Exception as exc:
			f.set_exception(exc)
		else:
			self.replies += 1

class ResponseMsg(BaseMsg):
	type = "reply"
	fields = "in-reply-to"

	def __init__(self,request=None, data=None):
		super().__init__(data=data)
		if request is not None:
			self.in_reply_to = request.message_id

