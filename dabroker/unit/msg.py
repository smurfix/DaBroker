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

import uuid
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
			setattr(self, _fmap[f], data.get(v,None))

class ReturnedError(RuntimeError):
	def __init__(self,err,msg=None):
		self.error = err
		self.message = msg
	
	def __str__(self):
		return self.error.message

class MsgError(_MsgPart):
	fields = "status id part message"

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
	fields = "type version debug"

	data = None
	error = None

	def __init__(self, hdr):
		self.msgid = uuid.uuid1()
		if hdr:
			super(BaseMsg,self)._load(hdr)

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
	def _load(cls, data):
		obj = cls()
		if 'data' in data:
			obj.data = data['data']
		if 'error' in data:
			obj.error = MsgError(data['error'])
		return obj

	@property
	def failed(self):
		return self.error is not None and self.error.failed

	def raise_if_error(self):
		if self.error and self.error.failed:
			raise self.error.returned_error()

class _RequestMsg(BaseMsg):
	"""A request packet. The remaining fields are data elements."""
	fields = "name message-id reply-to"

	def __init__(self, _name, **data):
		super().__init__()
		self.name = _name
		self.data = data
		self.message_id = uuid.uuid1()

	def make_response(self, **data):
		return ResponseMsg(self, **data)

	def make_error_response(self, exc, eid,part, fail=False):
		res = ResponseMsg(self)
		res.error = MsgError.build(exc, eid,part)
		return error

class RequestMsg(_RequestMsg):
	type = "request"

	def __init__(self, _name, _unit, **k):
		super().__init__(_name, **k)
		self.reply_to = _unit.recv_id

class AlertMsg(_RequestMsg):
	"""An alert which is not replied to"""
	type = "alert"

	def __init__(self, _name, _unit, **k):
		super().__init__(_name, **k)
		# do not set reply_to

class PollMsg(AlertMsg):
	"""An alert which requests replies"""
	def __init__(self, _name, _unit, callback, **k):
		next(callback)
		super().__init__(_name, _unit, *a,**k)
		self.reply_to = _unit.recv_id
		self.callback = callback
		self.replies = 0

	@asyncio.coroutine
	def recv_reply(self, f,msg):
		"""Incoming reply. @f is the future to trigger when complete."""
		try:
			if asyncio.iscoroutinefunction(self.callback):
				yield from self.callback(msg)
			else:
				self.callback(msg)
		except StopIteration:
			f.set_result(self.replies+1)
		except Exception as exc:
			f.set_exception(exc)
		else:
			self.replies += 1

class ResponseMsg(BaseMsg):
	type = "reply"
	fields = "in-reply-to"

	def __init__(self,request, **data):
		super().__init__(**data)
		self.in_reply_to = request.message_id

	@asyncio.coroutine
	def recv_reply(self, f,msg):
		"""Client side: Incoming reply. @f is the future to trigger when complete."""
		if msg.failed:
			f.set_exception(msg.error.returned_error())
		else:
			f.set_result(msg.data)

