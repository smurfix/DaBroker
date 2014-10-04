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

from weakref import WeakValueDictionary

from blinker._utilities import hashable_identity, reference, WeakTypes

from ..base import BaseRef,BaseObj
from ..base.service import current_service

_ref_cache = WeakValueDictionary()
class ClientBaseRef(BaseRef):
	"""DaBroker-controlled references to objects on the client."""
	def __new__(cls, key=None, meta=None, code=None):
		assert isinstance(key,tuple),key
		self = _ref_cache.get(key,None)
		if self is not None:
			return self
		return super(ClientBaseRef,cls).__new__(cls)
	def __init__(self, key=None, meta=None, code=None):
		if key in _ref_cache: return
		super(ClientBaseRef,self).__init__(key=key,meta=meta,code=code)
		self._dab = current_service.top
		_ref_cache[key] = self

	def __call__(self):
		return self._dab.get(self)

	### signalling stuff

	# inspired by blinker.base.signal(), except that I don't have a sender.
	# Instead I have a mandatory positional `signal` argument that's sent
	# to receivers

	def connect(self, receiver, weak=True):
		"""\
			Connect a signal receiver to me.

			`proc` will be called with two positional arguments: the
			destination object and the signal that's transmitted. Whatever
			keywords args the sender set in its .send() call are passed
			as-is.
			"""
		if not hasattr(self,'_receivers'):
			self._receivers = dict()

		receiver_id = hashable_identity(receiver)
		if weak:
			receiver_ref = reference(receiver, self._cleanup_receiver)
			receiver_ref.receiver_id = receiver_id
		else:
			receiver_ref = receiver
		self._receivers.setdefault(receiver_id, receiver_ref)
	
	def send(self, sig, **k):
		"""Distribute a signal locally."""
		receivers = getattr(self,'_receivers',None)
		if receivers is None:
			return
		disc = []
		for id,r in receivers.items():
			if r is None:
				continue
			if isinstance(r, WeakTypes):
				r = r()
				if r is None:
					disc.append(id)
					continue
			try:
				r(self,sig,**k)
			except Exception:
				logger.exception("Signal error: %s %s",repr(sig),repr(k))
		for id in disc:
			self._disconnect(id)

	def disconnect(self, receiver):
		"""Disconnect *receiver* from this signal's events.

		:param receiver: a previously :meth:`connected<connect>` callable
		"""
		receiver_id = hashable_identity(receiver)
		self._disconnect(receiver_id)

	def _disconnect(self, receiver_id):
		self._receivers.pop(receiver_id, None)
		if not self._receivers:
			del self._receivers

	def _cleanup_receiver(self, receiver_ref):
		"""Disconnect a receiver from all senders."""
		self._disconnect(receiver_ref.receiver_id)

class ClientBaseObj(BaseObj):
	"""base for all DaBroker-controlled objects on the client."""
	_obsolete = False

	def __init__(self):
		super(ClientBaseObj,self).__init__()
		self._refs = {}
	
	def _attr_key(self,k):
		return self._refs[k]
	
	def _obsoleted(self):
		"""Called from the server to mark this object as changed or deleted.
			To determine which, try to fetch the new version via `self._key()`."""
		self._obsolete = True

