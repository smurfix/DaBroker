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
import inspect

from . import CC_MSG,CC_DICT,CC_DATA
from ..util import attrdict, import_string, uuidstr

@asyncio.coroutine
def coro_wrapper(proc, *a,**k):
	"""\
		This code is responsible for turning whatever callable you pass in
		into a "yield from"-style coroutine.
		"""
	did_call = False
	if inspect.iscoroutinefunction(proc):
		proc = proc(*a,**k)
	if inspect.isawaitable(proc):
		return (yield from proc.__await__())
	if inspect.iscoroutine(proc):
		return (yield from proc)
	return proc(*a,**k)

class RPCservice(object):
	"""\
		This object handles one specific RPC service
		"""
	queue = None
	is_alert = None
	call_conv = None

	def __init__(self, fn,name=None, call_conv=CC_MSG):
		if name is None:
			name = fn.__module__+'.'+fn.__name__
		self.fn = fn
		self.name = name
		self.call_conv = call_conv
		self.uuid = uuidstr()
	
	async def run(self, *a,**k):
		res = await coro_wrapper(self.fn,*a,**k)
		return res

