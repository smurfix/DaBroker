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

# Some sqlalchemy helpers. They should be in dabroker.server.loader.sqlalchemy,
# but the separation makes sense (setup there / production code here).

from .thread import local_object
from sqlalchemy.inspection import inspect
from functools import wraps
from contextlib import contextmanager

import logging
logger = logging.getLogger("dabroker.server.loader.sqlalchemy")

_session = local_object()
_sqlite_warned = False

@contextmanager
def session_wrapper(obj):
	"""Provide a transactional scope around a series of operations."""
	loader = obj._dab.loader

	s_name = loader.id
	s = getattr(_session,s_name,None)
	if s is None:
		logger.debug("new session")
		s = loader.session()
		setattr(_session,s_name,s)
		if s.transaction is None:
			s.begin()
		s._dab_wrapped = 1
	elif not s._dab_wrapped:
		if s.transaction is None:
			s.begin()
		s._dab_wrapped = 1
	else:
		s._dab_wrapped += 1
		if ".dialects.sqlite." in s.bind.dialect.__class__.__module__:
			global _sqlite_warned
			if not _sqlite_warned:
				_sqlite_warned = True
				logger.warn("sqlite does not understand nested transactions")
			try:
				yield s
			finally:
				s._dab_wrapped -= 1
			return
		else:
			logger.debug("existing session")
			s.begin_nested()
	try:
		yield s
	except:
		s.rollback()
		raise
	else:
		s.commit()
	finally:
		s._dab_wrapped -= 1

def with_session(fn):
	@wraps(fn)
	def wrapper(self,*a,**k):
		with session_wrapper(self) as s:
			return fn(self,s, *a,**k)
	return wrapper

