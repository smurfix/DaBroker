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

import logging
import sys
import pytz
import locale
from functools import wraps
from werkzeug.local import LocalManager,Local,LocalStack

import gevent
from gevent.queue import Queue
from gevent.event import AsyncResult,Event
from signal import SIGINT

from six import string_types
	
logger = logging.getLogger("dabroker.util.thread")

class CloningLocalManager(LocalManager):
	"""\
		A LocalManager with a copy function which copies items from another context.
		The idea is to have contexts which survive a spawn().
		"""
	def copy_from(self,ident):
		for local in self.locals:
			storage = local.__storage__
			if ident in storage:
				storage[local.__ident_func__()] = storage[ident].copy()

local_objects = CloningLocalManager()
cloned_local_objects = CloningLocalManager()
class local_object(Local):
	def __init__(self):
		super(local_object,self).__init__()
		local_objects.locals.append(self)
class cloned_local_object(Local):
	def __init__(self):
		super(cloned_local_object,self).__init__()
		cloned_local_objects.locals.append(self)
class local_stack(LocalStack):
	def __init__(self):
		super(local_stack,self).__init__()
		local_objects.locals.append(self)

aux_cleanup = []

class Thread(object):
	"""\
		A generic thread, intended to be able to be used with greenlets as well as OS threads.

		You need to override `code()` (which gets passed the arguments from __init__()).

		"""
	def __init__(self, *a,**k):
		self.a = a
		self.k = k
		self.job = None
	
	def code(self, *a,**k):
		raise RuntimeError("You forgot to override %s.code()"%(self.__class__.__name__,))

	def _run(self,parent_ident):
		cloned_local_objects.copy_from(parent_ident)
		try:
			return self.code(*self.a,**self.k)
		except gevent.GreenletExit:
			pass
		finally:
			local_objects.cleanup()
			cloned_local_objects.cleanup()
			for p in aux_cleanup:
				p()
			
	def start(self):
		assert self.job is None
		self.job = gevent.spawn(self._run,gevent.getcurrent())
		return self
		
	def stop(self):
		if self.job is not None:
			self.job.kill()

	def kill(self, timeout=None):
		self.stop()
		self.join(timeout)

	def join(self,timeout=None):
		if self.job is not None:
			self.job.join(timeout=timeout)
			self.job = None
	
	@property
	def ready(self):
		if self.job is None:
			return True
		return self.job.ready

def spawned(fn):
	"""\
		A wrapper which runs the procedure in its own thread.
		"""
	@wraps(fn)
	def doit(*a,**k):
		class thr(Thread):
			def code(self,*a,**k):
				fn(*a,**k)
		return thr(*a,**k).start()
	return doit

def prep_spawned(fn):
	"""like `spawned` but does not yet start the thread"""
	@wraps(fn)
	def doit(*a,**k):
		class thr(Thread):
			def code(self,*a,**k):
				fn(*a,**k)
		return thr(*a,**k)
	return doit

class MainThread(Thread):
	"""Adapter from a code snippet to a Thread"""
	def __init__(self, code, *a,**k):
		if code is None:
			import pdb;pdb.set_trace()
		self._code = code
		super(MainThread,self).__init__(*a,**k)

	def code(self,*a,**k):
		return self._code(*a,**k)

class Main(object):
	"""\
		This class represents DaBroker's (or indeed any) main task.

		Specifically, it installs a hook for cleanly shutting down upon receiving SIGINT.

		Use .register_stop() to install cleanup code.

		Common usage:
		
			class MyMainThread(Thread):
				def code(self, args…):
					# do something, or simply …:
					self.shutting_down.wait()
			main = Main(MyMainThread, args…)
			main.run()
		"""
	_plinker = None
	_sigINT = None
	_thread = None
	_stops = None
	_stopping = False

	### Methods you override
	def setup(self):
		"""Override this to initialize everything.
			Do not call this method yourself: .run() does that."""
		pass
	def shutdown(self):
		"""Override this to cleanly terminate your tasks.
			Do not call this method yourself: use .stop() to terminate your program."""
		pass
	def cleanup(self):
		"""Override this to clean up after yourself.
			Do not call this method yourself: .stop() does that."""
		pass

	def main(self):
		"""Main code, if you don't have a separate thread.
			Deprecated / used for testing."""
		raise NotImplementedError("You should use a separate thread for main code")
	
	### Public methods
	def __init__(self, main=None, *a,**k):
		if main is None:
			main = MainThread(self.main)
		self._main = main
		self.a = a
		self.k = k

		self._stops = []
		self.shutting_down = Event()

	def spawn(self,thread,*a,**k):
		"""Start a thread object, and register for stopping"""
		if isinstance(thread,Thread):
			assert not a and not k, (thread,a,k)
		else:
			thread = thread(*a,**k)
		assert thread.ready, thread

		thread.start()
		self.register_stop(thread.stop)
		return thread

	def run(self):
		"""Run the main code"""
		assert self._thread is None and not self.shutting_down.is_set(), "You cannot call .run() twice!"

		try:
			logger.debug("Setting up")
			self._setup()
			logger.debug("Main program starting")
			self._thread = self.spawn(self._main, *self.a,**self.k)
			self._thread.join()
		except Exception:
			logger.exception("Main program died")
		else:
			logger.debug("Main program ended")
		finally:
			logger.debug("Cleanup starts")
			self._cleanup()
			logger.debug("Cleanup ends")

	def stop(self):
		"""Stop the main loop"""
		logger.info("Stop call received.")
		self._cleanup()
		logger.debug("End handler done.")
		
	def register_stop(self,job,*a,**k):
		"""Pass a function for cleanup code"""
		self._stops.insert(0,(job,a,k))
	
	### Internals
	def _setup(self):
		self._sigINT = gevent.signal(SIGINT,self._sigquit)
		self._plinker = gevent.spawn(self._plink)
		self.setup()
		self.register_stop(self.cleanup)

	def _plink(self):
		i=1
		while True:
			gevent.sleep(i)
			logger.debug("I am running")
			i += 1

	def _sigquit(self):
		logger.info("Signal received, trying to terminate.")
		gevent.spawn(self._cleanup)
	
	def _cleanup(self):
		self.shutting_down.set()
		if self._stopping:
			if self._stopping == gevent.getcurrent():
				return
			logger.debug("Cleanup entered again.")
		else:
			self._stopping = gevent.spawn(self._real_cleanup)
		self._stopping.join()

	def _real_cleanup(self):
		logger.debug("Cleanup entered.")
		if self._sigINT:
			self._sigINT.cancel()
			self._sigINT = None

		try:
			self.shutdown()
		except Exception:
			logger.exception("Cleanup code")
		finally:
			logger.debug("Killing main task again(?)")
			t,self._thread = self._thread,None
			if t:
				t.kill(timeout=5)

		for j,a,k in self._stops:
			logger.debug("Running %s",j)
			try:
				j(*a,**k)
			except BaseException:
				logger.exception("Running %s",j)
			else:
				logger.debug("Running %s",j)

		if self._plinker:
			self._plinker.kill()
			self._plinker = None
		logger.debug("Cleanup done.")

