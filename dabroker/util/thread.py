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

import gevent
from gevent.queue import Queue
from signal import SIGINT

from flask._compat import string_types
	
logger = logging.getLogger("dabroker.util.thread")

class Main(object):
	"""\
		This class represents DaBroker's (or indeed any) main task.

		Specifically, it installs a hook for cleanly shutting down upon receiving SIGINT.

		Use .register_stop() to install cleanup code.
		"""
	_plinker = None
	_sigINT = None
	_main = None
	_stops = []
	_stopping = False

	### Methods you override
	def setup(self):
		"""Override this to initialize everything"""
		pass
	def main(self):
		"""Override this with your main code"""
		raise NotImplementedError("You forgot to override %s.main" % (self.__class__.__name__,))
	def stop(self):
		"""Override this if you don't just want a killed task"""
		logger.debug("Killing main task")
		if self._main:
			self._main.kill(timeout=5)
	def cleanup(self):
		"""Override this to clean up after yourself. 'task' is the gevent task of the main loop"""
		pass
	
	### Public methods
	def __init__(self):
		self.stops = []

	def run(self):
		"""Start the main loop"""
		try:
			logger.debug("Setting up")
			self._setup()
			logger.debug("Main program starting")
			self._main = gevent.spawn(self.main)
			self._main.join()
			self._main = None
		except Exception:
			logger.exception("Main program died")
		else:
			logger.debug("Main program ended")
		finally:
			logger.debug("Cleanup starts")
			self._cleanup()
			logger.debug("Cleanup ends")

	def end(self):
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
		if self._stopping:
			if self._stopping == gevent.getcurrent():
				try:
					raise RuntimeError()
				except RuntimeError:
					logger.exception("Cleanup entered from cleanup task.")
				return
			else:
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
			self.stop()
		except Exception:
			logger.exception("Cleanup code")
		finally:
			logger.debug("Killing main task again(?)")
			if self._main:
				self._main.kill(timeout=5)

		for j,a,k in self._stops:
			logger.debug("Running %s",j)
			try:
				j(*a,**k)
			except Exception:
				logger.exception("Running %s",j)
			else:
				logger.debug("Running %s",j)

		if self._plinker:
			self._plinker.kill()
			self._plinker = None
		logger.debug("Cleanup done.")

