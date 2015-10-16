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

import weakproxy
import amqp
from threading import Thread

class Connection(object):
	task = None # background reader
	amqp = None # connection

	def __init__(self,unit):
		self.unit = weakproxy.ref(unit)
		cfg = unit.config.amqp
		self.amqp = amqp.connection.Connection(**cfg.server)
			self.setup_channels()
	except Exception as e:
			logger.exception("Not connected to AMPQ: host=%s vhost=%s user=%s", self.cfg['amqp_host'],self.cfg['amqp_virtual_host'],self.cfg['amqp_user'])
			c,self.connection = self.connection,None
			if c is not None:
					c.close()
			self.disconnected()
			raise

	def run(self):
		self.task = Thread(target=_run, args=(self,))
		pass
	def close(self):
		a,self.amqp = self.amqp,None
		if a is not None and a.transport is not None:
			try:
				a.close()
			except Exception:
				logger.exception("closing the connection")
			if a.transport is not None:
				try:
					a._do_close() # layering violation
				except Exception:

		t,self.task = self.task,None
		if t is not None:
			try:
				t.join(10)
			except Exception:
				logger.exception("stopping the reader task")

	def __del__(self):
		a,self.amqp = self.amqp,None
		if a is not None and a.transport is not None:
			a._do_close() # layering violation

def _run(self):
	self = weakref.ref(self)
	while True:
		amqp = None
		try:
			amqp = self().amqp
			if amqp.transport is None:
				return # got closed
			amqp.drain_events()
		except ReferenceError:
			return
		except Exception:
			logger.exception("reading from AMQP")
			if amqp is not None:
				try:
					amqp.close()
					while amqp.transport is not None:
						amqp.drain_events(timeout=10)
				except Exception:
					logger.exception("closing AMQP")
			return

