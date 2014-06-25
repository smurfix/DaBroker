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

# This is the main code of the broker.

from ..util.thread import Main
from gevent import sleep

class BaseBroker(Main):
	"""Base class for the DaBroker server"""
	queue = None
	root = None
	
	def make_queue(self):
		raise NotImplementedError("You need to override the queue generator")

	def make_root(self):
		raise NotImplementedError("You need to override the root object generator")

	def setup(self):
		self.queue = self.make_queue()
		self.root = self.make_root()

	def main(self):
		pass
		
class Broker(BaseBroker):
	pass

