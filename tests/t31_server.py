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

import os
import sys
from dabroker.server.service import BrokerServer
from dabroker.base import BrokeredInfo, Field,Callable, BaseObj
from dabroker.util import cached_property,exported
from dabroker.util.thread import AsyncResult

from dabroker.util.tests import test_init
logger = test_init("test.31.amqp.speed")

class TestServer(BrokerServer):
	@cached_property
	def root(self):
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Field("data"))
		rootMeta.add(Callable("pling"))
		self.add_static(rootMeta,1)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"
			data = {}

			@exported
			def pling(self,msg,**k):
				return {'info':"Yes I know", 'root':k['root']}

		root = RootObj()
		self.add_static(root,2,31)
		return root
	
