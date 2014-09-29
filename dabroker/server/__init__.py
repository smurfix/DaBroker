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

from ..base import BrokeredInfo,Callable

class ServerBrokeredInfo(BrokeredInfo):
	def add_callables(self,model):
		# TODO: this should be in a more generic location
		# and probably somewhat unified with .server.export_class()
		for a in dir(model):
			if a.startswith('_'): continue
			m = getattr(model,a)
			if not getattr(m,'_dab_callable',False): continue
			if getattr(m,'__self__',None) is model: # classmethod
				if self._meta is BrokeredInfoInfo:
					raise RuntimeError("You need a separate metaclass if you want to add class methods")
				self._meta.add(Callable(m.__name__))
			else: # normal method
				self.add(Callable(m.__name__))

from .service import BrokerServer
