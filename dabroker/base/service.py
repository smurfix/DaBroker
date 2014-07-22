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

# Basic service template

from ..util.thread import local_stack
current_service = local_stack()

class _BrokerEnv(object):
    def __init__(self,base):
        self.base = base
    def __enter__(self):
        current_service.push(self.base)
    def __exit__(self, a,b,c):
        base = current_service.pop()
        assert base is self.base
    
class BrokerEnv(object):
    @property
    def env(self):
        return _BrokerEnv(self)
