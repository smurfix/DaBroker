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

# This test mangles SQL, courtesy of sqlalchemy.

import os
import sys
from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
from dabroker.server.loader.sqlalchemy import SQLLoader
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj
from dabroker.client.service import BrokerClient

from gevent import spawn,sleep
from gevent.event import AsyncResult

from tests import test_init,LocalQueue,TestMain
from tests.t30_run import run_server,run_client

cfg = dict(userid='test', password='test', virtual_host='test')

e = AsyncResult()
s = spawn(run_server,ready=:e,config=cfg)

if not e.wait(5):
	s.kill()
	raise RuntimeError("The server did not start up")
c = spawn(run_client, config=cfg)

c.join(timeout=5)
if not c.ready():
	s.kill()
	raise RuntimeError("The client did not end")

s.kill() # TODO create a message for this

