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
from tests.t30_server import run_server
from tests.t30_client import run_client

from multiprocessing import Process,Event
e = Event()
s = spawn(target=run_server,kwargs={'ready':e,'config':cfg})
if not e.wait(5):
	s.terminate()
	raise RuntimeError("The server did not start up")
c = Process(target=run_client,kwargs={'config':cfg})

if c.join(5) is not None:
	s.terminate()
	raise RuntimeError("The client did not end")

s.terminate() # TODO create a message for this

