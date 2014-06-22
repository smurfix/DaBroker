# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, division, unicode_literals
##
## This is part of DaBroker, a distributed data access manager.
##
## DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
## it is licensed under the GPLv3. See the file `README.rst` for details,
## including an optimistic statements by the author.
##
## This paragraph is auto-generated and may self-destruct at any time,
## courtesy of "make update". The original is in ‘utils/_boilerplate.py’.
## Thus, please do not remove the next line, or insert any blank lines.
##BP

from dabroker import patch; patch()

from dabroker.broker import Broker
from gevent import spawn,sleep

from dabroker.util.thread import Main

from tests import test_init
logger = test_init("test.basic")

class Broker(Main):
    def main(self):
        logger.debug("Startup")
        sleep(0.5)
        logger.error("did not kill me")


def killme():
    logger.debug("started killer task")
    sleep(0.2)
    logger.debug("Terminating")
    b.stop()
waiter = spawn(killme)

b = Broker()
b.register_stop(waiter.kill) # not necessary here
b.register_stop(logger.debug,"shutting down")
b.run()
logger.debug("Exiting")



