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

# This test runs the test environment's local queue implementation.

from dabroker import patch; patch()
from dabroker.util.thread import Main

from gevent import spawn,sleep

from tests import test_init,LocalQueue

logger = test_init("test.09.localmsg")

counter = 0

def quadrat(msg):
    logger.debug("Server: got %r",msg)
    b.q.notify(msg*10)
    return msg*msg

def hello(msg):
    global counter
    logger.debug("Client: got %r",msg)
    counter += msg
    

class Broker(Main):
    def setup(self):
        self.q = LocalQueue(quadrat,hello)
        super(Broker,self).setup()
    def stop(self):
        self.q.shutdown()
        super(Broker,self).stop()

    def mult(self,i):
        global counter
        res = self.q.send(i)
        logger.debug("Sent %r, got %r",i,res)
        counter += res
        
    def main(self):
        for i in range(3):
            spawn(self.mult,i+1)
        sleep(0.5)

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()

assert counter == 1+4+9+10+20+30,counter

logger.debug("Exiting")
