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

# generic test setup

import logging,sys,os
logger = logging.getLogger("tests")

def test_init(who):
    if os.environ.get("TRACE","0") == '1':
        level = logging.DEBUG
    else:
        level = logging.WARN

    logger = logging.getLogger(who)
    logging.basicConfig(stream=sys.stderr,level=level)

    return logger

# reduce cache sizes and timers

from dabroker.client import service as s
from dabroker.util.thread import Main

s.RETR_TIMEOUT=1 # except that we want 1000 when debugging
s.CACHE_SIZE=5

# local queue implementation

try:
    from queue import Queue
except ImportError:
    from Queue import Queue
from traceback import format_exc
from bson import BSON

class RPCmessage(object):
    msgid = None
    def __init__(self,p,msg):
        self.p = p
        self.msg = msg

    def reply(self,msg):
        logger.debug("Reply to %s with %r", self.msgid,msg)
        msg = BSON.encode(msg)
        msg = RPCmessage(self.p,msg)
        msg.msgid = self.msgid
        self.p.reply_q.put(msg)
        
class ServerQueue(object):
    def __init__(self,p,worker):
        self.p = p
        self.next_id = -1
        self.worker = worker

    def _worker(self,msg):
        try:
            m = msg.msg
            m = BSON(m).decode()
            res = self.worker(m)
        except Exception as e:
            res = {'error':str(e),'tb':format_exc()}
        msg.reply(res)

    def _reader(self):
        from gevent import spawn
        while True:
            logger.debug("Server: wait for message")
            msg = self.p.request_q.get()
            logger.debug("Server: get msg %s",msg.msgid)
            spawn(self._worker,msg)
    
    def send(self,msg):
        msg = BSON.encode(msg)
        msg = RPCmessage(self.p,msg)
        msg.msgid = self.next_id
        self.next_id -= 1
        self.p.reply_q.put(msg)

class ClientQueue(object):
    def __init__(self,p,worker):
        self.p = p
        self.q = {}
        self.next_id = 1
        self.worker = worker

    def _reader(self):
        while True:
            msg = self.p.reply_q.get()
            if msg.msgid < 0:
                m = BSON(msg.msg).decode()
                self.worker(m)
            else:
                r = self.q.pop(msg.msgid,None)
                if r is not None:
                    m = msg.msg
                    m = BSON(m).decode()
                    r.set(m)
        
    def send(self,msg):
        from gevent.event import AsyncResult

        msg = BSON.encode(msg)
        msg = RPCmessage(self.p,msg)
        msg.msgid = self.next_id
        res = AsyncResult()
        self.q[self.next_id] = res
        self.next_id += 1

        self.p.request_q.put(msg)
        res = res.get()
        return res
    
class LocalQueue(object):
    def __init__(self, server_worker, client_worker=None):
        from gevent import spawn

        self.request_q = Queue()
        self.reply_q = Queue()

        sq = ServerQueue(self,server_worker)
        cq = ClientQueue(self,client_worker)
        self.server = spawn(sq._reader)
        self.client = spawn(cq._reader)
        self.cq = cq
        self.sq = sq

    def set_client_worker(self,worker):
        self.cq.worker = worker

    def set_server_worker(self,worker):
        self.sq.worker = worker

    def send(self,msg):
        return self.cq.send(msg)

    def notify(self,msg):
        return self.sq.send(msg)

    def shutdown(self):
        r = self.client; self.client = None
        if r is not None:
            r.kill()

        r = self.server; self.server = None
        if r is not None:
            r.kill()

from gevent import spawn,sleep
def killer(x,t):
    sleep(t)
    x.killer = None
    x.stop()

class TestMain(Main):
    def setup(self):
        super(TestMain,self).setup()
        self.killer = spawn(killer,self,5)
    def cleanup(self):
        super(TestMain,self).cleanup()
        if self.killer is not None:
            self.killer.kill()

