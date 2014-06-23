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

from ..base.serial import encode,decode

import logging
logger = logging.getLogger("dabroker.client.service")

class ServerError(Exception):
    """An encapsulation for a server error (with traceback)"""
    def __init__(self,err,tb):
        self.err = err
        self.tb = tb

    def __repr__(self):
        return "ServerError({})".format(repr(self.err))

    def __str__(self):
        r = repr(self)
        if self.tb is None: return r
        return r+"\n"+self.tb

class BrokerClient(object):
    def __init__(self, server):
        self.server = server

    def send(self, action, msg, **kw):
        logger.debug("send dec %s %r",action,msg)
        msg = encode(msg)
        if isinstance(msg,dict):
            msg.update(kw)
        else:
            kw['m'] = msg
            msg = kw
        msg['a'] = action
        logger.debug("send raw %r",msg)

        msg = self.server(msg)

        logger.debug("recv raw %r",msg)
        msg = decode(msg)
        logger.debug("recv dec %r",msg)

        if 'error' in msg:
            raise ServerError(msg['error'],msg.get('tb',None))
        return msg
