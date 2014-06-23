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

# This implements the main broker server.

from ..base.serial import encode,decode

from traceback import format_exc

import logging
logger = logging.getLogger("dabroker.server.service")

class BrokerServer(object):
    def __init__(self):
        pass

    def do_echo(self,msg):
        logger.debug("Echo %r",msg)
        return msg

    def recv(self, msg):
        """Basic message receiver. Ususally in a separate thread."""
        logger.debug("recv raw %r",msg)
        msg = decode(msg)
        logger.debug("recv dec %r",msg)
        job = msg.pop('a')

        try:
            msg = getattr(self,'do_'+job)(msg)
            logger.debug("send dec %r",msg)
            msg = encode(msg)
            logger.debug("send raw %r",msg)
            return msg
        except Exception as e:
            tb = format_exc()
            return {'error': str(e), 'trace':tb}



        


        
