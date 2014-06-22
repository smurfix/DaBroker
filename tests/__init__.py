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

def test_init(who):
    import logging,sys,os
    if os.environ.get("TRACE","0") == '1':
        level = logging.DEBUG
    else:
        level = logging.WARN

    logger = logging.getLogger(who)
    logging.basicConfig(stream=sys.stderr,level=level)

    return logger
