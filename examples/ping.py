#!/usr/bin/python3
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

import asyncio
from dabroker.unit import Unit
from dabroker.util.tests import load_cfg
import logging
import sys
from pprint import pprint
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

u=Unit("test.ping", load_cfg("test.cfg")['config'])

async def cb(r):
	pprint(r.data)

async def example():
	await u.start()
	await asyncio.sleep(0.2) # allow monitor to attach
	try:
		await asyncio.wait_for(u.alert("dabroker.ping",callback=cb), timeout=3)
	except asyncio.TimeoutError:
		pass
	finally:
		await u.stop()

def main():
	loop = asyncio.get_event_loop()
	loop.run_until_complete(example())
try:
	main()
except KeyboardInterrupt:
	pass

