#!/usr/bin/env python
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

## change the default encoding to UTF-8
## this is a no-op in PY3
## PY2 defaults to ASCII, but that's way beyond obsolete
import sys
try:
	reload(sys)
except NameError:
	# py3 doesn't have that
	pass
else:
	# py3 doesn't have this either
	sys.setdefaultencoding("utf-8")

## Use gevent?
if True:
	## You get spurious errors if the core threading module is imported
	## before monkeypatching.
	if 'threading' in sys.modules:
		raise Exception('threading module loaded before patching!')

	## All OK, so now go ahead.
	## This MUST be called outside of any import
	def patch():
		import gevent.monkey
		gevent.monkey.patch_all()

else:
	def patch():
		pass

# Warnings are bad, except for some which are not
from warnings import filterwarnings
filterwarnings("error")
filterwarnings("ignore",category=DeprecationWarning)
filterwarnings("ignore",category=PendingDeprecationWarning)
filterwarnings("ignore",category=ImportWarning)
filterwarnings("ignore",message="^Converting column '.*' from VARCHAR to TEXT") # mysql special

