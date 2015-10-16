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

__VERSION__ = (0,1,0)

# Not using gevent is not yet supported
# mainly because you can't kill/cancel OS threads from within Python
USE_GEVENT=True

## change the default encoding to UTF-8
## this is a no-op in PY3
# PY2 defaults to ASCII, which requires adding spurious .encode("utf-8") to
# absolutely everything you might want to print / write to a file
import sys
try:
	reload(sys)
except NameError:
	# py3 doesn't have reload()
	pass
else:
	# py3 also doesn't have sys.setdefaultencoding
	sys.setdefaultencoding("utf-8")

def patch():
	"""\
		Patch the system for the correct threading implementation (gevent or not).
		This function MUST be called as early as possible.
		It MUST NOT be called from within an import.
		"""

	if USE_GEVENT:
		## You get spurious errors if the core threading module is imported
		## before monkeypatching.
		if 'threading' in sys.modules:
			raise Exception('The ‘threading’ module was loaded before patching for gevent')
		import gevent.monkey
		gevent.monkey.patch_all()

	else:
		pass

# Warnings are bad, except for some which are not
from warnings import filterwarnings
filterwarnings("error")
#filterwarnings("ignore",category=DeprecationWarning)
#filterwarnings("ignore",category=PendingDeprecationWarning)
#filterwarnings("ignore",category=ImportWarning)
filterwarnings("ignore",message="^Converting column '.*' from VARCHAR to TEXT") # mysql special

def unit(app, cfg="/etc/dabroker.cfg", **args):
	"""Return the DaBroker unit for this app."""
	from dabroker.unit import Unit
	return Unit(app,cfg, **args)

