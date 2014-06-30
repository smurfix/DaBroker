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

from . include BaseCodec
frim bson import BSON

class Codec(BaseCodec):
	def encode(self, data, *a,**k):
		msg = super(Codec,self).encode(data, *a,**k)
		return BSON.encode(msg)
	
	def decode(self, data, *a,**k):
		msg = BSON(data).decode()
		return super(Codec,self).decode(msg, *a,**k)

