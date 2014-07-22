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

# This test verifies that data structures which are not a tree, or even a
# DAG (Directed Acyclic Graph), are transmitted correctly.

from dabroker import patch; patch()

from gevent import spawn

from dabroker.util.tests import test_init,LocalQueue,TestMain,TestRoot,TestClient,TestServer
from dabroker.client.service import BrokerClient

logger = test_init("test.10.selfref")

counter = 0

def make_message():
	"""This is nicely self-referential and whatnot"""
	m = {'m':1}
	mm = {'mm':2}
	foo=['f','oo']
	m['foo'] = foo
	mm['foo'] = foo
	foo.append(foo)

	m['mm1'] = mm
	m['mm2'] = mm
	mm['mm'] = mm

	bar = ['a','b','c']
	baz = ['d','e','f']
	bar.append(baz)
	baz.append(bar)
	mm['bar'] = bar
	mm['baz'] = baz
	return m

def check_message(m):
	assert m['m'] == 1,m['m']
	mm = m['mm1']
	foo = m['foo']
	assert foo[0] == 'f'
	assert foo[1] == 'oo'
	assert foo[2] is foo
	assert mm is m['mm2']
	bar = mm['bar']
	baz = mm['baz']
	assert bar[3] is baz
	assert bar is baz[3]

# Server's root object
class Test10_root(TestRoot):
	def __init__(self,server):
		self._server = server
		super(Test10_root,self).__init__()
	def callme(self,msg):
		check_message(msg)
		self._server.send("note",{'note':msg})
		return msg

class Test10_client(TestClient):
	def main(self):
		jobs = []
		for i in range(3):
			jobs.append(spawn(self.check,i+1))
		for j in jobs:
			j.join()

	def check(self,i):
		global counter
		msg = make_message()
		res = self.root.callme(msg)
		logger.debug("Sent %r, got %r",i,res)
		check_message(res)
		assert msg is not res
		counter += 1
	
	def do_note(self,msg):
		global counter
		logger.debug("more %s",msg)
		check_message(msg['note'])
		counter += 1

class Test10_server(TestServer):
	@property
	def root(self):
		return Test10_root(self)

class Tester(TestMain):
	client_factory = Test10_client
	server_factory = Test10_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()

assert counter == 6,counter

logger.debug("Exiting")
