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

# This test does the same thing as test22, except that all mangling
# happens on the server side.

import os
import sys
from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
from dabroker.server.loader.sqlalchemy import SQLLoader
from dabroker.base import BrokeredInfo, Field, BaseObj
from dabroker.client.service import BrokerClient
from dabroker.util import cached_property

from gevent import spawn
from gevent.event import AsyncResult

from dabroker.util.tests import test_init,TestMain,TestClient

logger = test_init("test.24.sql")
logger_s = test_init("test.24.sql.server")

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine
 
Base = declarative_base()
 
# Standard SQLAlchemy example

class Person(Base):
	__tablename__ = 'person'
	# Here we define columns for the table person
	# Notice that each column is also a normal Python instance attribute.
	id = Column(Integer, primary_key=True)
	name = Column(String(250), nullable=False)
 
class Address(Base):
	__tablename__ = 'address'
	# Here we define columns for the table address.
	# Notice that each column is also a normal Python instance attribute.
	id = Column(Integer, primary_key=True)
	street = Column(String(250))
	person_id = Column(Integer, ForeignKey('person.id'))
	person = relationship(Person,backref='addrs')

try:
	os.unlink('/tmp/test24.db')
except EnvironmentError:
	pass
engine = create_engine('sqlite:////tmp/test24.db', echo=(True if os.environ.get('TRACE',False) else False))
Base.metadata.create_all(engine)

DBSession = sessionmaker(bind=engine)

done = 0

class Test24_server(BrokerServer):
	seq = 0
	@cached_property
	def root(self):
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Field("data"))
		self.add_static(rootMeta,1)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"
			data = {}

		root = RootObj()
		self.add_static(root,2,24)

		sql = SQLLoader(DBSession,self)
		sql.add_model(Person,root.data)
		sql.add_model(Address,root.data)
		self.loader.add_loader(sql)

		return root

	def do_trigger(self,msg):
		self.seq += 1
		self.root.hello = "Step "+str(self.seq)
		self.send_updated(self.root)
		self.send("trigger",msg)
	
	def do_mangle_new(self,P,*key,**kw):
		logger.debug("mangle: new: %s %r %r",P,key,kw)
		res = self.obj_new(P,*key,**kw)
		logger.debug("mangle: new done")
		return res
	do_mangle_new.include = True

	def do_mangle_update(self,p,**kw):
		logger.debug("mangle: update: %s %r",p,kw)
		self.obj_update(p,**kw)
		logger.debug("mangle: update done")

	def do_mangle_delete(self,p):
		logger.debug("mangle: delete: %s",p)
		self.obj_delete(p)
		logger.debug("mangle: delete done")

class Test24_client(TestClient):
	def __init__(self,*a,**k):
		self.a = [None]
		for i in range(3):
			self.a.append(AsyncResult())
		super(Test24_client,self).__init__(*a,**k)

	def make_client(self):
		return TestClientMain()

	def do_trigger(self,msg):
		ar = self.a[msg]
		self.a[msg] = AsyncResult()
		ar.set(None)

	def jump(self,i,n):
		if i and n:
			logger.debug("GOTO {} to {}".format(i,n))
		elif n:
			logger.debug("GOTO trigger {}".format(n))
		elif i:
			logger.debug("GOTO {} waits".format(i))

		if n:
			self.send("trigger",n)
		if i:
			self.a[i].get()
			logger.debug("GOTO {} runs".format(i))

	@property
	def cid(self):
		return self.transport.next_id

	def job1(self):
		self.jump(1,0)
		logger.debug("Get the root")
		res = self.root
		logger.debug("recv %r",res)
		assert res.hello == "Step 1", res.hello
		P = res.data['Person']
		A = res.data['Address']
		assert P.name == 'Person',P.name
		r = P.find()
		assert len(r) == 0, r

		# A: create
		p1 = self.send("mangle_new", P, name="Fred Flintstone")
		q1 = self.send("mangle_new", P, name="Barney Rubble")
		a1 = self.send("mangle_new", A, street="Rubble Way", person=p1)

		self.jump(1,2) # goto B

		assert res.hello == "Step 1", res.hello
		res = res._key() # refresh
		assert res.hello == "Step 4", res.hello

		# D: refresh and check
		p2 = p1._key()
		assert p1.name == "Fred Flintstone", p1.name
		assert p2.name == "Freddy Firestone", p2.name
		assert a1.person is p2

		self.jump(0,2) # goto E

		global done
		done |= 1

	def job2(self):
		logger.debug("Get the root 2")
		res = self.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		A = res.data['Address']
		
		self.jump(2,0)

		# B: check+modify
		p1 = P.get(name="Fred Flintstone")
		q1 = P.get(name="Barney Rubble")
		a1 = A.get(street="Rubble Way")
		self.send("mangle_update",p1,name="Freddy Firestone")
		self.send("mangle_update",q1,person=q1)

		self.jump(2,3) # goto C

		# E: delete
		self.send("mangle_delete",p1)
		self.send("mangle_delete",q1)

		self.jump(0,3) # goto F

		global done
		done |= 2
	
	def job3(self):
		self.jump(3,0)

		# C: check
		session = DBSession()
		res = list(session.query(Person))
		assert len(res)==2,res
		res = res[0]
		assert res.name=="Freddy Firestone",res.name
		del session

		self.jump(3,1) # goto D

		# F: check
		session = DBSession()
		res = list(session.query(Person))
		assert len(res)==0

		global done
		done |= 4

	def main(self):
		j1 = spawn(self.job1)
		j2 = spawn(self.job2)
		j3 = spawn(self.job3)
		self.jump(0,1)
		j1.join()
		j2.join()
		j3.join()

class Tester(TestMain):
	client_factory = Test24_client
	server_factory = Test24_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()
assert done==7,done

logger.debug("Exiting")
