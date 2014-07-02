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

# This test mangles SQL, courtesy of sqlalchemy.

import os
import sys
from dabroker import patch; patch()
from dabroker.server.service import BrokerServer
from dabroker.server.loader.sqlalchemy import SQLLoader
from dabroker.base import BrokeredInfo, Field,Ref,Callable, BaseObj
from dabroker.client.service import BrokerClient

from gevent import spawn,sleep
from gevent.event import AsyncResult

from tests import test_init,LocalQueue,TestMain,TestClient

logger = test_init("test.22.sql")
logger_s = test_init("test.22.sql.server")

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
	street_name = Column(String(250))
	street_number = Column(String(250))
	post_code = Column(String(250), nullable=False)
	person_id = Column(Integer, ForeignKey('person.id'))
	person = relationship(Person,backref='addrs')

try:
	os.unlink('/tmp/test22.db')
except EnvironmentError:
	pass
engine = create_engine('sqlite:////tmp/test22.db', echo=(True if os.environ.get('TRACE',False) else False))
Base.metadata.create_all(engine)

DBSession = sessionmaker(bind=engine)

done = 0

class Test22_server(BrokerServer):
	_root = None

	@property
	def root(self):
		if self._root is not None:
			return self._root
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Field("data"))
		self.loader.static.add(rootMeta,1)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"
			data = {}

		root = RootObj()
		self.loader.static.add(root,2,99)

		sql = SQLLoader(DBSession,self)
		sql.add_model(Person,root.data)
		sql.add_model(Address)
		self.loader.add_loader(sql)

		self._root = root
		return root

	def do_trigger(self,msg):
		self.send("trigger",msg)
	
class Test22_client(TestClient):
	def __init__(self,*a,**k):
		self.a = [None]
		for i in range(3):
			self.a.append(AsyncResult())
		super(Test22_client,self).__init__(*a,**k)

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

	def ref(self,p):
		k = p._key
		def res():
			return self.get(k)
		return res

	@property
	def cid(self):
		return self.transport.next_id

	def job1(self):
		self.jump(1,0)
		logger.debug("Get the root")
		res = self.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		P = res.data['Person']
		assert P.name == 'Person',P.name
		r = P.find()
		assert len(r) == 0, r

		# A: create
		p1 = P.new(name="Fred Flintstone")
		p1r = self.ref(p1)
		self.commit()

		self.jump(1,2) # goto B

		# D: check
		p1 = p1r()
		assert p1.name == "Freddy Firestone", p1.name

		self.jump(0,2) # goto E

		global done
		done |= 1

	def job2(self):
		logger.debug("Get the root 2")
		res = self.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		
		self.jump(2,0)

		# B: check+modify
		p1 = P.get(name="Fred Flintstone")
		p1.name="Freddy Firestone"
		self.commit()

		self.jump(2,3) # goto C

		# E: delete
		P.delete(p1)
		self.commit()

		self.jump(0,3) # goto F

		global done
		done |= 2
	
	def job3(self):
		self.jump(3,0)

		# C: check
		session = DBSession()
		res = list(session.query(Person))
		assert len(res)==1
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
	client_factory = Test22_client
	server_factory = Test22_server

t = Tester()
t.register_stop(logger.debug,"shutting down")
t.run()
assert done==7,done

logger.debug("Exiting")
