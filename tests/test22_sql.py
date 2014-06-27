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

from tests import test_init,LocalQueue,TestMain

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
engine = create_engine('sqlite:////tmp/test22.db', echo=True)
Base.metadata.create_all(engine)

DBSession = sessionmaker(bind=engine)
session = DBSession()

class SearchBrokeredInfo(BrokeredInfo):
	objs = []
	def obj_add(self,obj):
		self.objs.append(obj)
	def obj_find(self,_limit=None,**kw):
		res = []
		for obj in self.objs:
			for k,v in kw.items():
				if getattr(obj,k,None) != v:
					break
			else:
				res.append(obj)
		return res

class TestBrokerClient(BrokerClient):
	def __init__(self,q,*a):
		self.q = q
		super(TestBrokerClient,self).__init__(*a)

	def do_trigger(self,msg):
		self.q.a[msg].set(msg)
	
class TestBrokerServer(BrokerServer):
	def __init__(self,sender=None):
		super(TestBrokerServer,self).__init__(sender=sender)
		rootMeta = BrokeredInfo("rootMeta")
		rootMeta.add(Field("hello"))
		rootMeta.add(Field("data"))
		self.loader.static.add(rootMeta,1)

		class RootObj(BaseObj):
			_meta = rootMeta
			hello = "Hello!"
			data = {}

		self.theRootObj = RootObj()
		self.loader.static.add(self.theRootObj,2,99)

		sql = SQLLoader(DBSession,self.loader)
		sql.add_model(Person,self.theRootObj.data)
		sql.add_model(Address)
		self.loader.add_loader(sql)

	def do_root(self,msg):
		logger_s.debug("Get root %r",msg)
		return self.theRootObj
	do_root.include = True

	def do_update(self,msg):
		if msg == 1:
			self.send("trigger",msg)
		else:
			raise RuntimeError(msg)
	
class Broker(TestMain):
	c = q = s = None
	def setup(self):
		self.a = [None,AsyncResult(),AsyncResult()]
		self.s = TestBrokerServer()
		self.q = LocalQueue(self.s.recv)
		self.c = TestBrokerClient(self,self.q.send)
		self.q.set_client_worker(self.c._recv)
		self.s.sender = self.q.notify
		super(Broker,self).setup()
	def stop(self):
		if self.q is not None:
			self.q.shutdown()
		super(Broker,self).stop()

	def ref(self,p):
		k = p._key
		def res():
			return self.c.get(k)
		return res

	@property
	def cid(self):
		return self.q.cq.next_id

	def job1(self):
		logger.debug("Get the root")
		res = self.c.root
		logger.debug("recv %r",res)
		assert res.hello == "Hello!"
		print(res._meta)
		print(res._meta._meta)
		P = res.data['Person']
		assert P.name == 'Person',P.name
		r = P.find()
		assert len(r) == 0, r

		p1 = P.new(name="Fred Flintstone")
		p1r = self.ref(p1)
		self.c._send("update",1)
		self.a[2].get()
		p1 = p1r()
		assert p1.name == "Freddy Firestone", p1.name

	def job2(self):
		logger.debug("Get the root 2")
		res = self.c.root
		logger.debug("recv %r",res)
		P = res.data['Person']
		self.a[1].get()
		p1 = P.get(name="Fred Flintstone")
		p1.name="Freddy Firestone"
		self.c.commit()
		self.c._send("update",2)

	def main(self):
		j1 = spawn(self.job1)
		j2 = spawn(self.job2)
		j1.join()
		j2.join()

b = Broker()
b.register_stop(logger.debug,"shutting down")
b.run()

logger.debug("Exiting")
