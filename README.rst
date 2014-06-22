DaBroker
========

DaBroker is a Data Access Broker, written in Python.
It exposes objects (e.g. database rows, or their NoSQL equivalent, or
indeed anything you might think of) to clients which might cache these
objects.

Rationale
#########

Assume that you have a database (or several) which is not read-only, but
not updated frequently either.

Further assume a distributed heap of processes which need to keep a dynamic
subset of that data in memory. For instance, if you serve a web site, the
home page qualifies. So does the page that was linked from Slashdot
yesterday.

Also assume that you don't want to work with stale data.

Last, assume that you will funnel all data updates through DaBroker.

Design
######

DaBroker is a message-based multi-process system. It uses a central broker
for serving information about your data structure and for object access.

DaBroker is a distributed system. You can run more than one broker.
You can use a variety of data back-ends.

DaBroker exposes common one/many-to-many/one semantics. Depending on the
client's language, accessing linked objects is no different from working
with local object references.

DaBroker does not expose a generic query interface. You can, however,
easily add application-specific back-ends.

DaBroker's data serialization language is BSON, i.e. binary JSON.
Other serializers are possible.

System Layout
#############

Storage
-------

You need at least one persistent storage system. For instance, a SQL or
NoSQL database. You have, or can generate, introspection data (which
tables exist, column names, default values, foreign keys, …).

Message passing
---------------

You need a 0MQ server. DaBroker has been tested with RabbitMQ.

Server
------

The DaBroker server listens to a RPC-style queue. It exposes methods to

  * retrieve "root" objects

  * retrieve data items

  * cache data descriptions (if available)

  * retrieve related objects

  * create, update and delete objects

The server also broadcasts information about new / updated / deleted
objects to a publish-only queue. Clients which cache objects listen to that
queue and invalidate their cached copies.

The DaBroker server does not keep persisent state. Clients which update
objects are expected to supply old field contents for verification that
they do not update stale values. Serial numbers on objects might be
supported in a future version.

Source, Documentation, etc.
###########################

TBD

