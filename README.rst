DaBroker
========

DaBroker is short for Data Access Broker.

DaBroker exposes objects (e.g. database rows, or their NoSQL equivalent, or
indeed anything you might think of) to clients which might cache these
objects.

DaBroker is written in Python. If you understand JSON and RabbitMQ, 
a client in a different language is reasonably straightforward.

Rationale
#########

Assume that you have a database (or several) which is not read-only, but
not updated frequently either.

Further assume processes which need to keep a dynamic subset of that data
in memory. For instance, if you serve a web site, the home page qualifies.
So does the page that was linked from Slashdot yesterday.

Also assume that you don't want to work with stale data.

Last, assume that you can funnel all updates through DaBroker.

With DaBroker, you can (mostly) forget about network latency or excessive
memory usage because "hot" data will be cached on the client, dynamically.

The DaBroker server sends "this is no longer valid" messages to all
clients. Following an object reference will then update the cached copy
automatically. Objects that are in active use will not be modified
"behind your back".

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

DaBroker does not constrain data types. Its serializer can reproduce
arbitrary data structures including self-referential objects and loops.
It will only transmit Python objects it knows about.

System Layout
#############

Storage
-------

You probably need at a persistent object storage. For instance, a SQL or
NoSQL database. You need to store or extract introspection data (which
tables exist, column names, default values, foreign keys, …).

Message passing
---------------

You need a 0MQ server. DaBroker has been tested with RabbitMQ. Other
methods for passing messages around are possible.

The DaBroker unit tests use a RabbitMQ vhost. You need to do this, once:

    rabbitmqctl add_vhost test
    rabbitmqctl add_user test test
    rabbitmqctl set_permissions -p test test ".*"  ".*"  ".*"

Message passing overhead, on a reasonably current server, is on the order
of 1 millisecond per RPC call.

Server
------

The DaBroker server listens to an RPC message queue. It exposes methods to

  * retrieve "root" objects

  * call methods on objects

  * retrieve related objects

  * create, update and delete objects

The server also broadcasts information about new / updated / deleted
objects to a separate queue.

The DaBroker server itself does not keep persistent state. Clients which
update objects are expected to supply old field contents for verification
that they do not update stale values.

This means that, assuming that your server code uses a shared-state
back-end (an SQL database, NoSQL storage), you can easily run more
than one server in parallel if one should be too slow.

Client
------

The DaBroker client asks the server for a root object. It then accesses
other objects by simply reading the appropriate attributes. This works much
like accessing one-to-many or many-to-one references in SQLAlchemy, except
that the server can export method calls, as well as objects which are not a
row in a table. The server operator selects what to export. By default, no
method calls and all attributes and relationships are exposed.

The client also, of course, listens for the server's invalidation requests
and other broadcast messages.

The client uses a caching system. It makes sure that each object with a
given key exists exactly once.

The client is thread-safe in the sense that you can run any number of
greenlets which do whatever you like to your data.

The client is not thread-safe in the sense that changed attributes will be
immediately visible to other threads in our client, but not to anything
else in the system. Changes are batched and sent to the server when you
tell the DaBroker code to do so.

DaBroker verifies that the attributes of the objects you update have not
been modified. In that case, the update is reverted.

When in doubt, use multiple processes.

Access control
##############

None.

However, you can tell the DaBroker server to only export a single root
object with an "auth" method, which clients need to call with correct
parameters in order to get at the actual data.

While the stream of broadcast messages does contain details of obsolete
objects, actual object references contain a hash value which is required
for direct access.

Source, Documentation, etc.
###########################

TBD

License
#######

DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>,
it is licensed under the GPLv3. See the file `LICENSE` for details.

Note that I would have liked to publish this code under the AGPL instead
(so that everybody will _have_to_ share their extensions and other
interesting pybble-related code), but life is not perfect, so I'll merely
state my wish here that you in fact _do_ share your work. Whether you
ultimately do, or not, is up to you.

