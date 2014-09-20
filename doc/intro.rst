
Use case
########

Let's say that you have a database. Or several. Or something else that can
be expressed as a collection of typed objects. Something which is
read-only, or updated fairly infrequently. DaBroker does not care (much).
, as long
as these objects have well-defined data fields and/or links to other objects:
and types.

Let's further assume that you have processes which need to keep a dynamic
subset of that data in memory. For instance, if you serve a web site, the
home page qualifies. So does the page that was linked from Slashdot or
Reddit yesterday.

Also, you probably might want to …

    * have some sort of access control

    * use Pythonic syntax for accessing data

    * not care whether the next bit you need is cached at the moment

    * not work with stale data

    * not have an object change under your nose, i.e. while you're processing it

    * not lock everything preemptively

Last, assume that you can funnel all updates, assuming there are any, through DaBroker.

DaBroker
--------

DaBroker is a client/server system.

The server assigns an object reference key to everything it can send to the
client and supplies (at least) a method to find a "root object". It
supports multiple concurrent object stores (database tables, static data,
you-name-it).

The client generates proxy objects for the content it receives from the
server. Object references and method calls are handled transparently.

The DaBroker server sends "this is no longer valid" messages to all
clients when an object is updated. The client will then remove the
invalid objects from its cache, so that following an object reference will
update the cached copy automatically. It will not touch the actual objects.

Design
######

DaBroker is a message-based multi-process system. It uses a central broker
for serving information about your data structure and for object access.

DaBroker is a distributed system. You can run more than one broker.
You can use a variety of data back-ends.

DaBroker exposes common one-to-many and many-to-one semantics.

DaBroker does not itself have a generic query interface. You can, however,
easily add application-specific back-ends.

DaBroker does not constrain your data serialization scheme. It currently
supports JSON, BSON (i.e. binary JSON) and Python's `marshal` module.
The only mandatory requirement is support for strings and string-keyed
dictionaries / hashes. All current serializers do also support lists, 
integers, floats, and True/False/None; that can be made optional if
necessary.

DaBroker does not constrain your data types. Its serializer can reproduce
arbitrary data structures, including self-referential objects and loops.
It will only transmit Python objects it knows about.

System Layout
#############

Storage
-------

You probably want persistent object storage. For instance, a SQL or
NoSQL database.

You need to tell DaBroker about your objects' metadata:
data fields, foreign keys, and methods the client may call.

DaBroker supports extracting field and relationship metadata from
SQLAlchemy.

Message passing
---------------

You need a 0MQ server. DaBroker has been tested with RabbitMQ. Other
methods for passing messages are possible.

The DaBroker unit tests need a RabbitMQ server with a "test" virtual host:
run `tests/setup_rabbitmq.sh` before you run the tests the first time.

If you set the `AMQP_HOST` environment variable, the tests will use a
RabbitMQ server on that host.

Message passing overhead, on a reasonably current server, is on the order
of 1…2 milliseconds per RPC call. No work has yet been done to optimize
this.

Server
------

The DaBroker server listens to an RPC message queue. It exposes methods to

  * retrieve the "root" object

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

The DaBroker client asks the server for its root object. It then accesses
other objects by reading the appropriate attributes or calling methods.
This works exactly like accessing one-to-many or many-to-one references in
SQLAlchemy's ORM.

The client also, of course, listens for the server's invalidation requests
and other broadcast messages.

The client caches objects. It makes sure that each object with a given key
exists exactly once (except when that object is updated).

The client is thread-safe in the sense that you can run any number of
greenlets which do whatever you like to your data.

The client is not thread-safe in the sense that changed object attributes
will be immediately visible to other threads in your client, but not to
anything else in the system. Changes are batched and sent to the server
when you tell the DaBroker code to do so.

DaBroker verifies that the attributes of the objects you update have not
been modified. If that happens, the update is reverted.

When in doubt, use multiple processes.

Access control
##############

None, so far.

However, you can tell your DaBroker server to export a root object that only
has an "auth" method, which clients need to call with correct parameters in
order to get at the actual data.

Retrieving or modifying object data is secured by a hash value which the
server only sends to a client that receives that specific object. This
prevents clients from maliciously retrieving or changing random objects.

However, the server's broadcast messages contain details of obsolete, new,
and changed objects. Otherwise the client cache could not be cleared.
Set the "hidden" attribute on a field or reference if you want to block
a specific field – either for privacy, or when you know that you never
search for it anyway and want to save some bandwidth.

Source, Documentation, etc.
###########################

Source code, issue tracker, etc., is available at
https://github.com/smurf/dabroker .

The documentation is not yet online because somebody needs to verify that
the ReST renders correctly, convert the whole mess to Sphinx, document the
API, and whatnot.

License
#######

DaBroker is Copyright © 2014 by Matthias Urlichs <matthias@urlichs.de>
and whoever else submits patches (assuming that I accept them, which is
not unheard-of).

DaBroker is licensed under the GPLv3. See the file `LICENSE` for details.

While I would have liked to publish this code under the AGPL instead
(so that everybody shall _have_to_ share their extensions and other
interesting DaBroker-related code), life is not perfect, so I'll merely
state my wish that you in fact _do_ share your work. Whether you ultimately
do, or not, is up to you.

