Server code
===========

Startup
-------

Connecting to DaBroker is easy:

    from dabroker.server import BrokerServer
    broker = BrokerServer(cfg={…})
    broker.root = root
    broker.start(purge=False)

`purge=False` is the default. Set it to True if you cannot restart the
whole system after a crash/reconfiguration because of old messages in the
server's queue. This should not usually happen.

Of course, this begs the question how to construct the root (or
any other objects, for that matter).

Serving objects
---------------

`DaBroker` supports strongly-typed objects. It distinguishes attributes
which refer to single other DaBroker objects (`*-to-one` relationships) or
lists (`one-to-many` relationships), methods which the client can call to
execute code on the server, and "other" data (integers, strings, or
structured data for which you have registered a codec).

So, first you need to create a `info` object which describes your data.
Let's say your root object looks like this:

    from dabroker.base import BaseObj

    class MyRoot(BaseObj):
        version_string = "0.12.3"
        version_tuple = (0,12,3)

        def login(self, name, hash):
            # verify password here
            return make_real_root(name)
        status = Status()

with

    class Status(BaseObj):
        servers = ("joe","curly","moe")
        load = {'joe':0.1, 'curly':None, 'moe':2.5}
        health = "OK"

The client obtains a root object once; it's therefore a good idea to move
mutable data to another object. You'll see how to publish updates later.

    broker.export_class(Status, 'servers load health')
    root = MyRoot()
    broker.export_object(root, 'version_string version_tuple login', refs='status')
    broker.add_static(root,"root")
    # export_class does not override 

You can also pass in an iterable. Class inspection is only used 

You can of course do all of this manually:

    from dabroker.base import BrokeredInfo, Field,Ref,Callable

    rootInfo = BrokeredInfo("rootInfo")
    rootInfo.add(Field("version_string"))
    rootInfo.add(Field("version_tuple"))
    rootInfo.add(Ref("status"))
    rootInfo.add(Callable("login"))

    statusInfo = BrokeredInfo("statusInfo")
    statusInfo.add(Field(servers))
    statusInfo.add(Field(load))
    statusInfo.add(Field(health))

    MyRoot._meta = rootInfo
    Status._meta = statusInfo

    # Next, add unique keys:

    broker.add_static(rootInfo,"root","meta")
    broker.add_static(statusInfo,"status","meta")

    broker.root = root = MyRoot()

    broker.add_static(root,"root")
    broker.add_static(root.status,"status")

`add_static` is a convenience method which expands to

    broker.loader.static.add(root.status,"status")

because DaBroker supports multiple loaders.

Thus:

    >>> print root._key

shows

    R:('static','root')‹ABCDEFGH›

You can now do this on the client:

    >>> print broker.root.status.health
    OK
    >>> real_root = broker.root.login("jane","sing a song and whist1e")
    >>> # except that you really shouldn't transmit cleartext passwords;
    >>> # instead, one good way is to get a random challenge value from
    >>> # the server, and return sha1(challenge+password) instead

on the client as well as on the server.

Attributing functions
---------------------

It's tedious to remember to manually add each function of an object which
you want to export to your metaobject. For this reason, there's a couple of
convenience functions which allow you to decorate your regular methods:

    >>> from dabroker.util import exported
    >>> class ServedObj(…):
    >>>     @exported(include=True)
    >>>     def special_object(self, …):
    >>>         return SomeExportedObject()

    >>> broker.add_callables


Searching
---------

DaBroker's built-in search handles equality-based `get` (returns one record
or raises an error) and `find` requests. In the meta object which shall
support searching, you need to set the `_dab_cached` attribute to some
non-`None` value, and add a method

    @exported
    def _dab_search(self, _limit=None,**kw):
        # a sample which finds nothing
        if False:
            yield None

which returns the objects in question, up to the given limit.

There also should be a `_dab_count` function (without the `_limit`
parameter) which simply returns the number of items that an unlimited
search with the same parameters would return.

Other functions can easily be implemented. Look at the implementation of
`dabroker.client.service.find`

Databases
---------

The method above is OK for new objects, but it's a bit tedious if you
already have a data description, e.g. if you want to use DaBroker to serve
data from a SQL database.

For this reason, it is reasonably easy to add database tables to DaBroker.
SQLAlchemy and its ORM is supported directly:

    from dabroker.server.loader.sqlalchemy import SQLLoader
    # `person` and `address` are the standard SQLAlchemy example tables

    sql = SQLLoader(DBSession,broker)

    sql.add_model(Person, root.data, rw=True)
    sql.add_model(Address)

This creates and registers a loader, and builds info objects for your models.
The "Person" entry is added to root.data (or any other dictionary;
presumably so that the client may directly access the model).

The `_dab_cached` attribute is supported.

The `rw` parameter can hold three values. The default is `False` (read-only),
which means that the client can call `.get()` and `.find()` methods on the
class object to retrieve records (both are translated to calling
`_dab_search` on the server).
`True` adds `.new()` and `.delete()` (which is usually done by syncing the client).

If `rw` is `None`, neither of these methods is available; the client can
only read attributes, and call methods which you explicitly export.

By default, all attributes known to SQLAlchemy are exported.
Add a `hide` parameter with a set of field names to exclude if you want to
block access to some fields.

Updating an object
------------------

Notify your clients.

    broker.obj_update(status, health="poor")

Client objects are _not_ updated in-place. Instead, they are invalidated so
that accessing them via some reference will retrieve them from the server.

The client will immediately see this change:

    >>> print root.status.health
    poor

If you replace objects and the references pointing to them,
you need to invalidate the reference's container:

    class Status(BaseObj):
        seq = 0
        [...]
    old_status = root.status
    old_seq = Status.seq
    Status.seq += 1

    new_status = broker.obj_new(Status, health="poor")
    broker.add_static(new_status, "status",Status.seq)
    broker.obj_update(root, status=new_status)
    broker.obj_delete(old_status)
    broker.del_static(new_status, "status",old_seq)

The client would then need to refresh its copy of the root object to see
the new status:

    >>> root = root._key()
    >>> print root.status.health
    poor

Database transactions
---------------------

Summary:
    
    from dabroker.util.sqlalchemy import session_wrapper,with_session

    def foo(x,y,z):
        with session_wrapper(x) as session:
            [whatever]

or, equivalently,

    @with_session
    def bar(session, x,y,z):
        # This is called as `bar(x,y,z)`
        [whatever]

You can safely nest these calls; the session is stored as a thread-local
object and the wrapper will use savepoints if nested. The `obj_*` methods
use these wrappers internally.

The first parameter must be a model created by `sql.add_model()`, or an
object of that model, so that the wrapper can find the correct database
engine to use.

Note: If you have to use a database which does not understand savepoints,
you need to let errors propagate through the outermost wrapper or `with`
scope, otherwise you'll get inconsistencies. DaBroker knows that sqlite
does not (and in fact raises an error if you try), and will issue a warning
(you can set dabroker.util.sqlalchemy._sqlite_warned to True to suppress
it).

Calling the server
------------------

Besides using methods on server objects which have been published, you
can call server methods directly:

Server, subclassing `dabroker.server.BrokerServer`:

    def do_hello(self,msg):
        return "hello "+msg

Client:

    assert broker.call("hello","you") == "hello you"

If you mark a server method with an "include" attribute, as in

    def do_special(self,msg):
        return SpecialObject(msg)
    do_special.incldue = True

the server will send top-level objects (i.e. the return value itself, or
the objects in a returned list) directly. All other objects are proxied by
a BaseRef object (in essence, their key) and need to be retrieved by the
client if/when it needs them. (Currently, the client does not hint to
the server which objects it has deleted from its cache.)

Shutdown
--------

Call

    broker.stop()

Note that DaBroker is using threads internally. You need to cleanly take
down all threads when your program terminates,
otherwise Python's threading system may stall. Also, you may or may not be
able to simply call sys.exit() from a thread if you see a fatal error.
This also applies to termination by signal (SIGINT, Control-C).

`dabroker.util.thread.Main` is a helper class which will clean up when your
main program gets a signal, or simply ends.

    class MyMain(Main):
        broker = None
        def __init__(self,cfg):
            self.cfg = cfg
            super(MyMain,self).__init__()
        def setup(self):
            self.broker = BrokerServer(cfg=self.cfg)
            self.broker.root = MyRoot()
            self.broker.start()
        def main(self):
            # Your main code doesn't actually need to do anything
            self.shutting_down.wait()
        def stop(self):
            # If you started additional tasks, this is a good place to tell
            # them to terminate.
        def cleanup(self):
            if self.broker is not None:
                self.broker.stop()

    main = MyMain(cfg={…})
    main.run()

Multiple servers
----------------

For load balancing or reliability, you might want to run more than one
server at a time. DaBroker supports this mode. However, there are a few
caveats.

The AMQP transport broadcasts server messages (chiefly, object invaliation
notices) to all clients. Other servers do not listen to this queue.
Therefore, the easiest solution is for the server to not have any mutable
internal state whatsoever; instead, you delegate that to the database.
If you can't do that, adding a server-to-server channel to the AMQP
transport is easy (TODO, in fact).

Server restart
--------------

Some transports allow you to restart the server, without the client even
being aware of that. For this to work, it's imperative that object keys
do not change between server invocations. DaBroker's static loader
intentionally does not provide a way to assign a new key by sequence
number or randomly; that's your application's job.

