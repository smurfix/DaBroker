Server code
===========

Startup
-------

Connecting to DaBroker is easy:

    from dabroker.server import BrokerServer
    broker = BrokerServer(cfg={…})
    broker.root = root
    broker.start()

Of course, this begs the question how to construct the root object (or
others).

Serving objects
---------------

`DaBroker` supports strongly-typed objects. It distinguishes attributes
which refer to other DaBroker single objects (`*-to-one` relationships) or
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

Construct the info objects:

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

Next, add unique keys:

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

You can now do this

    >>> print broker.root.status.health
    OK
    >>> real_root = broker.root.login("jane","sing a song and whist1e")
    >>> # except that you really shouldn't transmit cleartext passwords;
    >>> # instead, one good way is to get a random challenge value from
    >>> # the server, and return sha1(challenge+password) instead

on the client as well as on the server.

Databases
---------

The method above is OK for new objects, but it's a bit tedious if you
already have a data description, e.g. if you want to use DaBroker to serve
data from a SQL database.

For this reason, it is reasonably easy to add database tables to DaBroker.
SQLAlchemy and its ORM is supported directly:

    from dabroker.server.loader.sqlalchemy import SQLLoader
    # `person` and `address` are the standard SQLAlchemy example tables

    sql = SQLLoader(DBSession,self)
    sql.add_model(Person,root.data)
    sql.add_model(Address)
    self.loader.add_loader(sql)

Updating an object
------------------

    broker.obj_update(status, health="poor")

The client will immediately see this change:

    >>> print root.status.health
    poor

Alternately, you can send a new object:

    old_status = root.status
    new_status = broker.obj_new(Status, health="poor")
    broker.add_static(new_status, "status","new")
    broker.obj_update(root, status=new_status)
    broker.obj_delete(old_status)

The client would then need to refresh the root object to see the new
status:

    >>> root = root._key()
    >>> print root.status.health
    poor

Calling the server
------------------

Besides using methods on server objects which have been published, you
can call server methods directly:

Server, subclassing `dabroker.server.BrokerServer`:

    def do_hello(self,msg):
        return "hello "+msg

Client:

    assert broker.call("you") == "hello you"

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

`dabroker.util.thread.Main` is a helper class which will try to do the
right thing in these situations.

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

