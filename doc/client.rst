Client code
===========

Startup
-------

Connecting to DaBroker is easy:

    from dabroker.client import BrokerClient
    broker = BrokerClient(cfg={…})
    root = broker.root

You now can access the data fields, objects and methods which your server
publishes. This includes metadata via the root (or in fact any other)
object's `_meta` attribute.

All brokered objects are instances of (a subclass of)
dabroker.client.codec.ClientBaseObj. The server tells the client which data
fields are normal Python data and which refer to other DaBroker objects.

Whenever you read one of the latter attributes, the DaBroker client will
check the local cache and return the current version of the object which
that attribute refers to. Thus, when you write

    obj_b = obj_a.some_obj

the DaBroker client will take the key stored in `obj_a` for the `some_obj`
attribute, fetch that object from cache or the server, and save it in
`obj_b`. However, it will _not_ check in the cache (or on the server)
whether the server's version of `obj_a.some_obj` still refers to `obj_b`.

This bascially means that at any time, a single object represents a specific
consistent instance at the time it was accessed. If you want a consistent
snapshot of a whole list of objects, there are two ways to do this:

    * Write a server method that returns them in a list, e.g. via a database transaction.
      Don't forget to flag the method with `.include=True`.

    * Collect the objects in a list or another convenient data structure, and do

        objs = […]
        while True:
            updated = False
            for o in objs:
                n = o._key()
                if n is not o:
                    updated = True
                new_objs.append(n)
            objs = new_objs
            if not updated: break
        # the snapshot is in `objs`

Refreshing an object
--------------------

Easy:

    obj = obj._key()

or in two steps:

    saved = obj._key
    # later
    obj = saved()

This releases your reference to the object and then fetches the current
copy from cache, or from the server. You can find an example in `test22`,
at the "# refresh" comment in `job1()`.

You can test `obj._obsolete` to discover whether the server has sent an
invalidation notive for that object.

Client-side subclassing
-----------------------

If you want to add your own client-side attributes or methods for client-local
processing, the recommended way is to register it with `@baseclass_for`:

    from dabroker.client.codec import baseclass_for, ClientBaseObj

    @baseclass_for("static","root","meta")
    class my_root(ClientBaseObj):
        def test_me(self):
            logger.debug("Local call")

    from dabroker.client import BrokerClient
    broker = BrokerClient(cfg={…})
    broker.root.test_me()

Here, `static root meta` is the key of the class you want to subclass on
the client. You need to inherit from `ClientBaseObj`, otherwise passing
(references to) your objects back to the server will not work.

See `test11` for an example.

Calling the server
------------------

The easiest way is to call the server directly:

Server, subclassing `dabroker.server.BrokerServer`:

    def do_hello(self,msg):
        return "hello "+msg

Client:

    assert broker.call("you") == "hello you"

However, most often you'll want to use a method that already exists on the
server side; simply add a `Callable` entry to the server object's info class:

    rootMeta.add(Callable("callme"))

    class TestRoot(BaseObj):
        _meta = rootMeta
        def callme(self,msg):
            return "hello "+msg

The client then merely needs to do

    from dabroker.client import BrokerClient
    broker = BrokerClient(cfg={…})
    assert broker.root.callme("me") == "hello me"

The server can mark the `Callable` as cachable on the client side:

    rootMeta.add(Callable("callme", cached=True))

On the client side, the call to the server needs to be wrapped in a `with`
statement:

    with broker.env:
        […]
        broker.root.callme("me") # calls the server
        broker.root.callme("me") # doesn't

In fact it makes sense to wrap the client's whole thread with this.

See `tests/__init__.py`. Caching is tested in `test21`.

Calls on invalidated (i.e. out-of-date or deleted) objects are never cached.

Shutdown
--------

Call

    broker.disconnect()

Note that DaBroker is using threads internally. You need to cleanly take
down your as well as DaBroker's threads if your program terminates,
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
            self.broker = BrokerClient(cfg=self.cfg)
        def main(self):
            do_whatever_with(self.broker.root)
        def stop(self):
            # If you start additional tasks, this is a good place to tell
            # them to terminate.
            pass
        def cleanup(self):
            if self.broker is not None:
                self.broker.disconnect()

    main = MyMain(cfg={…})
    main.run()

