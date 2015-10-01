Hacking
=======

Data types
----------

This section refers to data types like `Decimal` or `datetime.date`, i.e.
types which do not have DaBroker-specific attributes and are recreated 1:1
on the client. For client-local attributes and methods on DaBroker objects,
see `client.rst`.

Dabroker includes codecs for several common objects like `datetime`.

If you need to pass other classes through DaBroker's RPC, you need to
register a simple mapper class which can encode and decode your type. As an
example, see `dabroker.base.codec._timedelta`:

    import datetime
    from dabroker.base.codec import codec_adapter

    @codec_adapter
    class _timedelta(object):
        cls = datetime.timedelta
        clsname = "timedelta"

        @staticmethod
        def encode(obj, include=False):
            ## the string is purely for human consumption
            return {"t":obj.total_seconds(),"s":str(obj)}

        @staticmethod
        def decode(t,s=None,**_):
            return dt.timedelta(0,t)

`cls` is the class your codec works on, `clsname` the name it'll have in
the encoded object. Please choose something unique like
"your_project_name.typename". The codec's class itself
(`_timedelta` in this example) is never instantiated.

If your `encode` method returns elements which themselves need to be
encoded, that's not a problem: dabroker's codec will handle this case for
you. Likewise, it will pass already-assembled objects to your `decode`
method.

Your `decode` method should mention all known optional data fields and
ignore anything it does not understand, to guard against future extensions
(or additional attributes introduced by DaBroker).

The `include` parameter is used by DaBroker's caching system to determine
whether to send complete object data or just the key and metadata objects.
As a special case, if its value is `None` the encoded object is intended
for broadcasting: you should skip private fields (like user's addresses or
their encrypted passwords; I hope I don't have to mention that you should
not even store un-encrypted passwords, much less transmit them).

Codecs
------

Straightforward, by overriding `dabroker.base.code.BaseCodec`.
See `dabroker.base.codec.bson` for an example.

Transports
----------

Most likely, you'll split up the transport implementation into three parts:
the common part, server-specific and client-specific methods.

The code for AMQP lives in dabroker.{base,server,client}.transport.amqp,
respectively. Interfacing with DaBroker is via the .callbacks attribute,
which is a subclass of dabroker.base.transport.BaseCallbacks.

Note that the server part of your transport is responsible for returning
the data it gets from calling .callbacks.recv() back to the client. You do
not need to try to preserve message ordering, but it's imperative that you
operate asynchronously.

Warning: If you want your transport to support multiple independent
servers, you need to make sure that a server's broadcast message reaches
all clients, no matter which server they're currently talking to.

There is also a local transport, surprisingly named "local", which you can
use if the server thread lives in the same process as your client. It is
mainly useful for testing. Note that the server part adds a special
`_LocalQueue` element to the configuration dictionary which the client
needs to read. You therefore need to setup the server first, but that's a
reasonable thing to do in any case.

