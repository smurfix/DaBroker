Client code
===========

Startup
-------

Connecting to DaBroker is easy:

    from dabroker.client import BrokerClient
    broker = BrokerClient(cfg={…})
    root = broker.root

You now can access the data fields, objects and methods which your server
publishes, transparently. This includes metadata via the object's `_meta`
attribute.

All brokered objects are instances of (a subclass of)
dabroker.client.codec.ClientBaseObj. The server tells the client (by way of
a metaclass, accessible via the `_meta` attribute) which data fields are
normal Python data and which refer to other DaBroker objects.
Whenever you read one of these attributes, the DaBroker client will check
the local cache and return the current version of that object. Other
attributes are not special.

If you examine the client's root object with the debugger, it will look
something like this:

    (Pdb) pp self.root.__dict__
    {'_dab': <__main__.Test11_client object at 0x7ff1bfc43610>,
    '_key': R:(u'static', u'root')‹GRjiMxoQ›,
    '_meta': <ClientInfo:rootMeta>,
    '_refs': {u'more': R:(u'static', 1, 2, 3)‹AbDiG3xA›},
    u'data': {},
    u'hello': u'Hello!'}

This is the associated metadata object, `<ClientInfo:rootMeta>`:

    (Pdb) pp self.root._meta.__dict__
    {'_class': [None, None],
    '_dab': <__main__.Test11_client object at 0x7ff1bfc43610>,
    '_key': R:(u'static', u'root', u'meta')‹LNZOGl0S›,
    '_meta': ClientBrokeredInfoInfo(u'BrokeredInfoInfo Singleton'),
    '_refs': {},
    'backrefs': {},
    'calls': {u'callme': <dabroker.base.Callable object at 0x7ff1bf7c85d0>},
    'client': <__main__.Test11_client object at 0x7ff1bfc43610>,
    'fields': {u'data': <dabroker.base.Field object at 0x7ff1bf7c8350>,
                u'hello': <dabroker.base.Field object at 0x7ff1bf7c84d0>},
    'name': u'rootMeta',
    'refs': {u'_meta': <dabroker.base.Ref object at 0x7ff1bf7c8550>,
            u'more': <dabroker.base.Ref object at 0x7ff1bf7c8590>},
    'searches': <WeakValueDictionary at 140676276486800>}

Of special interest here is the `root.more` attribute, which refers to
another DaBroker object. An up-to-date version will be returned whenever
you access it, as only that object's key is stored here (in the `_refs`
attribute). By contrast, if you save a DaBroker object anywhere else
(local or global variable, another attribute), it will not be updated.

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

