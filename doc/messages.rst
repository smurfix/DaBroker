Message data
============

DaBroker's serializer translates arbitrary Python data structures to
a hierarchy of non-self-referential JSON-compatible `dict` and `list`
objects, which can then serialized without problem using almost any
generic serialization protocol (JSON, BSON, marshal, …).

Python objects (excluding unstructured types, i.e. numbers and strings)
are translated to dictionaries with a couple of special elements.

    *   _o

        The object's type. Plain dictionaries don't get a type element.

    *   _oi

        The object's identifier. This is a sequence number. It is not
        unique across encodings.

    *   _or

        This is another reference to an object. The 'real' value is the
        object whose `_oi` element is that number.

    *   _o_*

        As the encoding needs to be 100% transparent, any Python dictionary
        keys which originally start with `_o` get an underscore inserted as
        the third character. This is of course reversed during deserialization.

The result is then wrapped as the `data` element of a dict. A `cache`
element (optional) contains a list of completely-codeable objects which
occur more than once. Incomplete objects (i.e. recursive data structures)
are marked inline and will be patched when decoding.

An example:

    >>> from dabroker.base.codec import BaseCodec as C
    >>> c=C(None)
    >>> from pprint import pprint as p
    >>> from datetime import date
    >>> now=date(2014,7,4)
    >>> e={"Hi":"there"}
    >>> m=c.encode({'one':e,'two':e,'now':now}))
    >>> p(m)
    {u'cache': [{'Hi': 'there', u'_oi': 4}],
     u'data': {'now': {u'_o': u'date', u'd': 735418, u's': '2014-07-04'},
               'one': {u'_or': 4},
               'two': {u'_or': 4}},
     u'msgid': 42}
    >>> mm=c.decode(m)
    >>> p(mm)
    {u'cache': [{'Hi': 'there', u'_oi': 4}],
     u'data': {'now': {u'_o': u'date', u'd': 735418, u's': '2014-07-04'},
               'one': {u'_or': 4},
               'two': {u'_or': 4}},
     u'msgid': 42}
    >>> mmm=c.decode2(mm)
    >>> p(mmm)
    {'now': datetime.date(2014, 7, 4),
    'one': {'Hi': 'there'},
    'two': {'Hi': 'there'}}
    >>> mmm['one'] is mmm['two']
    True
    >>> mmm['one'] is e
    False
    >>> mmm.msgid
    42
    >>> 

The decoding step is a two-phase process because additional auxiliary
attributes may be present in the message which may influence decoding;
for instance, the DaBroker server sends the last broadcast's msgid in
RPC replies so that processing the answer can be delayed until all
intermediate broadcasts have been processed.

These elements are _not_ subject to the encoding that's performed on the
`data` or `error` members.

Strings are always transmitted as plain text, even if they are
repeated _and_ long enough to benefit from reference counting.

Errors
------

Error messages are propagated through RPC. Errors are sent as an object in
the message's `error` attribute (with a string representation of the
problem as the value). The (first step of the) decoder will raise the error
when it encounters it.

An optional `tb` element contains the traceback. It will be printed
properly when the resulting error is logged on the client.

TODO: No attempt is yet made to distinguish error severities
(e.g. retryable or not).

Object types
------------

These are the values of the `_o` key. Unrecognized object types will raise
an error.

    *   LIST

        Python lists are encoded with the actual list in a `_d` element.
        They also get an `_oi` so that referencing them via `_or` works.

    *   date

        The value is the current Julian date, stored in `d`. `s` contains a
        human-readable form which is ignored when importing.

    *   datetime

        The value is a Unix timestamp in `t` (int or float). A
        human-readable string with unspecified timezone (probably UTC) is
        stored in `s`.

    *   timedelta

        A fancy name for "number of seconds" (Python's `datetime.timedelta`
        type). The number of seconds is in `t`, the human-readable string
        is in `s`.

    *   time

        A time-of-day. The difference to `timedelta` is that `time` is
        translated to UTC timezone when exporting. The imported result will
        be an offset from UTC.

    *   Obj

        DaBroker's basic object type. `f` is a dict containing the
        non-reference fields of this object. Reference fields (i.e. those
        pointing to other DaBroker objects) are encoded by adding their
        keys to the `r` dict. All objects have at least one reference
        field named `_meta` which points to that object's type.
        The actual object key is a `Ref` object, stored in the `k` element
        and accessible as the object's `_key` attribute.

    *   Ref
        
        References to other objects. The actual key is a tuple, stored in 
        the `k` element. The `c` element contains a crypto key which is
        required to retrieve that object; it is not included in the
        server's broadcasts which invalidate data.

    *   Info

        A DaBroker meta object. It describes the data fields of the objects
        whose `_meta` field refers to this `Info` object.

        The minimum data fields of `Info` objects are
        
        *   name

            The class name. Informative.

        *   fields

            A dict of field names which contain non-DaBroker objects, 
            transmitted when you load the object itself.

        *   refs

            A dict of field names which refer to other DaBroker objects,
            auto-loaded as soon as you access them.

        *   backrefs

            Other objects which refer to this one. Typically auto-generated
            from the data description.

        *   calls

            Methods. Transparently calls the corresponding method on the
            server.

        `Info` objects are themselves DaBroker objects and have their own
        `_meta` pointer. This chain ultimately ends at a singleton whose
        key consists of an empty list and which is its own meta object.
        This singleton is hard-coded in the client.

    *   _F, _R, _B, _C

        Field types used in DaBroker's meta object, corresponding to
        `fields`, `refs`, `backrefs` and `calls` lists. See `Info`, above.

        These currently do not contain any special information.

Specific serializations
-----------------------

    *   BSON

        As BSON requires a top-level dictonary, every message is wrapped in one.
        The sole element is `_m`.

    *   JSON

        No special considerations. Slower than BSON. Text-only, therefore
        good for debugging and for implementations in other languages.

    *   marshal

        No special considerations. Not portable. About as fast as BSON.

RPC
---

DaBroker interprets these fields when receiving a message:

    *   _m  

        The name of the method to be called.

    *   _o

        The object to send the message to. If missing, the server object's
        method 'do_MSGNAME' is called.

    *   _a

        An array of un-named arguments.

    *   _mt

        A flag. This corresponds to the 'meta' flag of the `Callable`
        element defining a method call and specifies that a class method is
        called on the DaBroker object. If False or missing, the method will
        be called on the class.

All other fields are interpreted as named arguments.

In order to prevent the client from calling arbitrary object methods, the
method 'MSGNAME' needs to have a '_dab_callable' attribute whose value is true.

Replies are transmitted directly. The transport is responsible for
associating replies with the originating call.

Specific transports
-------------------

    *   AMQP

        Messages are sent to the queue 'dab_queue',
        unless otherwise specified in the 'rpc_queue' configuration item.
        Server replies use the AMQP correlation id to associate replies
        with requests.

        Server alerts are sent to the exchange 'dab_alert',
        unless otherwise specified in the 'rpc_queue' configuration item,
        with a routing key of 'dab_info'.
