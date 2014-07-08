Message data
============

DaBroker's serializer translates arbitrary Python data structures to
a hierarchy of non-self-referential JSON-compatible `dict` and `list`
objects, which can then serialized without problem using almost any
generic serialization protocol (JSON, BSON, marshal, …).

Python objects (excluding unstructured types, i.e. numbers and strings)
are translated to dictionaries with a couple of special elements.

    *   _o

        The object's type. Dictionaries don't get a type.

    *   _oi

        The object's identifier. This is a sequence number which is not
        unique across encodings.

    *   _or

        This is another reference to an object. The value is that object's
        `_oi` number.

    *   _o_*

        As the encoding needs to be 100% transparent, any Python dictionary
        keys which originally start with `_o` get an underscore inserted as
        the third character. This is of course reversed during deserialization.

An example:

    >>> from dabroker.base.codec import BaseCodec as C
    >>> c=C()
    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    TypeError: __init__() takes at least 2 arguments (1 given)
    >>> c=C(())
    >>> c=C(None)
    >>> from pprint import pprint as p
    >>> from datetime import date
    >>> now=date(2014,7,4)
    >>> p(c.encode({'one':e,'two':e,'now':now}))
    {'now': {'_o': 'date', 'd': 735418, 's': '2014-07-04'},
     'one': {'_or': 3},
     'two': {'_d': ['Hello', 'there'], '_o': 'LIST', '_oi': 3}}

The codec will not add `_oi` elements to dictionaries that don't need them.
It may not actually pack lists in dicts if that is not necessary.

    :note: Current implementation: References are tagged but not marked.
    If/when it hits a data structure that's not a strict tree, it switches
    to a "complicated" algorithm.

Strings are always transmitted as plain strings, even if they are long
enough to benefit from reference counting.

Errors
------

Error messages are propagated through RPC. Errors are sent as a dict with
an `_error` key (with a string representation of the problem as the value).
The decoder will raise the error when it encounters it.

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
        The actual object key is in the `k` element.

    *   Ref
        
        References to other objects. Their sole content is a `k` element
        with the object's key.

        Keys currently are transmitted as simple lists. TODO.

    *   Info

        DaBroker's meta object. It describes the data fields of the objects
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

        No special considerations.

