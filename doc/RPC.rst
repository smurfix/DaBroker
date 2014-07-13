RPC
===

Calls from the client to the server are encoded as dicts with a number of
specific elements.

    *   _m

        The server method to call.

    *   _o

        The object to operate on, if any.

    *   _a

        Positional arguments, if any. (Try not to use them.)

On the server, if `_o` is present, `_m` contains the name of a method of
that object. Otherwise, the contents of `_m` are prefixed with "do_" and
name a method of the server object.

In any case, the remaining elements are used as named arguments.
The return value of the call is returned to the client.

On the server, if the method in question has an "include" attribute which
is `True`, the returned object, or list of objects, is packed directly.
Otherwise, only object references are transmitted.

Server methods
==============

    *   root

        Returns the root of the object hierarchy. If you need to do any
        client authorization, expose something which only has a "register"
        method.

        What "root of the object hierarchy" actually means is up to you.

    *   echo

        Returns its single argument unmodified.

    *   ping

        Same as `echo`, but no argument.

    *   pong

        No argument. The client is supposed to call this method when it
        received a `ping` broadcast from the server.

    *   get

        Basic object resolution. Single argument: the key of an object.
        The actual object will be returned.

    *   find

        Basic object search. Arguments: the meta object's `key`, and a dict
        `k` of key-value parameters. This method is supposed to return all
        objects with this meta and the matching key/value pairs.

        Values referring to other objects are encoded as their keys.

Client methods
==============

Clients listen to server broadcasts. There is no response.

    *   ping

        The client is supposed to respond by calling the server `pong`
        method.

    *   invalid

        The (positional) arguments are keys. The objects corresponding to
        these keys are invalidated on the client.

    *   invalid_key

        `_key` refers to an object and `_meta` to its class. Other
        arguments are interpreted as (old_value,new_value) tuples.
        
        This method invalidates the given object as well as any search results
        on the given `meta` with no key whose value does not match the arguments.

        Assume that the arguments to `invalid_key` are a=(1,2) b=(2,3).
        Results of a search for a=1 c=3 will be flushed, as well as those
        for x=1 y=2 or one that contains no parameters (and thus returns
        everything).

        As this message contains information about changed fields, it leaks
        information. TODO: Mitigate by marking fields as secret.

