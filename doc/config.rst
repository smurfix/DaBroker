Configuration
#############

Server
------

Client
------


Common parameters
-----------------

    *   codec

        The coding method to use.

        You can either use a single word which is translated to
        `dabroker.base.codec.NAME.Codec`, or a compound name
        which will be regarded as an object and instantiated directly.

        Default: bson.
        
        Transports which accept list+dict can use "null".

    *   transport

        The message transport to use.
        
        You can either use a single word which is translated to
        `dabroker.server.transport.NAME.Transport`, or a compound name
        which will be regarded as an object and instantiated directly.

        Default: amqp.

    *   username

        Default: empty.

    *   password

        Default: empty.

    *   host

        Host where the message exchange lives.

        Default: localhost.

    *   virtual_host

        If the queue system supports multiple domains, list yours here.

        Default: AMQP: "/"

    *   rpc_queue

        AMQP: Name of the queue name on which the server listens for RPC requests.

    *   exchange

        AMQP: Name of the exchange where clients register for broadcasts.

