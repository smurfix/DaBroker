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

        Default: null, which only works with the "local" transport.
        
        Available: bson json marshal

    *   transport

        The message transport to use.
        
        You can either use a single word which is translated to
        `dabroker.server.transport.NAME.Transport`, or a compound name
        which will be regarded as an object and instantiated directly.

        The default transport will only exchange messages within the same
        process and requires 

        Default: local.

        Available: amqp.

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

