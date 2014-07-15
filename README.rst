DaBroker is short for Data Access Broker.

DaBroker exposes objects (e.g. database rows, or their NoSQL equivalent, or
indeed anything you might think of) to clients. Clients cache these objects
until they do not want them any more, or until they are invalidated by the
server.

Multiple servers can run in parallel, for redundancy and increased speed.

DaBroker is written in Python. If you know JSON and AMQP, writing a client
in a different language is reasonably straightforward. Other marshaling
formats or transports can be plugged in easily.

