#!/usr/bin/python3
#
# A tiny little script that tests some basic kombu functions
# A learning exercise only, not a test bed
# Kombu documentation is, well a tad lacking in places
#
# The goal, create a queue, and put a few messages on it.
#
# Use another script to try find and examine that queue.

from kombu import Connection, Queue, Exchange, Producer, Consumer

URL = "pyamqp://CoGs:ManyTeeth@localhost/CoGs"

xname = "kombu.test.exchange"
qname = "kombu.test.queue"
rkey = "kombu.test.queue.routing.key"

with Connection(URL) as conn:
    # Connection is lazy. Force a connection now.
    conn.connect()
    c = conn.connection
    laddr = c.sock.getsockname()
    raddr = c.sock.getpeername()
    c.name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
    c.name_short = f"{laddr[0]}:{laddr[1]}"

    print(f'Connection: {c.name_short}')

    # Create a channel on the conection and log it in the RabbitMQ webmonitor format                     
    ch = c.channel()
    ch.name = f'{c.name} ({ch.channel_id})'
    ch.name_short = f'{c.name_short} ({ch.channel_id})'

    print(f'Channel: {ch.name_short}')
    
    x = Exchange(xname, channel=ch)
    x.declare() # Makes the exchange appears on the RabbitMQ web monitor
    
    q = Queue(qname, exchange=x, channel=ch, routing_key=rkey, durable=True)
    q.declare() # Makes the queue appears on the RabbitMQ web monitor
    
    print(f'Exchange: {x.name}')
    print(f'Queue: {q.name}')
    print(f'Routing Key: {rkey}')
    
    p = Producer(ch)
    
    p.publish("message 1", exchange=x, routing_key=rkey)
    p.publish("message 2", exchange=x, routing_key=rkey)
    p.publish("message 3", exchange=x, routing_key=rkey)
    p.publish("message 4", exchange=x, routing_key=rkey)
    p.publish("message 5", exchange=x, routing_key=rkey)
    

    
