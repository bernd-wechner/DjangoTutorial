from celery import current_app

from kombu import Connection, Queue, Exchange, Producer

from .config import logger
from builtins import getattr

def augment_queue(queue):
    '''
    
    :param queue:
    '''
    queue.producer = Producer(queue.channel, exchange=queue.exchange, routing_key=queue.routing_key)
    queue.publish = queue.producer.publish
    return queue

def augmented_queue(name, channel, exchange, routing_key):
    '''
    Returns a Kombu Queue, augmented with a producer and publish method.
    
    :param name:        The name of the Queue
    :param channel:     The channel it is bound to
    :param exchange:    The exchange is is bound to
    :param routing_key: The routing key that reaches the Queue on that Exchnage
    '''
    q = Queue(name, channel=channel, exchange=exchange, routing_key=routing_key, no_declare=True)
    return augment_queue(q)

def has_open_connection(queue):
    '''
    Returns true if the Queue provided has an open connection.
    
    :param queue: A Kombu Queue
    '''
    if ch := getattr(queue, 'channel', False):
        if c := getattr(ch, 'connection', False):
            if getattr(c, 'connected', False):
                return True
    
    return False
    

class ArgumentError(Exception):
    pass

class InteractiveConnection:
    '''
    A Context manager that provides a a task with all the queues it needs and an open 
    connection that is closed when done.
    '''
    
    # Attributes
    task = None         # An Interactive Task instance that is augmented with the Queues an Interactive Connection needs
    connection = None   # The Kombu Connection that the Queues open on
    channel = None      # The Kombu Channel that the Queues are bound to
    exchange = None     # The Kombu Exchanges the Queues are bound to
    
    # The Queues themselves are attached to task as:
    #
    # task.management_queue
    # task.instruction_queue
    # task.update_queue
   
    def __init__(self, task):
        self.task = task
        
    def __enter__(self):
        task = self.task
        
        # Get the queue names based on on the queue_name_root
        qnames = task.queue_names(task.queue_name_root)
        management_queue_name = qnames[task.management_key()]
        instruction_queue_name = qnames[task.instruction_key()]
        if task.update_via_broker:
            update_queue_name = qnames[task.update_key()]

        logger.debug(f'Connecting to: {current_app.conf.broker_url}')
        logger.debug(f'\tQueue Name Root: {task.queue_name_root}')

        self.connection = Connection(current_app.conf.broker_url)
         
        # Connection is lazy. Force a connection now.
        self.connection.connect()

        # TODO,: Check this, looks odd
        c = self.connection.connection
        logger.debug(f'\tConnection: {task.connection_names(c)[1]}')

        # Create a channel on the connection and log it in the RabbitMQ webmonitor format                     
        ch = c.channel()
        self.channel = ch
        logger.debug(f'\tChannel: {task.channel_names(ch)[1]}')

        x = Exchange(task.exchange_name, channel=ch)
        self.exchange = x
        logger.debug(f'\tExchange: {x.name}')

        # Attach to the management queue (for managing interactions)
        task.management_queue = augmented_queue(management_queue_name, ch, x, task.management_key())
        logger.debug(f'\tManagement Queue: {task.management_queue.name}')
        logger.debug(f'\t\tReached via: {task.management_queue.exchange.name} -> {task.management_queue.routing_key}')
        
        # Attach to the instruction queue (for reading instructions)
        task.instruction_queue = augmented_queue(instruction_queue_name, ch, x, task.instruction_key())
        logger.debug(f'\tInstruction Queue: {task.instruction_queue.name}')
        logger.debug(f'\t\tReached via: {task.instruction_queue.exchange.name} -> {task.instruction_queue.routing_key}')

        if task.update_via_broker:
            # Attach to the instruction queue (for reading instructions)
            task.update_queue = augmented_queue(update_queue_name, ch, x, task.update_key())
            logger.debug(f'\tUpdate Queue: {task.update_queue.name}')
            logger.debug(f'\t\tReached via: {task.update_queue.exchange.name} -> {task.update_queue.routing_key}')
                    
        return self
    
    def __exit__(self, type, value, traceback):  # @ReservedAssignment
        self.connection.release()

class ManagementQueue:
    '''
    A subset of InteractiveConnection which provides just the Management queue to work with.
    '''
    # Attributes
    task = None         # An Interactive Task instance that is augmented with the Queues an Interactive Connection needs
    connection = None   # The Kombu Connection that the Queues open on
    channel = None      # The Kombu Channel that the Queues are bound to
    exchange = None     # The Kombu Exchanges the Queues are bound to
    
    def __init__(self, task):
        self.task = task
    
    def __enter__(self):
        task = self.task
        
        # If the task already provides on, just use that
        if task.management_queue and has_open_connection(task.management_queue):
            return task.management_queue
        
        # Else connect to the management queue and return an augmented Queue 
        else:
            management_queue_name = task.name
    
            logger.debug(f'Connecting to: {current_app.conf.broker_url}')
    
            self.connection = Connection(current_app.conf.broker_url)
             
            # Connection is lazy. Force a connection now.
            self.connection.connect()
    
            # As odd as this looks the Kombu Connection object has a connection
            # attribute which is the AMQP connection and only it has the socket
            # information that task.connection_names() needs. A channel() can 
            # be
            c = self.connection
            logger.debug(f'\tConnection: {task.connection_names(c.connection)[1]}')
    
            # Create a channel on the connection and log it in the RabbitMQ webmonitor format                     
            ch = c.channel()
            self.channel = ch
            logger.debug(f'\tChannel: {task.channel_names(ch)[1]}')
    
            x = Exchange(task.exchange_name, channel=ch)
            self.exchange = x
            logger.debug(f'\tExchange: {x.name}')
    
            # Attach to the management queue (for managing interactions)
            task.management_queue = augmented_queue(management_queue_name, ch, x, task.management_key())
            logger.debug(f'\tManagement Queue: {task.management_queue.name}')
            logger.debug(f'\t\tReached via: {task.management_queue.exchange.name} -> {task.management_queue.routing_key}')
            
            return task.management_queue

    def __exit__(self, type, value, traceback):  # @ReservedAssignment
        if self.connection:
            self.connection.release()

class InteractiveExchange:
    '''
    A context manager that provides the interactive exchange for publishing 
    messages.
    '''
    def __init__(self, task):
        '''
        Accessing an Interactive Exchange need a task only for the generic info in it, 
        and so this can be an instantiaton of Interactive()
        
        :param task: A task that provides the exchange name and some logging formatters
        '''
        self.task = task
        
    def __enter__(self):
        # We have a task that we need only for the exchnage name and some logging formatters.
        task = self.task
        
        logger.debug(f'Connecting to: {current_app.conf.broker_url}')

        self.connection = Connection(current_app.conf.broker_url)
         
        # Connection is lazy. Force a connection now.
        self.connection.connect()

        # TODO,: Check this, looks odd
        c = self.connection.connection
        logger.debug(f'\tConnection: {task.connection_names(c)[1]}')

        # Create a channel on the connection and log it in the RabbitMQ webmonitor format                     
        ch = c.channel()
        self.channel = ch
        logger.debug(f'\tChannel: {task.channel_names(ch)[1]}')

        x = Exchange(task.exchange_name, channel=ch)
        self.exchange = x
        logger.debug(f'\tExchange: {x.name}')
                    
        return x
    
    def __exit__(self, type, value, traceback):  # @ReservedAssignment    
        self.connection.release()
