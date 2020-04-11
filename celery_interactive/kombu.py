import functools, uuid

from celery import current_app
from celery.exceptions import Ignore

from kombu import Connection, Queue, Exchange, Producer, Consumer

from amqp.exceptions import NotFound

from .base import InteractiveBase
from .config import debug, logger

class Interactive(InteractiveBase):
    '''
    Provides the wrapper around a task function that is needed to support
    Interactive communications. 
    
    Implements the 3 key Broker interactions:
    
        instruct(instruction)
            Used by Task/Client (the task in the View context)
            Sends an instruction to a running task instance.
            
        check_for_instruction()
            Used by Task/Worker (the task in the Worker context)
            Checks for an instruction, returning None if none found. 
            Non blocking. 

        wait_for_instruction()
            Used by Task/Worker (the task in the Worker context)
            Checks for an instruction, waiting until one arrives. 
            Blocking. 
    
    Based in large part on the tutorials here:
    
        https://medium.com/python-pandemonium/talking-to-rabbitmq-with-python-and-kombu-6cbee93b1298
        https://medium.com/python-pandemonium/building-robust-rabbitmq-consumers-with-python-and-kombu-part-1-ccd660d17271

    An effort to implement these using Kombu.SimpleQueue failed: 

        https://stackoverflow.com/questions/60599301/celery-kombu-simplequeue-get-never-returns-even-when-message-appears-in-queue
        
    and so instead a Kombu.Queue is used with the following communication stack:
    
        Connection using conf.broker_read_url or conf.broker_write_url as needed
        Channel (barely relevant detail just asks the connection to provide one)
        Exchange (always named 'celery.interactive' and has a routing key to Queue)
            the routing_key is just the id of the running task
        Queue (configurable name, but each running instance of a task has it's own unique Queue it can watch)
            the Queue object is available in self.instruction_queue
        
    *@DynamicAttrs* <- Stops PyDev editor from complaining about missing attrs
    '''
    
    def default_queue_name(self, task_id=None):
        '''
        Builds a default queue name that an Interactive task can use.
        
        If use_indexed_queue_names is True, we'll use indexed queue names
        and not this default. That name must be communicated to a client 
        with a "QUEUE" state update. If it is false we can use this queue
        name, and don't need to communicate it to a client as it can be
        constructed from the task's name and running task_id alone.
        
        To wit, a client can use this queue name until a QUEUE state 
        update arrives but it must accept that the queue of this name 
        may not exist. 
        '''
        # Can be called with no arguments from the celery worker side
        # but from the client side need to provide a task_id either as
        # and argument or plugged into self.request.id.
        if not task_id:
            task_id = self.request.id
            
        return f'{self.name}.{task_id}'
        
    def queue_exists(self, name):
        '''
        Return True if a Queue of that name exists on the Broker 
        and False if not. 
        
        :param name: The name of the Queue to test
        '''
        with Connection(current_app.conf.broker_url) as conn:
            try:
                # Not especially well documented, but"
                #
                # https://docs.celeryproject.org/projects/kombu/en/stable/reference/kombu.transport.virtual.html#channel
                # https://docs.celeryproject.org/projects/kombu/en/stable/_modules/amqp/channel.html
                #
                # the queue_declare method of channel can be used to test
                # if a queue exists using passive=True which will not make any 
                # changes to the server (broker). From the channel docs above:   
                #
                #     passive: boolean
                # 
                #     do not create queue
                # 
                #     If set, the server will not create the queue.  The
                #     client can use this to check whether a queue exists
                #     without modifying the server state.
                # 
                #     RULE:
                # 
                #         If set, and the queue does not already exist, the
                #         server MUST respond with a reply code 404 (not
                #         found) and raise a channel exception.                
                #
                # The exception raised is amqp.exceptions.NotFound
                conn.default_channel.queue_declare(name, passive=True)
                exists = True
            except NotFound:
                exists = False
            except Exception as E:
                exists = False
                logger.error(f"Unexpected exception from Channel.queue_declare(): {E}")
                    
        return exists    
    
    def indexed_queue_name(self, i):
        return f'{self.name}.{i}'
    
    def first_free_indexed_queue_name(self):
        '''
        Returns the first free indexed queue name available on the Broker.
        '''
        i = 1
        while self.queue_exists(self.indexed_queue_name(i)):
            i += 1
        
        return self.indexed_queue_name(i)
    
    def __call__(self, *args, **kwargs):
        '''
        This is the entry point for Taks.delay() or task.apply_async(). We can call Task.run() 
        to run the actual Task decorated function. But we wrap that here around the connection 
        required to support Interactive communications.
        '''
        my_id = self.request.id
        
        if debug:
            logger.debug(f'Interactive task: {self.name}-{my_id}')
        
        # First configure the exchange and queue names
        xname = "celery.interactive"
        if self.use_indexed_queue_names:
            qname = self.first_free_indexed_queue_name()
        else:
            qname = self.default_queue_name()
        
        # A unique string to flag this result should be ignored.
        # It should simply have no chance of overlapping with an
        # actual task result. So we throw in a uuid for good measure.
        # We do this so thatw e can catch the Ignore exception, to
        # cleanly destroy the Queue this task was using before the 
        # final exit. 
        IGNORE_RESULT = f"__ignore_this_result__{uuid.uuid1()}"

        if debug:
            logger.debug(f'Connecting task to: {current_app.conf.broker_read_url}')
            
        with Connection(current_app.conf.broker_read_url) as conn:
            try:
                # Connection is lazy. Force a connection now.
                conn.connect()
                c = conn.connection
                laddr = c.sock.getsockname()
                raddr = c.sock.getpeername()
                c.name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
                c.name_short = f"{laddr[0]}:{laddr[1]}"
                if debug:
                    logger.debug(f'\tConnection: {c.name_short}')

                # Create a channel on the conection and log it in the RabbitMQ webmonitor format                     
                ch = c.channel()
                ch.name = f'{c.name} ({ch.channel_id})'
                ch.name_short = f'{c.name_short} ({ch.channel_id})'
                if debug:
                    logger.debug(f'\tChannel: {ch.name_short}')
                
                x = Exchange(xname, channel=ch)
                q = Queue(qname, exchange=x, channel=ch, routing_key=my_id, durable=True)

                if debug:
                    logger.debug(f'\tExchange: {x.name}')
                    logger.debug(f'\tQueue: {q.name}')

                # Makes the exchange and queue appear on the RabbitMQ web monitor
                x.declare()
                q.declare()
                
                # We only need to give task the q property as the exchange,channel and 
                # connection are all known by q on the off chance the task_functon wants 
                # access to them.
                #
                # q.exchange                  holds the exchange
                # q.echange.name              holds the name of the exchange
                # q.channel                   holds the channel
                # q.channel.channel_id        holds the channel ID
                # q.channel.name              holds the channel name
                # q.channel.connection        holds the connection
                # q.channel.connection.name   holds the connection name 
                
                self.instruction_queue = q
                
                result = self.run(self, *args, **kwargs)

                # Leave the exchange alone (it's reusable for other Interactive tasks)
            except Ignore:
                result = IGNORE_RESULT
            except Exception as e:
                logger.error(f'ERROR: {e}')
                self.update_state(state="FAILURE", meta={'result': 'result to date', 'reason': str(e)})
                result = None

            # Delete the queue before the task completes
            self.instruction_queue.delete()
        
            if debug:
                logger.debug(f'Deleted Queue: {q.name}')
            
        if result == IGNORE_RESULT:
            raise Ignore() 
            
        return result        

    def instruct(self, instruction):
        '''
        Given an instance of celery.Task (self) and an instruction will,
        if the task has an id in its request.id attribute to identify 
        a running instance of the task, send it the provided instruction.
        
        Used by a client wanting to instruct a running task instance to 
        do something. It's entirely up to the task whether it's even 
        checking for let alone acting on such instructions.
        
        To do so we open the exchange 'celery.interactive' using the 
        connection:
        
            current_app.conf.broker_write_url
            
        and use the task id as the routing_lkey to identify reach the 
        tasks Queue.
        
        :param instruction: The instruction to send (a string is ideal but kombu has to be able to serialize it)
        '''
        task_id = self.request.id
        
        if debug:
            print(f'Sending Instruction: {instruction} -> {task_id}')
        
        if task_id:
            with Connection(current_app.conf.broker_write_url) as conn:
                with conn.channel() as ch:
                    # Connection is lazy. Force a connection now.
                    conn.connect()
                    c = conn.connection
                    laddr = c.sock.getsockname()
                    raddr = c.sock.getpeername()
                    c.name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
                    c.name_short = f"{laddr[0]}:{laddr[1]}"
                    if debug:
                        print(f'\tConnection: {c.name_short}')
        
                    ch.name = f'{c.name} ({ch.channel_id})'
                    ch.name_short = f'{c.name_short} ({ch.channel_id})'
                    if debug:
                        print(f'\tChannel: {ch.name_short}')
        
                    x = Exchange(name="celery.interactive", channel=ch)
                    if debug:
                        print(f'\tExchange: {x.name}')
                    
                    p = Producer(ch)
                    p.publish(instruction, exchange=x, routing_key=task_id)
                
                    if debug:
                        print(f'\tSent: {instruction} to exchange {x.name} with routing key {task_id}')
    
    def check_for_instruction(self):
        '''
        Performs a non-blocking read on self.instruction_queue 
        (i.e. checks for an instruction).
        
        Returns None if no instruction found.
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.instruction_queue to check for abort messages."

        if debug:
            logger.debug(f'CHECKING queue "{q.name}" for an instruction.')
            
        try:
            message = q.get()
        except Exception as E:
            # TODO: Diagnose why my Queue dies, if it does ... 
            message = None
            logger.error(f'Error checking {getattr(q, "name")}: {E}')

        if message:            
            instruction = message.payload       # get an instruction if available
            message.ack()                       # remove message from queue
            return instruction
        else:
            return None
    
    def wait_for_instruction(self, prompt=None, interim_result=None, continue_monitoring=None):
        '''
        Performs a blocking read on self.instruction_queue 
        (i.e. waits for an instruction).
        
        :param prompt: Optionally, a prompt that will be sent along with 
                       the state update to WAITING, so the View can if
                       it wants, prompt a user (by whatever means it can) 
                       to provide an instruction.
                       
        :param interim_result: Optionally an interim result that the View
                               can present to the user if desired. A task 
                               returns a result when it's complete, but 
                               if it enters into a wait for instructions
                               it may have an interim result that it wants 
                               feedback on (classically the case for a 
                               commit or rollback request).  

        :param continue_monitoring: If a string is provided will be 
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.q to check for abort messages."
        
        instruction = None
        
        def got_message(body, message):
            nonlocal instruction
            instruction = body  # Not really an @UnusedVariable
            message.ack()
        
        meta = {'prompt': prompt, 'interim_result': interim_result, 'continue_monitoring': continue_monitoring}
        self.update_state(state="WAITING", meta=meta)
        
        if debug:
            logger.debug(f'Updated status to WAITING with info: {meta}')
            logger.debug(f'WAITING for an instruction ... (listening to queue: {q.name})')
                
        ch = q.exchange.channel
        c = ch.connection
        with Consumer(ch, queues=q, callbacks=[got_message], accept=["text/plain"]):
            # drain_events blocks until a message arrives then got_messag() is called.  
            c.drain_events()
        
        if debug:
            logger.info(f'RECEIVED instruction: {instruction}')
        
        return instruction