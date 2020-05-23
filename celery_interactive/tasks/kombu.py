from celery import current_app
from celery.result import AsyncResult

from kombu import Connection, Queue, Exchange, Producer, Consumer

from amqp.exceptions import NotFound

from .. import log
from ..contexts.kombu import ManagementQueue, InteractiveExchange, augment_queue
from ..decorators.celery import ConnectedCall

from .base import InteractiveBase

class InteractiveKombu(InteractiveBase):
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
    
        Connection using conf.broker_url
        Channel (barely relevant detail just asks the connection to provide one)
        Exchange (always named 'celery.interactive' and has a routing key to Queue)
            the routing_key is just the id of the running task
        Queue (configurable name, but each running instance of a task has it's own unique Queue it can watch)
            the Queue object is available in self.instruction_queue
        
    *@DynamicAttrs* <- Stops PyDev editor from complaining about missing attrs
    '''
    # Use indexed queue names? If True adds a small overhead in finding a free 
    # indexed queue name on the broker when the task runs. This can make things
    # a little easier to minitor if needed than using that UUID task_id in the
    # queue name. But the UUID is available without consultingt he broker so 
    # marginally more efficient to use.
    #
    # Can of course be configured in any function decorated with  @Interactive.Config
    use_indexed_queue_names = True
    
    # The queues that will be used herein for interactions
    #
    # management_queue is always a kombu.Queue object with a get() method.
    # instruction_queue is a kombu.Queue in the Task/Worker context and kombu.Producer in the Task/Client context
    # update_queue is a kombu.Queue in the Task/Client context and kombu.Producer in the Task/Worker context
    #
    # update_queue is used only if updates_via_broker is True
    #
    # Basically A knmbu.Queue is for reading messages, and a kombu.Producer for writing messages.
    #
    # These are assigned values in:
    #
    # self.__call__ - in the Task/Worker context
    # ConnectedView - in the Task/Client context
    #
    # self.create_queues - is responsible for creating these queues
    #
    # It is called before self.apply_async publishes the task using the before_task_publish signal.
    
    queue_name_root = None      # The root for instruction and update queue names
    management_queue = None     # Named after the task name
    instruction_queue = None    # Named using queue_name_root
    update_queue = None         # Named using queue_name_root
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # See https://celery.readthedocs.io/en/latest/userguide/tasks.html#list-of-options
        #
        # When decorating a task function with:
        #
        # @app.task(bind=True, base=Interactive, conf=my_conf_function)
        #
        # Then self.conf is set to my_conf_function
        #
        # This is buried in a small note in the documentation:
        #
        # "Any keyword argument passed to the task decorator will actually be set as an 
        # attribute of the resulting task class"
        #
        # The configuration function can modify the task instance freely
        # notably defining the templates in task.django.templates but can 
        # also override methods and more customising the Interactive Task
        # as it sees fit.

        task_configurator = getattr(self, "conf", None)
        if callable(task_configurator):
            task_configurator(self, *args, **kwargs)
                    
        self.monitor_title = getattr(self, "monitor_title", getattr(self, "initial_monitor_title", None))

    def default_queue_name_root(self, task_id=None):
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
        
    def indexed_queue_name_root(self, i):
        return f'{self.name}.{i}'
    
    def queue_exists(self, name):
        '''
        Return True if a Queue of that name exists on the Broker 
        and False if not. 
        
        :param name: The name of the Queue to test
        '''
        with Connection(current_app.conf.broker_url) as conn:
            try:
                # Not especially well documented, but:
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
                log.error(f"Unexpected exception from Channel.queue_declare(): {E}")
                    
        return exists    
    
    def management_key(self, task_id=None):
        '''
        The routing key for the Management Queue.
        '''
        return f"{task_id if task_id else self.request.id}"

    def instruction_key(self, task_id=None):
        '''
        The routing key for the Instruction Queue.
        '''
        return f"{task_id if task_id else self.request.id}<"

    def update_key(self, task_id=None):
        '''
        The routing key for the Updates Queue.
        '''
        return f"{task_id if task_id else self.request.id}>"

    def queue_names(self, queue_name_root, except_management=False):
        '''
        Given a queue name root, returns a dict of all the queue names we
        want to create keyed on the routing key to use. One for instructions 
        sent to the worker, another (possibly) for updates sent from the worker.
        
        Two or three queues are used:
        
            1) A management queue, always named after the task
            2) An instruction queue for sending instructions to the task
            3) Optionally an update queue for sending updates back to the client
        
        :param queue_name_root:
        '''

        qnames = {} if except_management else {self.management_key():  self.name}
        qnames[self.instruction_key()] = f"{queue_name_root}<"
        if self.update_via_broker:
                qnames[self.update_key()] = f"{queue_name_root}>"
                
        return qnames

    def queue_name_root_available(self, queue_name_root):
        '''
        Returns True if the queue_name_root is available, that is, the queues 
        we want to create can be created (don't already exist)
        :param queue_name_root:
        '''
        for k, qname in self.queue_names(queue_name_root).items():
            # The management queue us expected to exist but the other
            # queue slots must not exist for the queue_name_root to be
            # available.
            if not k == self.management_key() and self.queue_exists(qname):
                return False
            
        return True

    def first_free_indexed_queue_name_root(self):
        '''
        Returns the first free indexed queue name available on the Broker.
        '''
        i = 1
        while not self.queue_name_root_available(self.indexed_queue_name_root(i)):
            i += 1
        
        return self.indexed_queue_name_root(i)
    
    def create_queues(self, initial_status=None):
        '''
        Creates (or ensure the existence of) the queues that an Interactive 
        task needs. 
        
        At least:
        
            1) One queue named after the task used for managing Interactions
                - This queue is used to store single message all the time,
                  that is always consumed and requeued which contains the map 
                  of IDs to queue name roots that serves both to communicate 
                  the queue name root and serve as a locking mechanism if only
                  one running instance is permitted (one_at_a_time is in force).
                  
            2) One queue for instructions (client->task)
            
            3) Optionally one queue for updates (task->client)
                - We can use the standard Celery update_status mechanism and/or
                  our own queue depending on the configurations:
                      update_via_backend
                      update_via_broker
        
        This (create_queues) should be called in the Task/Client before the task is 
        published (i.e. a request sent to the worker to run the task). That way:
        
            1) the queues exist by the time the task is running.
            2) the queue name root is known to the client.
            3) the existence of the request is known to both sides in case one_at_a_time is in force

        There is an odd asymmetry in Kombu.
        
        We can send messages using only the known exchange an the task_id as a routing 
        key. Put to read a queue, we need to know its name. The management queue has a 
        predictable name (that of the task), the instruction and update queues less so.
        We can name them using the task_id, or sequentially (if use_indexed_queue_names
        is True, which is cleaner if monitoring the broker as the tas_id which is UUID 
        is a long messy thing to have in a queue name).
        
        The task will receive the queue name root as a kwarg "queue_name_root" but only 
        if use_indexed_queue_names is True (it doesn't need a queue_name_root if not, as 
        it can use the default_queue_name_root(). Either way the task can check the 
        management queue to confirm its assigned queue_name_root.
        
        Returns: the queue name root used.
        '''
        log.debug(f'Creating Queues for: {self.fullname}')
        log.debug(f'\tBroker: {current_app.conf.broker_url}')
        
        with Connection(current_app.conf.broker_url) as conn:
            try:
                # Connection is lazy. Force a connection now.
                conn.connect()
                c = conn.connection
                log.debug(f'\tConnection: {self.connection_names(c)[1]}')

                # Create a channel on the connection and log it in the RabbitMQ webmonitor format
                ch = c.channel()
                log.debug(f'\tChannel: {self.channel_names(ch)[1]}')
                
                # Create or get the exchange
                x = Exchange(self.exchange_name, channel=ch, durable=True)
                x.declare() # Makes the exchange appears on the RabbitMQ web monitor
                log.debug(f'\tExchange: {x.name}')

                # Ensure the management queue exists before we do anything else     
                # kill zombies() expects to find it in place.            
                q = Queue(self.name, exchange=x, channel=ch, routing_key=self.management_key(), durable=True)
                q.declare() # Makes the queue appears on the RabbitMQ web monitor
                log.debug(f'\tManagement Queue: {q.name}')
                self.management_queue = augment_queue(q)
                
                # Kill any zombie queues first
                self.kill_zombies()
                
                # First configure the queue names
                if self.use_indexed_queue_names:
                    qname_root = self.first_free_indexed_queue_name_root()
                else:
                    qname_root = self.default_queue_name_root()
                    
                self.queue_name_root = qname_root
                    
                self.set_management_data(qname_root)
                
                qnames = self.queue_names(qname_root)

                # Create the other queues
                for k, qname in qnames.items():
                    # Already created the management queue
                    if k != self.management_key():
                        q = Queue(qname, exchange=x, channel=ch, routing_key=k, durable=True)
                        q.declare() # Makes the queue appears on the RabbitMQ web monitor

                        if k == self.instruction_key() and initial_status:
                            log.debug(f'\tInstruction Queue: {q.name}')
                            self.instruction_queue = augment_queue(q)
    
                        elif k == self.update_key():
                            log.debug(f'\tUpdate Queue: {q.name}')
                            self.update_queue = augment_queue(q)
                            
                            if initial_status:
                                updt_msg = q.get()
                                if updt_msg and isinstance(updt_msg.payload, tuple):
                                    # If a valid tuple was on the queue, requeue it
                                    # Pretty unlikely seeings we just created the queue! But still...
                                    updt_msg.requeue()
                                else:
                                    # If an invalid message (not a tuple) was on the queue
                                    # just ack() it (to get rid of it)
                                    if updt_msg:
                                        updt_msg.ack()
        
                                    assert initial_status, "create_queues: When and update_key is defined an initial_message must be defined when creating the queue."
        
                                    # We put                             
                                    p = Producer(ch, exchange=q.exchange, routing_key=q.routing_key)
                                    p.publish(initial_status)
                            
            except Exception as e:
                log.error(f'QUEUE CREATION ERROR: {e}')
                
            return qname_root

    def delete_queues(self, queue_name_root=None):
        '''
        The opposite of create_queues, will delete the queues that are specific to 
        the running instance of the task and remove its management data from the 
        management queue. 
        
        :param queue_name_root: Optionally a queue_name_root to use. This is useful
                                primarily for clean up of zombie queues.
        '''
        with Connection(current_app.conf.broker_url) as conn:
            try:
                # Connection is lazy. Force a connection now.
                conn.connect()
                c = conn.connection
                log.debug(f'\tConnection: {self.connection_names(c)[1]}')

                # Create a channel on the connection and log it in the RabbitMQ webmonitor format
                ch = c.channel()
                log.debug(f'\tChannel: {self.channel_names(ch)[1]}')
                
                # Create or get the exchange
                x = Exchange(self.exchange_name, channel=ch)
                log.debug(f'\tExchange: {x.name}')

                # Get the queue name root from management data 
                if not queue_name_root:
                    queue_name_root = self.getattr('queue_name_root', None)
                    if not queue_name_root:
                        queue_name_root = self.get_management_data()
                        if not queue_name_root:
                            log.debug(f'Delete Queues: Request to delete queues cannot be fulfilled for lack of a queue root name,')
                
                if queue_name_root:
                    # And the queue names
                    qnames = self.queue_names(queue_name_root)
                    
                    # Create the queues
                    for k, qname in qnames.items():
                        # We keep the management queue, that is never deleted
                        if not k == self.management_key():
                            q = Queue(qname, channel=ch, no_declare=True)
                            q.delete()
                            log.debug(f'Deleted Queue: {q.name}')
                        
            except Exception as e:
                log.error(f'QUEUE DELETION ERROR: {e}')
    
    def kill_zombies(self):
        '''
        Attempts to find zombie queues using the persistent and durable 
        management data, that is queues that have no active process using 
        them, and delete them. Simple housekeeping.
        
        Technically should not arise as only the management queue is
        durable and the others should die with a server restart. 
        '''
        mgmt_data = self.get_management_data(all=True)
        active = current_app.control.inspect().active()
        
        active_ids = set()
        for node_id in active:
            for task_info in active[node_id]:
                if task_info['name'] == self.name:
                    active_ids.add(task_info['id'])
                    
        managed_ids = set(mgmt_data.keys()) if mgmt_data else set()
        
        log.debug(f'Zombie Search, active ids: {active_ids}')
        log.debug(f'Zombie Search, managed ids: {managed_ids}')
        log.debug(f'Zombie Search, happy with: {active_ids & managed_ids}')
        log.debug(f'Zombie Search, will delete from management data: {managed_ids - active_ids}')
        log.debug(f'Zombie Search, will cull from active tasks: {active_ids - managed_ids}')

        # Any tasks being managed but not active are not needed in in management data
        # But they may have left zombie queues lying around. So we look for and delete the
        # queues and then remove the task form the management data.
        del_ids = managed_ids - active_ids
        if del_ids:
            with InteractiveExchange(self) as x:
                for task_id in del_ids:
                    queue_name_root = mgmt_data[task_id]
                    qnames = self.queue_names(queue_name_root, except_management=True)
                    for qname in qnames:
                        q = Queue(qname, channel=x.channel, no_declare=True) # TODO: need channel at least
                        q.delete()
    
            self.del_management_data(del_ids)
        
        # Then, any active tasks that don't have management data probably got
        # lost along the way somewhere. If we're feeling aggressive we can ask 
        # them to abort. 
        #
        # We don't know the names of these queues as we don't have their 
        # queue_name_root (which we stored in management data which they are
        # lacking). But we can send a message via the exchange and use their
        # instruction routing key.        
        ids_to_cull = active_ids - managed_ids
        if ids_to_cull and self.cull_forgotten_tasks:
            try:
                with InteractiveExchange(self) as x:
                    for task_id in ids_to_cull:
                        # TODO: THis is currently broken because of Kombu bug:
                        # See: 
                        #    https://github.com/celery/kombu/issues/1174
                        # 
                        # In mean time must specify content encoding explicitly
                        x.publish(self.DIE_CLEANLY, routing_key=self.instruction_key(task_id), content_encoding='utf-8')
            except Exception as e:
                log.error(f'ZOMBIE KILL ERROR: {e}')
                
        log.debug(f'Zombie Kill Done.')
    
    def connection_names(self, connection):
        '''
        Returns  a simple 2-tuple of long name and short name for the
        connection that matches what the RabbitMQ web monitor displays.
        
        Kombu connections don't have names per se. In the monitoring 
        interfaces they are identified by local and remote socket 
        properties.
        
        A simple convenience for debug logging.
        
        :param connection: a Kombu Connection
        '''
        laddr = connection.sock.getsockname()
        raddr = connection.sock.getpeername()
        name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
        name_short = f"{laddr[0]}:{laddr[1]}"
        return (name, name_short)
    
    def channel_names(self, channel):
        '''
        Returns  a simple 2-tuple of long name and short name for the
        channel that matches what the RabbitMQ web monitor displays.
        
        Kombu channels don't have names per se. In the monitoring 
        interfaces they are identified by local and remote socket 
        properties.
        
        A simple convenience for debug logging.
        
        :param channel: a Kombu Channel
        '''
        laddr = channel.connection.sock.getsockname()
        raddr = channel.connection.sock.getpeername()
        cname = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
        cname_short = f"{laddr[0]}:{laddr[1]}"

        name = f'{cname} ({channel.channel_id})'
        name_short = f'{cname_short} ({channel.channel_id})'
        
        return (name, name_short)
    
    @property
    def exchange_name(self):
        return "celery.interactive"
    
    def set_management_data(self, queue_name_root=None):
        '''
        Fetches the management data from the management queue, and ensures
        this task has an entry there which conveys the queue_name_root it
        was assigned/is using.
        '''
        my_id = self.request.id
        my_qnr = queue_name_root if queue_name_root else getattr(self, "queue_name_root", None)
        
        if not my_qnr:
            log.error(f'Interactive queue name root missing!')
            return
        
        log.debug(f'Setting interactive management data: {self.fullname}')
        
        with ManagementQueue(self) as q:
            try:
                mgmt_mesg = q.get()
                
                if mgmt_mesg:
                    mgmt_data = mgmt_mesg.payload
                    log.debug(f'\tExisting Management Data: {mgmt_data}')

                    if isinstance(mgmt_data, dict):
                        mgmt_mesg.ack()
                        mgmt_data[my_id] = my_qnr
                    else:
                        mgmt_mesg.reject()
                        log.error(f'Interactive management data is corrupt!')
                else:
                    mgmt_data = {my_id: my_qnr}
                
                q.publish(mgmt_data)
                log.debug(f'\tNew Management Data: {mgmt_data}')
                
            except Exception as e:
                log.error(f'SET MANAGEMENT DATA ERROR: {e}')
    
    def get_management_data(self, all=False):  # @ReservedAssignment
        '''
        Peeks onto the Management Queue (named after the task) to see if it
        can find an entry for self (based on self.request.id).
        '''
        my_id = self.request.id
        
        log.debug(f'Getting interactive management data: {self.fullname}')
        
        with ManagementQueue(self) as q:
            try:
                # Peek at the management data on the management queue
                mgmt_mesg = q.get()
                if mgmt_mesg:
                    mgmt_data = mgmt_mesg.payload
                    mgmt_mesg.requeue()
                    log.debug(f'\tManagement Data: {mgmt_data}')
                    
                    if isinstance(mgmt_data, dict):
                        if all:
                            return mgmt_data
                        else:
                            return mgmt_data.get(my_id, None)
                    else:
                        log.error(f'Interactive management data is corrupt!')
                else:
                    log.warning(f'Interactive management data was missing.')
                    q.publish({})
                    return {}
                
            except Exception as e:
                log.error(f'GET MANAGEMENT DATA ERROR: {e}')
    
    def del_management_data(self, task_ids=None):
        '''
        Fetches the management data from the management queue, and deletes
        this task's entry if there is one.
        '''
        my_id = self.request.id
        
        log.debug(f'Deleting interactive management data: {self.fullname}')
        
        with ManagementQueue(self) as q:
            try:
                mgmt_mesg = q.get()
                
                if mgmt_mesg:
                    mgmt_data = mgmt_mesg.payload
                    log.debug(f'\tExisting Management Data: {mgmt_data}')

                    if isinstance(mgmt_data, dict):
                        mgmt_mesg.ack()
                        
                        if task_ids:
                            for task_id in task_ids:
                                mgmt_data.pop(task_id, None)
                        else:    
                            mgmt_data.pop(my_id, None)
                    else:
                        mgmt_mesg.reject()
                        log.error(f'Interactive management data is corrupt!')
                else:
                    mgmt_data = {}
                
                q.publish(mgmt_data)
                log.debug(f'\tNew Management Data: {mgmt_data}')
                
            except Exception as e:
                log.error(f'DEL MANAGEMENT DATA ERROR: {e}')

    @ConnectedCall
    def __call__(self, *args, **kwargs):
        # We assume the Task is bound, i.e. #app.task(bind=True)
        # TODO: We could check if it or isn't and insert self only if bound.
        return self.run(self, *args, **kwargs)

    def instruct(self, instruction):
        '''
        Given an instance of celery.Task (self) and an instruction will,
        send an instruction to that task. 
        
        Requires that the task has the attribute "instruction_queue" which is
        a Kombu producer pointing to the instruction queue. The ConnectedView
        decorator wraps a view in a connection that provides that attribute.
        
        :param instruction: The instruction to send (a string is ideal but kombu has to be able to serialize it)
        '''
        log.debug(f'Sending Instruction: {instruction} -> {self.request.id}')
        
        self.instruction_queue.publish(instruction)
    
        log.debug(f'\tSent: {instruction} to exchange {self.instruction_queue.exchange.name} with routing key {self.instruction_queue.routing_key}')
    
    def check_for_instruction(self):
        '''
        Performs a non-blocking read on self.instruction_queue 
        (i.e. checks for an instruction).
        
        Returns None if no instruction found.
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.instruction_queue to check for abort messages."

        log.debug(f'CHECKING queue "{q.name}" for an instruction.')
            
        try:
            message = q.get()
        except Exception as E:
            # TODO: Diagnose why my Queue dies, if it does ... 
            message = None
            log.error(f'Error checking {getattr(q, "name")}: {E}')

        if message:            
            instruction = message.payload       # get an instruction if available
            message.ack()                       # remove message from queue

            if instruction == self.DIE_CLEANLY:
                self.die_cleanly()

            return instruction
        else:
            return None
    
    def wait_for_instruction(self, prompt=None, continue_monitoring=None):
        '''
        Performs a blocking read on self.instruction_queue 
        (i.e. waits for an instruction).
        
        :param prompt: Optionally, a prompt that will be sent along with 
                       the state update to WAITING, so the View can if
                       it wants, prompt a user (by whatever means it can) 
                       to provide an instruction.
                       
        :param continue_monitoring: If a string is provided will be 
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.q to check for abort messages."
        
        instruction = None
        
        def got_message(body, message):
            nonlocal instruction
            instruction = body  # Not really an @UnusedVariable
            message.ack()
        
        meta = {'prompt': prompt, 'progress': self.progress.as_dict(), 'continue_monitoring': continue_monitoring}
        self.send_update(state="WAITING", meta=meta)
        
        log.debug(f'Updated status to WAITING with info: {meta}')
        log.debug(f'WAITING for an instruction ... (listening to queue: {q.name})')
            
        ch = q.exchange.channel
        c = ch.connection
        with Consumer(ch, queues=q, callbacks=[got_message], accept=["text/plain"]):
            # drain_events blocks until a message arrives then got_messag() is called.  
            c.drain_events()
        
        log.info(f'RECEIVED instruction: {instruction}')
        
        if instruction == self.DIE_CLEANLY:
            self.die_cleanly()
            
        return instruction
    
    def send_update(self, state, meta=None):
        '''
        Called by a running task (Task/Worker) to send an update to a client (Task/Client)
        
        Has a signature like that of self.update_state() - a Celery built-in, but respects
        the choice of broker and/or backend for communicating the update to the client. 
        
        The client in turn can call task.get_update() to, similarly, check for an update
        respecting the choice of broker and/or backend for the communication.
        
        :param state: A string describing the state of the running task
        :param meta:  A dictionary (with string keys) containing extra information for the update
        '''
        if self.update_via_backend:
            log.debug(f'Updating State, state:{state}, meta: {meta}')
            self.update_state(state=state, meta=meta)
            
        if self.update_via_broker:
            log.debug(f'Sending Update: {self.request.id} -> {state}, {meta}')
            m = (state, meta)
            self.update_queue.publish(m)
            log.debug(f'\tSent: {m} to exchange {self.update_queue.exchange.name} with routing key {self.update_queue.routing_key}')

    class Result:
        '''
        A basic Result class that masquerades as a simple Celery.AsyncResult class.
        
        Simple has a clear meaning here. All it does is make the state, info/result/meta
        attributes available in exactly the same way AsyncResult does. A list of Result 
        objects is what get_update returns and each one can, like an AsyncResult be asked
        for state, and info/result (these are two names for the same thing in Celery 
        AsyncResult.
        
        The info/result/meta story:
            AsyncResult supports a result and info attribute that are identical.
            Celery's update_state takes a meta kwarg, which is used to populate 
            result/info. So we support all three names in this masquerade.
            
            They are in Celery terms synonyms in the AsyncResult object. 
        '''
        state = status = None
        info = result = meta = None
        
        def __init__(self, state, meta):
            # See: https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.AsyncResult.state
            #
            # Standard states are:
            # 
            # PENDING    
            #     The task is waiting for execution.
            # STARTED
            #     The task has been started.
            # RETRY
            #     The task is to be retried, possibly because of failure.
            # FAILURE
            #     The task raised an exception, or has exceeded the retry limit. The result attribute then contains the exception raised by the task.
            # SUCCESS
            #     The task executed successfully. The result attribute then contains the tasks return value.            
            self.state = self.status = state
            
            # See: https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.AsyncResult.info
            #
            # Can be either:
            #
            # What the task returns - in the case of SUCCESS
            # An exception instance - in case of FAILURE
            #
            # Anything else for custom custom states but notably:
            #
            # A dict - in the case of any call to task.update_state in which case
            #          in which case it has the value of the meta argument (which is a dict)
            #          See:  https://docs.celeryproject.org/en/stable/reference/celery.app.task.html#celery.app.task.Task.update_state
            self.info = self.result = self.meta = meta
            
        def __eq__(self, other):
            return self.status == other.status and self.info == other.info

    def get_updates(self, task_id=None):
        '''
        Called by a client (Task/Client) to get updates sent by a running worker (Task/Worker)  
        
        Fetches the latest updates from the a running instance of this task. It needs a task id and
        this can be provided as a kwarg or found in self.request.id.
         
        :param task_id:
        '''
        if task_id:
            self.request.id = task_id
        elif self.request.id:
            task_id = self.request.id

        if task_id:            
            log.debug(f'Fetching updates from: {self.fullname}')
    
            if self.update_via_backend:
                r = AsyncResult(task_id)
                backend_update = self.Result(r.state, getattr(r, 'info', None))
                log.debug(f'\tBackend update: {backend_update.state}, {backend_update.info}')
    
            if self.update_via_broker:
                messages = []
                broker_updates = []
                
                # Repeat until no more messages
                while True: 
                    try:
                        message = self.update_queue.get()
                    except Exception as E:
                        # TODO: Diagnose why my Queue dies, if it does ... 
                        message = None
                        log.error(f'Error checking update queue, {self.update_queue.name}: {E}')
                    
                    if message:
                        messages.append(message)
                        payload = message.payload
                        # The payload is a 2-tuple containing state and info
                        result = self.Result(*payload)
                        broker_updates.append(result)
                        log.debug(f'\tBroker update: {result.status}, {result.info}')
                    else:
                        break
                    
                # The last message should be requeued so that we always have the last update
                # available at least (the queue is never empty). This mimics Celery's AsyncResult
                # in that we will always have a result available. All other ones we can ack() to
                # remove them from the queue.
                if messages:
                    for message in messages[:-1]:
                        message.ack()
                        
                    messages[-1].requeue()
                
            if self.update_via_broker and not self.update_via_backend:
                return broker_updates
            elif self.update_via_backend and not self.update_via_broker:
                return [backend_update]
            elif self.update_via_backend and self.update_via_broker:
                if broker_updates:
                    # All is good if the backend update agrees with the last broker update
                    if backend_update == broker_updates[-1]:
                        log.debug(f'\tBackend and Broker agree.')
                        return broker_updates
                    else:
                        # If we are prioritising the broker ignore the backend result and return the brokers
                        if self.update_via_broker > self.update_via_backend:
                            log.debug(f'\tBackend and Broker disagree. Broker is prioritised')
                            return broker_updates
                        # Otherwise if we are priortising the backend then
                        # return both the broker and backend results but put backend's 
                        # results at end (latest update). That way when reading the updates
                        # (with get_updates) the history leading up to the backend update
                        # is available - which is whole reason we offer a update_by_broker 
                        # namely because of Celery's retaining only the last update)
                        else:
                            broker_updates.append(backend_update)
                            log.debug(f'\tBackend and Broker disagree. Backend is prioritised.')
                            return broker_updates
                else:
                    # This means we didn't persist the update. We should persist it and check 
                    # the persisted one against the AsynchResult (backend_update. 
                    log.debug(f"get_updates: INTERNAL ERROR. No broker update in Queue. One should always be on the Queue.") 
                            
            return broker_updates
        else:
            log.debug(f"get_updates: No task_id provided or found in self.request.id")
            return [] 