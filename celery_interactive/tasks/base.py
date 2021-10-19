from celery import current_app, Task as Celery_Task
from celery.signals import before_task_publish
from celery.exceptions import Ignore

from .. import log
from ..web.django import Django
from ..progress import Progress, default_steps, default_stages

import threading

class InteractiveBase(Celery_Task):
    '''
    Augments the Celery.Task with some additional functions and features
    that make Interactive Celery tasks a breeze.  
    
    The problem as at Celery 4.4, is that it offers us the means to start tasks 
    and get feedback from them, but it provides us with no documented or easy means 
    to send running tasks any further instructions.
    
    Celery rests upon, works with and provides two directions of communication between 
    a Client (who wants the task performed) and a Worker (who will perform the task):
    
    Client -> Worker: broker. 
        The broker is by default an amqp provider like RabbitMQ or Redis.
        In fact as at Celery 4.4 only amqp brokers are supported:
            http://docs.celeryproject.org/en/latest/getting-started/brokers/
        
    Worker -> Client: backend
        Many more backends are supported than brokers:
            https://docs.celeryproject.org/en/stable/userguide/configuration.html#task-result-backend-settings
        The backend is intended for storing results upon completion but for long
        running tasks can also be used to send updates as the task progresses 
        (to drive progress bars for example).
            https://www.distributedpython.com/2018/09/28/celery-task-states/
            
    Workers though can be configured and are by default configured to support concurrency, 
    that is to run more than one task at a time, and does this by setting up an execution pool:
        https://www.distributedpython.com/2018/10/26/celery-execution-pool/
        
    The consequence is, not least in the default pooling strategy which sets up a worker 
    (which Client can send task requests to via the broker) and forks a number of child 
    processes to actually execute the tasks. It holds comms with its children close to its 
    chest though and the broker only provides means of communicating with the worker, 
    not its children. 
    
    Common reason to send a request (via the broker) to a worker are:
    
    starting tasks:
        https://docs.celeryproject.org/en/latest/userguide/calling.html
    controlling the worker:
        https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
        https://medium.com/@djsmith42/creating-custom-celery-commands-1f0692d01918
    
    But there are at least two common use cases in which further instructions to a 
    running task NOT the worker (that started the task in its execution pool) are 
    very useful:
    
    1) When desiring to terminate/abort a running task.
    
        Celery provides a revoke mechanism:
            in celery.app.control:    https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
            in celery.result:         https://docs.celeryproject.org/en/stable/reference/celery.result.html
        but revoke only cancels a task BEFORE it starts running, 
        that is, if it's still in a worker's queue, scheduled to start.
        So, not very useful for long running tasks we'd like to terminate.
        
        Celery also provides in its contrib module an AbortableTask variant:
            in celery.contrib.abortable: https://docs.celeryproject.org/en/latest/reference/celery.contrib.abortable.html
        but this is poorly implemented:
            a) it abuses the result backend to implement a client -> worker 
               communciation (recall the result backend is intended for 
               worker -> client comms).
            b) as a consequence it only works for database backends (which permit this
               abuse) that many of us aren't using and don't wish to use (in fact rpc 
               backends come highly recommended - and utilize the same amqp provider that
               the broker uses).
            
    2) To provide user input to a complicated task
    
        There is no currently documented means for doing this, only expressed desires 
        to see one:
            https://stackoverflow.com/questions/30481996/interact-with-celery-ongoing-task
            https://stackoverflow.com/questions/59796397/celery-interact-communicate-with-a-running-task
        
        A basic use case for this is in processing very complicated data submission with far 
        reaching consequences and a good deal of data cleaning and validation overhead. In 
        such a case we can perform all validation and impact assessment inside of a database 
        transaction. In the Django case:
            https://docs.djangoproject.com/en/3.0/topics/db/transactions/#controlling-transactions-explicitly
        
        Having performed this validation and impact assessment we now seek input from the 
        user to confirm intent, that the transaction should be committed. The database 
        transaction has to remain open while we send a response to the user declaring 
        the impact, and requesting confirmation. If that can all happen in one process 
        all good and fine but in a web service this demands particular attention as
        the processes responding to HTTP requests often have a life-span of one request 
        (cannot hold a database transaction open between two requests). Particularly in 
        the Django case with the transaction context manager there is no way within that 
        context to round trip a confirmation request to the user.
        
        The most common work around cited, is to save the original request, perform whatever 
        impact assessments we need, consult the user, and then if requested to commit, load
        the saved request and commit it. That is in fact a very sensible and sound solution
        provided the impact assessment is simple and clean. If the impact assessment is best
        performed by saving Django objects (easily the case for very complicated far reaching 
        submission, say changing a setting that impacts a great many Django objects/database 
        tables and rows). In such cases it can be much easier to assess impact inside of a 
        transaction, perform all the required changes to the database, summarize impacts, 
        request permission to commit, and then commit if granted. If we implement such a 
        transaction inside a Django view it must be committed or rolled back before it's 
        done (and so we can't ask the user for input).
        
        And so a transaction manager is needed. A daemon process that can open and hold 
        open a database connection and transaction and keep it open while the web server 
        round trips a confirmation request. So that we can decide on committing or rolling
        back with one or more round trips to the user (i.e. across multiple Views). 
        
        For this celery tasks are well suited as they run in workers that meet this 
        criterion and provide all of the comms needed to start the task and 
        get results back (an impact assessment along with request to confirm). But 
        there is no means by which the web server can send the users response back
        to the waiting task. This class provides a plug and play means to do so.
        
    These references were used in building this solution:
        https://ask.github.io/celery/tutorials/clickcounter.html
        https://docs.celeryproject.org/projects/kombu/en/stable/userguide/examples.html
        
    Provided are, a decorator:
    
        Django.PulseCheck: 
                which decorates a Django view function.
                
                Intended for use by a progress bar that sends back an AJAX request 
                to the function thus decorated, checking the pulse of the task. It 
                checks the status of the task and returns a response that the progress 
                bar can use, and if a cancel has been requested sends an abort message 
                to the task and if an instruction was submitted (in the request), sends 
                the instruction to the task.
                
                It is up the task to check the queue for cancel requests or instuctions
                and act on them. If the task ignores them they will have no effect.
                The task can call methods like self.check_for_abort(), or 
                self.check_for_instruction() and act on the results - these check
                self.q quietly and act (raise Abort) or report as required.

    and two Django views:
    
        django.pulse:
                An empty Django.PulseCheck decorated view. Saves you writing 
                a view if all you want is the standard features. If you want 
                to add any information to response, you can write your own 
                view and decorate it with Django.PulseCheck.
                
        django.instruct:
                A simple view which sends an instruction to a running task.
                Entirely up to the task if it checks for and acts on it it.
                All this does is put the instruction into the queue for that
                task. Expects the instruction in the request. 
                
    '''

    ############################################################################################    
    # Attributes
    ############################################################################################    
    
    # A configuration that permits or prohibits parallelism.
    # Set to True if you want that only one running instance is
    # permitted at a time. Can be enabled in the decorator with:
    # @app.task(bind=True, base=Interactive, one_at_a_time=True)   
    one_at_a_time = False

    # Try and cull any forgotten tasks before starting this one.
    # Adds a little start up overhead, and can be turned off by
    # decorating with:
    # @app.task(bind=True, base=Interactive, cull_forgotten_tasks=False)   
    cull_forgotten_tasks = True

    # Configuring the update mechanism (message from task to client)
    #
    # 0 = Don't use this mechanism
    # non-zero = higher of two breaks conflicts
    #
    # One or more need to be non-zero 
    # If both are non-zero, the higher  one breaks any conflicts 
    # (if different, updates are understood by checking each one).
    #
    # Celery ordinarily sends updates via its results backend. This
    # has come concerning properties, not least when an RPC backend is 
    # in use.
    #
    # See: 
    #        https://docs.celeryproject.org/en/stable/userguide/tasks.html#states
    #
    #        "When a task moves into a new state the previous state 
    #        is forgotten about, but some transitions can be deduced, 
    #        (e.g., a task now in the FAILED state, is implied to have 
    #        been in the STARTED state at some point)."
    #
    # In other words if an Interactive task wants to send an update like
    # WAITING back to the client, any number of possible reasons might 
    # provoke Celery to send a an update of state and if it does, the 
    # WAITING state is never noticed. 
    # 
    # The only evidence for this we've spotted is the state of PENDING
    # overwriting a state like WAITING. If we notice WAITING (i.e. check
    # pulse before the state is overwritten) this can be handled by the
    # Task/Client by remembering that it saw WAITING and interpreting all
    # PENDING states until the wait has ended as STILL_WAITING.
    #
    # Which is precisely what the Django PulseCheckView does, but it's
    # entirely up to the Client implementation to do such. 
    #
    # But this won't work if Celery sends a PENDING state before we notice
    # the WAIITNG state. So it depends on the frequency of state checking
    # and on how frequently this happens.
    #
    # And this is just one exemplar of what can go wrong if an Interacive
    # task relies on Celery's state focussed update mechanism (i.e. only 
    # interesed in the Tasks' current state not its history of states),
    # that if more than one update is sent between state checks, only the
    # last would be seen. 
    #
    # And so given we're already sending instructions to the Task/Worker 
    # via the broker we can also have Task/Worker send updates back via 
    # the broker (and not the via the results backend). The advnatage here 
    # is that no updates are overwritten, we can see and act on all updates.
    #
    # We can always continue sending updates via the backend as well (or
    # even exclusively if we're happy to risk not noticing some). But it's 
    # strongly recommended to proritise updates via the broker and use the
    # updates via backend only as an integrity check. If they are both 
    # enabled (non zero) then self.get_updates() will check both and 
    # if the backend agrees with the last broker update, be happy,
    # or in case of a conflict prioritse the the higher of the two 
    # and log a warning (not a critical error, but certain very fishy)
    #
    # These can of course be set with the task decorator, e.g:
    # @app.task(bind=True, base=Interactive, update_via_backend=0)   
    
    update_via_backend = 1
    update_via_broker  = 2

    # The Task/Worker is ready to accept instructions from Task/Client.
    # That is the whole raison d'etre of Interactive tasks. That clients
    # can interact with the running task. This is predicated on the 
    # Task/Worker checking for instriuctions and acting on them of course.
    # Interactive only provides the tools for Task/Worker to do that, cannot
    # oblige it to.
    #
    # Some standard instructions are implemented below (cancel, abort, die, 
    # commit, rollback ...) and they are associated with specific templates
    # in self.django.templates. General instructions can be sent using 
    # self.instruct() and a template for those is also configured in 
    # self.django.templates. When the task receives an instruction
    # with internal methods self.check_for_instruction() or 
    # self.wait_for_instruction() then it sends an INSTRUCTED update to
    # the client. The client can then continue or redirect in response to
    # an acknowledgement of the the Instruction. 
    #
    # This can be configured with the following three settings. An instruction
    # is just a string. And these first two are lists of instructions.
    # If an instruction is in the continue list the client will continue doing
    # what its doing (probably monitoring progress of the task), and if it's
    # in the redirect list, it will redirect to a page defined by 
    # self.django.templates.instructed.
    #
    # the default response is either "continue" or "redirect" and describes
    # what to do if the instruction is in neither list. 
    #
    # It is entirely up to the status checking agent to honor these, as they
    # are (honoured) in the Django PulseCheckView. 
    instruction_response_continue = []
    instruction_response_redirect = []
    instruction_response_default = "continue"

    # After sending a task an instruction we have a configurable sleep time
    # to wait for its repsonse to appear in the queue. Use this with caution, 
    # keeping it at 0 should be fine. Depends on the frequency of status checks.
    # This sleep will cause a delay in the HTTP response being delivered and
    # may be create poor UX. What it might do, if it's small enough is allow time 
    # for the task to receive the instruction and acknowledge it, so that 
    # The acknowledgement arrives on the status check performed after the 
    # instruction was sent, but in the same HTTP request, saving one round trip.  
    wait_for_abort = 0         # seconds
    wait_for_instructed = 0    # seconds
    
    # After sending a commit or rollback request to the task we can either
    # respond by with a template or continue monitoring. That is requested
    # by the task when it waits for an instruction, and/or when when it 
    # acknowledges that it's acted on the instruction. We can either redirect 
    # to the specified template on sending the instruction to the task or
    # wait until it's acknowledged or it's done (fulfilled the instruction).
    #
    # By default we'll wait for the acknowledgement. Which means we have 
    # to leave the question asker in place waiting. The asker of course
    # was either specified by the templated task.django.templates.confirm
    # or by the monitor defined by task.django.templates.monitor. Either
    # one may have been presenting the user with the question commit or 
    # rollback?
    #
    # TODO: If these are true then:
    #       The confirmation has a negative and positve URL.
    #       If these are false it should redirect to that URL as per normal 
    #       If these are True then needs to for an AJAX fetch no the 
    #       negative and positve URL, and pulsecheck waiting for
    #       the COMMITTED or ROLLEDBACK state. 
    #       It can use the Pulsechecker, but that needs to have an hourglass mode
    #       added in place of a progress bar. 
    wait_for_committed = True
    wait_for_rolledback = True
    
    # Define some standard strings used for prompts (Task -> Client) and
    # instructions  (Client -> Task).
    #
    # These need not be user friendly and the idea is that they don't 
    # clash with any strings a user might want to use.
    ASK_CONTINUE = "__continue?__"
    CONTINUE = "__continue__"
    ABORT = "__abort__"
    DIE_CLEANLY = "__die_cleanly__"

    ASK_COMMIT = "__commit?__"
    COMMIT = "__commit__"
    ROLLBACK = "__rollback__"

    ############################################################################################    
    # Methods
    ############################################################################################    
    
    def __init__(self, *args, **kwargs):
        # Add an instance of Django interfaces
        self.django = Django(self)
        # Add an instance of Progress to track progress
        # Initial steps or stages can be provided in the decorator attributes
        # e.g. @app.task(bind=True, steps=100, stages=3)
        self.progress = Progress(self, getattr(self, "steps", default_steps), getattr(self, "stages", default_stages))

    def apply_async(self, args=None, kwargs=None, **options):
        '''
        Before we publish the task (i.e. send a request via the broker
        to have a worker execute it) we want to create the queues
        needed to interact with it and let the task (Task/Worker)
        know the queue_name_root that we elected to use. 
        
        We need a task_id to do that, and we don't have one here yet
        (though we could specify one). So we tap into the before_task_publish
        signal that Celery issues, by which time we have a task_id, and
        patch the queue_name_root into the kwargs sent to the task there,
        
        We define the signal handler herein to hide it a level deeper
        (so it's not visible as a method of Interactive), and keep it 
        close to its functional context i.e. the signal fires just after
        apply_async() is called, just before the message requesting the 
        task to run is published.
        
        :param args:
        :param kwargs:
        '''
        @before_task_publish.connect
        def __create_queues__(sender, *args, **kwargs):
            '''
            The signal handler that creates queues just prior to 
            the task request being published.
            
            :param sender: a string naming the task that is being published.
            '''
            # sender being the name only, we fetch the task from the Celery
            # registry. This is an instance of the task that we can use to
            # call its create_queues() method. We might be able to use self
            # from the embracing apply_async context
            task = current_app.tasks[sender]
 
            # To create Queues we need the task ID. This is delivered by the 
            # before_task_publish signal in the headers. create_queues expects
            # it in self.request.id
            task.request.id = kwargs["headers"]["id"] 

            # Create the queues that we need (this also:
            #  1) Establishes a queue name root (qnr)
            #  2) Saves the qnr to management data
            log.debug("Creating Queues ...") 
            qnr = task.create_queues()
            
            # INFORM TASK/WORKER OF THE queue_name_root (qnr) we are using
            #
            # Task/Worker has access to the management queue to double check
            # this but we add it to the kwargs for the task as queue_name_root.
            #
            # That's the only Celery standard way there is to communicate 
            # something to the task before it runs.
            task_kwargs = kwargs["body"][1]
            task_kwargs["queue_name_root"] = qnr
            
            # Farm some time-consuming work out to a background thread so
            # as not to hold up delivery of a response.
            clean_up = threading.Thread(target=self.kill_zombies)
            send_context = threading.Thread(target=self.send_context)
            
            clean_up.start()
            send_context.start()
            
        # If we only want one instance of task running at a time, 
        # enforced this now before we publish the task (ask a 
        # worker to run it).
        #
        # We know one is running if there an entry on the management
        # queue already.
        
        if self.one_at_a_time:
            # Note: the Kombu exchange has a persistent delivery mode by default.
            #       meaning messages are stored in memory and on disk and survive
            #       server or Celery restarts.
            #
            #       See: "delivery_mode" and "durable" in
            #       https://docs.celeryproject.org/projects/kombu/en/stable/reference/kombu.html 
            #
            #       But, while the message survives the running worker likely does 
            #       not and so we might have to be more rigorous here. Tools we have 
            #       available for greater rigour include:
            #
            #       app.control.inspect().active()
            #
            #       which returns a dict of all running (active) tasks, We can cross
            #       check if it's already running against this to boost confidence.
            #
            #       app.control.inspect().stats()
            #
            #       reveals the PIDs of Celery and all its workers. We could save 
            #       these in management data as well, and when its returned, compare 
            #       the PIDs. If they are the same as before our trust in the lock 
            #       rises some more.
            #
            # TODO: Consider improving the trust we have in this lock as per notes above.
            already_running = self.get_management_data()
            if already_running:
                raise self.Exceptions.AlreadyRunning
            
        return super().apply_async(args, kwargs, **options)
   
    def start(self, *args, **kwargs):
        '''
        Start the task
        '''
        #result = self.delay(*args, **kwargs)
        result = self.apply_async(args=args, kwargs=kwargs)
        return result
    
    @property
    def shortname(self):
        return self.name.split('.')[-1]

    @property
    def fullname(self):
        return f"{self.shortname} -> {self.name} with id: {self.request.id}"

    #######################################################################
    # Instructions from Client to Task/Worker
    #######################################################################
    
    def please_continue(self):
        '''
        Called by Task/Client
        
        Sends an instruction to continue to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to ask a running task instance to continue, in response 
        to its request for such an instruction. It's entirely up to the task whether it's 
        even checking for let along acting on such instructions. 
        '''
        self.instruct(self.CONTINUE)

    def please_abort(self):
        '''
        Called by Task/Client

        Sends an instruction to abort to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to abort a running task instance. It's entirely up 
        to the task whether it's even checking for let along acting on such 
        instructions. The client  can't force it to stop with this method, in fact 
        there is not clean easy way in Celery to do that (though this:
        
            app.control.revoke(task_id, terminate=True, signal='SIGKILL')
            
        will kill the worker process).  But Celery docs note:
        
            The terminate option is a last resort for administrators when a 
            task is stuck. It’s not for terminating the task, it’s for 
            terminating the process that’s executing the task, and that 
            process may have already started processing another task at 
            the point when the signal is sent, so for this reason you must 
            never call this programmatically.
            
        So not want you want for clean exit. For a clean exit, alas, all
        we can do is ask the taks nicely to abort. This method does that.
        '''
        self.instruct(self.ABORT)

    def please_commit(self):
        '''
        Called by Task/Client

        Sends an instruction to commit to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.
        
        Used by a client wanting to ask a running task instance to commit results 
        to a database, in response to its request for such an instruction. It's 
        entirely up to the task whether it's  even checking for let along acting 
        on such instructions.
        '''
        self.instruct(self.COMMIT)

    def please_rollback(self):
        '''
        Called by Task/Client

        Sends an instruction to rollback to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to ask a running task instance to rollback an
        open databse transaction, in response to its request for such an instruction. 
        It's  entirely up to the task whether it's  even checking for let along acting 
        on such instructions.
        '''
        self.instruct(self.ROLLBACK)

    #######################################################################
    # Instructions from Task/Worker to Client
    #######################################################################

    def wait_for_continue_or_abort(self, continue_monitoring=None):
        '''
        Called by Task/Worker
        
        Waits for an instruction from the user, prompting to continue or abort
        '''
        # Outcome is always either an Abort exception or we continue, and if continuing we continue
        # any monitoring that was happening when we were called. 
        if not continue_monitoring:
            if getattr(self, "monitor_title", None):
                continue_monitoring = self.monitor_title
            else: 
                continue_monitoring = "<No Title Provided>"
                
        instruction = self.wait_for_instruction(self.ASK_CONTINUE, continue_monitoring)
        if instruction == self.ABORT:
            self.abort()
        else:
            assert instruction == self.CONTINUE, f"Invalid instruction {instruction} received by wait_for_continue_or_abort()"
            return instruction

    def wait_for_commit_or_rollback(self, continue_monitoring=None):
        '''
        Called by Task/Worker

        Waits for an instruction from the user, prompting for a commit or rollback.
        '''
        instruction = self.wait_for_instruction(self.ASK_COMMIT, continue_monitoring)
        if instruction == self.ROLLBACK:
            self.rollback()
        else:
            assert instruction == self.COMMIT, f"Invalid instruction {instruction} received by wait_for_commit_or_rollback()"
            return instruction

    # TODO: support a PAUSE instruction which when sent will, if enabled
    # cause a blocking check which waits for a continue instruction.
    # This should support a PAUSE button on the progress bar that just sees the task pause.
    def check_for_abort(self): 
        '''
        Called by Task/Worker

        Checks for an instruction and if it's an abort instruction will 
        abort the task (by raising self.Abort) else will just return the
        instruction. 
        '''
        instruction = self.check_for_instruction()
        if instruction == self.ABORT:
            self.abort()
        else:
            return instruction

    def die_cleanly(self):
        '''
        Called by Task/Worker. 

        The task should clean up the queues it was using (i.e. the client has abandoned that 
        job) and then terminate. This is reserved for instances where a task is waiting on
        user input for example, and never receives it. It sits around forever. But an effort
        to clean up zombies (tasks in this lost state) can send them a DIE_CLEANLY insttuction
        and the task on receiving it can call this to do that.
        
        Raises a Killed exception.
        '''
        self.send_update(state="KILLED")
        self.delete_queues()
        raise self.Exceptions.Killed
    
    def bail_with_fail(self, **kwargs):
        '''
        Called by Task/Worker.
        
        Bails from the task, sends a FAILURE state, and the supplied data back to Task/Client
        and prevents Celery from updating state further by raising Ignore(). 
        
        :param meta:
        '''
        # When state is FAILURE it seems Celery expects an Exception in meta.
        #
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
        #            
        # And FAILURE indicates an exceptionw as raised so celeryw ants an Exception in meta.
        self.send_update(state='FAILURE', meta=self.Exceptions.Bail(kwargs))
        raise Ignore()

    def abort(self):
        '''
        Called by Task/Worker. 
        
        Abort the task! Raises an Abort exception.        
        '''
            
        pd = self.progress.as_dict()
        pd['description'] = f"Aborted"
        self.send_update(state="ABORTED", meta={'progress': pd})
        raise self.Exceptions.Abort

    def rollback(self):
        '''
        Called by Task/Worker. 
        
        The task should roll back any open transaction it has. 
        
        Raises a Rollback excteption.         
        '''
        pd = self.progress.as_dict()
        pd['description'] = f"Rolledback"
        self.send_update(state="ROLLEDBACK", meta={'progress': pd})
        raise self.Exceptions.Rollback

    ############################################################################################    
    # END of TASK extensions
    #
    # BEGINNING of DJANGO INTERFACE
    ############################################################################################    

    # Include the Django subclass
    
    Django = Django

    class Exceptions:

        class Bail(Ignore):
            """A task can raise this to Bail execution.
            
               Bail differs only nuancally from Abort in that fail_with_bail 
               uses it as a carrier when sending the FAILURE status. The 
               implication is that the task has elected to bail of its own 
               accord while Abort suggest it was asked to abort by Client.
            
               It does nothing more than request Celery to ignore the 
               task from here on in (not send an update to state to
               to the client).
            """

        class Abort(Ignore):
            """A task can raise this to Abort execution.
            
               It does nothing more than request Celery to ignore the 
               task from here on in (not send an update to state to
               to the client).
            """
            
        class Killed(Ignore):
            """A task can raise this if asked to die cleanly.
            
               It does nothing more than request Celery to ignore the 
               task from here on in (not send an update to state to
               to the client).
            """
    
        class Rollback(Ignore):
            """A task can raise this to trigger a Rollback.
            
               If the task is wrapped in Django's @transaction.atomic 
               the exception can trigger a rollback. 
               
               A task can be wrritten as one transaction and ignore
               this exeption in which case it will act as a Celery 
               Ignore (i.e. asks Celery not to update state to SUCCESS
               and return a result, but leave the sate alone).
               
               Good practice would be to catch the Rollback exception
               update status to ROLLEDBACK and the raise Ignore. 
               
               If the task is written in multiple transactions, then
               the Rollback exception can be caught and teh task 
               continue to the next transaction.    

               An Interavtive task has a rollback() method which does
               this, namely sets status to ROLLEDBACK then raises this
               exception and so handles that for a task conveniently. 
               The task cna then just catch the exception and move onto
               the next transaction.
            """
            
        class AlreadyRunning(Exception):
            '''
            An exception thrown only if one_at_a_time is True and an
            instance of this task is already seen to be running.
            '''
