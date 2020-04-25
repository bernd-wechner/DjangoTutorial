from celery import current_app, Task as Celery_Task
from celery.signals import before_task_publish
from celery.exceptions import Ignore

from inspect import signature

class InteractiveBase(Celery_Task):
    '''
    Augments the Celery.Task with some additional functions andf features
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
            
    Workers though can be conifgured and are by default configured to to support concurrency, 
    that is to run more than one task at a time and does this by setting up an execution pool:
        https://www.distributedpython.com/2018/10/26/celery-execution-pool/
        
    The consequence is, not least in the default pooling strategy which sets up a worker 
    (which client can send task requests to via the broker) and forks a number of child 
    processes to actually execute the tasks. It holds comms with its children close to its 
    chest though and the broker only provides open means of communicating with the worker, 
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
        that is if it's still in a worker's queue scheduled to start.
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
        tuples). In such cases it can be much easier to assess impact inside of a transaction,
        perform all the required changes to the database, summarize impacts, request permission
        to commit, and then commit if granted. If we implement such a transaction inside a
        Django view it must be committed or rolled back before it's done.
        
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
        
    Provided a decorator:
    
        Django.PulseCheck: 
                which decorates a Django view function.
                
                Intended for use by a progress bar that sends back an AJAX request 
                to the function thus decorated, checking the pulse of the task. It 
                checks the status of the task and returns a response the progress 
                bar can use, and if a cancel has been requested sends an abort message 
                to the task and if an instruction was submitted (in the request), sends 
                the instruction to the task.
                
                It is up the task to check the queue for cancel requests or instuctions
                and act on them. If the task ignores them they will have no effect.
                The task can call methods like self.check_for_abort(), or 
                self.check_for_instruction() and act on the results - these check
                self.q quietly and act (raise Abort) or report as required.

    and provides two Django views:
    
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
    
    _progress = None
    
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

    # Configuring the update mechanism
    #
    # 0 = Don't use this mechanism
    # non-zero = higher of two breaks conflicts
    #
    # One or more need to be non-zero 
    # If both are non-zero, the higher  one breaks any conflicts 
    # (if  different updates are understood by checking each one).
    update_via_backend = 1
    update_via_broker  = 2

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
        self.django = self.Django(self)

    def apply_async(self, args=None, kwargs=None, **options):
        '''
        Before we publish the task (send a request via the broker
        to have a worker execute it) we want to create the queues
        needed to interact with it and let the task (Task/Worker)
        know the queue_name_root that we elected to use. 
        
        We need a task_id to do that, and we don't have one here yet
        (though we could specify one). So we tap into the before_task_publish
        signal that Celery issues, by which time we have a task_ID, and
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
            # sender being the name only we fetch the task from the Celery
            # registry. This is an instance of the task that we can use to
            # use its create_queues() method. We might be able to use self
            # from the embracing apply_async context
            task = current_app.tasks[sender]
 
            # To create Queues we need the task ID. This is delivered by the 
            # before_task_publish signal in the headers. create_queues expects
            # it in self.reques.id
            task.request.id = kwargs["headers"]["id"] 

            # Create the queues that we need (this also:
            #  1) Establishes a queue name root (qnr)
            #  2) Saves the qnr to management data
            if task.update_via_broker:
                # TODO: consider what meta data we might add to this status
                #
                # I think the PIDs of celery and it's pool workers is a good 
                # thing to record with this state. 
                #
                # Stats are reported in three tiers:
                #    key 1: node name - a node is one worker in a cluster. A cluster
                #           may have many nodes. See:
                #             https://stackoverflow.com/a/61316552/4002633
                #             https://docs.celeryproject.org/en/latest/reference/celery.bin.multi.html#celery.bin.multi.MultiTool.MultiParser.Node
                #           The name is not meaningful per se, and casn't be divined other than 
                #           by looking at the keys of the stats dict. By default nodes are named 
                #           as celery@hostname, or if more than one node in a cluster, celery1@hostname, 
                #           celery2@hostname etc. But it can be configured when the worker is started.
                #    key 2: 'pid' holds the worker PID and 'pool' holds a pool dict
                #    key 3: 'processes' holds a list of PIDs of the pool processes for this node.
                #
                # We can capture all these PIDs in a single dir keyed on node PID with
                # a list of Pool PIDs as the value. And that might be useful for us to guage
                # stability of the worker pool. Or not. 
                stats = current_app.control.inspect().stats()
                
                assert stats, "Celery appears to be down! Can't start I Interactive Tasks without Celery."
                
                PIDs = {}
                for node in stats:
                    PIDs[stats[node]['pid']] = stats[node]['pool']['processes']
                
                initial_status = ("REQUESTED", PIDs)
                qnr = task.create_queues(initial_status)
            else:
                qnr = task.create_queues()
            
            # INFORM TASK/WORKER OF THE queue_name_root we are using
            #
            # Task/Worker has access to the management queue to double check
            # this but we add it to the kwargs for the task as queue_name_root.
            #
            # That's the only Celery standard way there is to communicate 
            # something to the task before it runs.
            task_kwargs = kwargs["body"][1]
            task_kwargs["queue_name_root"] = qnr
            
        # If we only want one instance of task running at time, 
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
            #       these in management data as well, and when its returned compare 
            #       the PIDs. If they are the same as before our trust in the lock 
            #       rises some more.
            already_running = self.get_management_data()
            if already_running:
                raise self.Exceptions.AlreadyRunning
            
        return super().apply_async(args, kwargs, **options)
   
    def start(self, *args, **kwargs):
        '''
        A wrapper around self.delay()
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
                    
    def progress(self, percent=0, current=0, total=0, description="", result=""):
        '''
        Trivially, builds a consistent JSONifieable data structure to describe task progress
        to a progress bar running in Javascript on a web browser.
        
        Called with no arguments returns a 0,0,0 progress indicator, useful at
        outset or to simply create the dictionary.
        
        :param percent: 0 to 100 indication %age complete
        :param current: The last step completed (an integer) 
        :param total: The total number of steps before the task is complete
        :param description: A string describing the last step completed
        :param result: An interim result if wanting to report one
        '''
        
        P = {}
        for p in list(signature(self.progress).parameters):
            P[p] = eval(p)
            
        self._progress = P
        return P                
    
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

    def send_progress(self, progress, check_for_abort=True):
        '''
        Sends a simple progress report back to the Client.
        
        :param progress:
        '''
        self.send_update(state="PROGRESS", meta={'progress': progress})
        if check_for_abort:
            self.check_for_abort()

    def wait_for_continue_or_abort(self, interim_result=None, progress=None, continue_monitoring=None):
        '''
        Waits for an instruction from the user, prompting to continue or abort
        '''
        # Outcome is always either an Abort exception or we continue, and if continuing we continue
        # any monitoring that was happening when we were called. 
        if not continue_monitoring:
            if getattr(self, "monitor_title", None):
                continue_monitoring = self.monitor_title
            else: 
                continue_monitoring = "<No Title Provided>"

        instruction = self.wait_for_instruction(self.ASK_CONTINUE, interim_result, continue_monitoring)
        if instruction == self.ABORT:
            self.abort(progress, interim_result)
        else:
            assert instruction == self.CONTINUE, f"Invalid instruction {instruction} received by wait_for_continue_or_abort()"
            return instruction

    def wait_for_commit_or_rollback(self, interim_result=None, progress=None, continue_monitoring=None):
        '''
        Waits for an instruction from the user, prompting for a commit or rollback.
        '''
        instruction = self.wait_for_instruction(self.ASK_COMMIT, interim_result, continue_monitoring)
        if instruction == self.ROLLBACK:
            self.rollback(progress, interim_result)
        else:
            assert instruction == self.COMMIT, f"Invalid instruction {instruction} received by wait_for_continue_or_abort()"
            return instruction

    # TODO: support a PAUSE instruction which when sent will, if enabled
    # cause a blocking check which waits for a continue instruction.
    # This should support a PAUSE button on the progress bar that just sees the task pause.
    def check_for_abort(self, progress=None, result=None): 
        '''
        Checks for an instruction and if it's an abort instruction will 
        abort the task (by raising self.Abort) else will just return the
        instruction. 
         
        :param progress: a progress indicator in form of self.progress()
                         optional, and will provide it to a client with 
                         the state update to ABORTED. 
        :param result:   a result indicator. Given the task is aborted 
                         and won't run to completion an opportunity to
                         return the partial result here. Provided to 
                         clients along with the sate update to ABORTED
        '''
        instruction = self.check_for_instruction()
        if instruction == self.ABORT:
            self.abort(progress, result)
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
        '''
        self.send_update(state="KILLED")
        self.delete_queues()
        raise self.Exceptions.Killed

    def abort(self, progress=None, result='unfinished result'):
        '''
        Called by Task/Worker. 
        
        The task should abort.
        
        :param progress: latest progress if any.
        :param result:   latest result if any. 
        '''
        # If no progress indicator is provided use the last one that self.progress 
        # was used for. Because this is called by the running task instance it has
        # persistence during execution and self._progress is up to date.
        if not progress:
            progress = self._progress
            
        progress['description'] = f"Aborted"
        self.send_update(state="ABORTED", meta={'result': result, 'progress': progress})
        raise self.Exceptions.Abort

    def rollback(self, progress=None, result='unfinished result'):
        '''
        Called by Task/Worker. 
        
        The task should roll back any open transaction it has.
        
        :param progress: latest progress if any.
        :param result:   latest result if any. 
        '''
        # If no progress indicator is provided use the last one that self.progress 
        # was used for. Because this is called by the running task instance it has
        # persistence during execution and self._progress is up to date.
        if not progress:
            progress = self._progress
            
        progress['description'] = f"Rolledback"
        meta = {'result': result, 'progress': progress}
        
        self.send_update(state="ROLLEDBACK", meta=meta)
        raise self.Exceptions.Rollback

    ############################################################################################    
    # END of TASK extensions
    #
    # BEGINNING of DJANGO INTERFACE
    ############################################################################################    

    # Include the Django subclass
    
    from .django import Django
    Django = Django

    class Exceptions:

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
            
               If the rask is wrapped in Django's @transaction.atomic 
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
