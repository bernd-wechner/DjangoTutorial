# -*- coding: utf-8 -*-
"""Interactive Tasks.

Interactive tasks overview
==========================

A slightly tangled web of decorators and Task extensions that provides 
an broker derived queue for communication between a Client and a running
Task.

Classic application is to request a task to abort execution. But more general
instructions can easily be sent, as long as they have responses implemented 
in the task itself. 

Interactive is a class based on cleery.Task and best used in a task
decorator akin to:

    @app.task(bind=True, base=Interactive)
    def my_task_function(self):
        pass
    
Alwasy use bind=True, so that the task receives an instance of itself 
in the first argument (self).

base-Interactive, means the task will have the methods defined here-in.

 

"""
import re, json

from celery import current_app, Task 
from celery.result import AsyncResult
from celery.exceptions import Ignore
from celery.utils.log import get_logger
from kombu import Connection
from kombu.simple import SimpleQueue
from django.http.response import HttpResponse
from dal.forward import Self

logger = get_logger(__name__)

class Interactive(Task):
    '''
    Provides two decorators and some support functions that make interactive Celery tasks a breeze.
    
    The problem as at Celery 4.4, is that it offers us the means to start tasks and get feedback from them,
    but it provides us with no documented or easy means to send running tasks and further instructions.
    
    Celery rests upon, works with and provides two directions of communication between a client (who wants
    a task performed) and a worker (who will perform the task):
    
    client -> worker: broker. 
        The broker is by default an amqp provider like RabbitMQ or Redis.
        In fact as at Celery 4.4 only amqp brokers are supported:
            http://docs.celeryproject.org/en/latest/getting-started/brokers/
        
    worker -> client: backend
        Many more backends are supported than brokers:
            https://docs.celeryproject.org/en/stable/userguide/configuration.html#task-result-backend-settings
        The result backend was clearly intended for storing results upon completion but for long
        running tasks can also be used to send updates as the task progresses 
        (to drive progress bars for example).
            https://www.distributedpython.com/2018/09/28/celery-task-states/
            
    Workers though can be confgured and are by default configured to to support concurrency, that is
    to run more than one task at a time and does this by setting up an execution pool:
        https://www.distributedpython.com/2018/10/26/celery-execution-pool/
        
    The consequence is, not least int he default pooling strategy which sets up a worker (which the client 
    can send task requests to via the broker) and forks a number of child processes to actually execute the 
    tasks. It holds comms with its children close to its chest though and the broker only provides open means
    of communicating with the worker, not its children. 
    
    Common reason to send a request (via the broker) to a worker are:
    
    starting tasks:
        https://docs.celeryproject.org/en/latest/userguide/calling.html
    contolling the worker:
        https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
        https://medium.com/@djsmith42/creating-custom-celery-commands-1f0692d01918
    
    But there are at least two common use cases in which further instructions to a running task NOT the worker 
    (that started the task in its execution pool) are very useful:
    
    1) When desiring to terminate/abort a running task.
    
        Celery provides a revoke mechanism:
            in celery.app.control:     https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
            in celery.result:         https://docs.celeryproject.org/en/stable/reference/celery.result.html
        but revoke only cancels a task BEFORE it starts running, that is if it's still in a worker's queue scheduled to start.
        
        Celery also provides in its contrib module an AbortableTask variant:
            in celery.contrib.abortable: https://docs.celeryproject.org/en/latest/reference/celery.contrib.abortable.html
        but this is poorly implemented:
            a) it abuses the result backend to implement a client -> worker communciation (recall the result backend 
                is intended for worker -> client comms).
            b) as a consequence it only works for database backends which well, many of us aren't using and don't wish to use.
            
    2) To provide user input to a complicated task
    
        There is not currently any doumented means for doing this only expressed desires to see one:
            https://stackoverflow.com/questions/30481996/interact-with-celery-ongoing-task
            https://stackoverflow.com/questions/59796397/celery-interact-communicate-with-a-running-task
        
        A basic use case for this is in processing very complicated data submission with far reaching
        consequences and a good deal of data cleaning and validation overhead. In such a case we can 
        perform all validation and impact assesment inside of a database transaction. In the Django case:
            https://docs.djangoproject.com/en/3.0/topics/db/transactions/#controlling-transactions-explicitly
        which can then be committed or rolled back. 
        
        Having performed this validation and impact assessment we now seek input from the user to confirm 
        intent, that the transaction should be commited. The database transaction has to remain open while
        we send a response to the user declaring the impact, and requesting confirmation. If that can all 
        happen in one process all good and fine but in a web service this demands particular attention as
        the processes responding to HTTP requests often have a lifespan of one request (cannot hold a 
        database transaction open between two requests). Particularly in the Django case with the transaction 
        context manager there is no way within that context to round trip a conrimation request to the user.
        
        And so the a transaction manager is needed. A daemon process that can open and hold open a database
        connection and transaction and keep it open while the web server round trips a confirmation request.
        
        For this celery tasks are well suited as they run in workers that meet this criterion and provide
        all (most) of the comms needed to start the task and get results back (an impct assesment along with
        request to confirm). But there is no means by which the web sever can no send the users response back
        to the waiting task. This class provides a plug and play means to do so.
        
    These references were used in building this solution:
        https://ask.github.io/celery/tutorials/clickcounter.html
        https://docs.celeryproject.org/projects/kombu/en/stable/userguide/examples.html
        https://docs.celeryproject.org/projects/kombu/en/stable/userguide/simple.html
        
    Provided are two decorators:
    
        Task: which decorates a Celery task. It should be applied before app.task
                and a celery task can be made interactive as follows:
                
                @app.task(bind=True)
                @Interactive.Task
                def my_task(self, q):
                    ...
                
                app.task is the celery decorator that produces a Task class for Celery. 
                bind=true is a request on the decorator to provide the task instance 
                    as the first argument (self). We need this here, so that we can 
                    get the instantiated tasks task_id. It is used herein to ensure
                    messages are routed to the running task instance.
                    
                Interactive.Task provides a second arument which is an instance of 
                kombu.simple.SimpleQueue
                    https://docs.celeryproject.org/projects/kombu/en/stable/reference/kombu.simple.html
                
                This is needed by some of the functions provided herein.
        
        Pulse: which decorates a Django view function.
        
                Intended for use by a progress bar that sends back an AJAX request to function
                checking the pulse of the task. It checks the status of the task and returns a 
                response the progress bar can use, and if a cancel has been requested sends an
                abort message to the task.
                
                It is up the task to check the queue for such a message and terminate on 
                request. See check_abort below. 
    '''
    
    _progress = None
    
    class Abort(Ignore):
        """A task can raise this to Abort execution.
        
           It does nothing more than request Celery to ignore the 
           task from here on in (not send an update to state to
           to the client).
        """

    def queue_name(self, task_id):
        '''
        Builds and returns the name of a SimpleQueue that is used for communication
        between the Interactive task that is running and any client.
          
        :param task_id: The task id, ensuring every unique task has its own queue name
        '''
        return f'Interactive_{task_id}'
    
    def progress(self, percent=0, current=0, total=0, description=""):
        '''
        Trivially, builds a consistent JSONifieable data structure to describe task progress
        to a progress bar running in Javascript on a web browser.
        
        :param percent: 0 to 100 indication %age complete
        :param current: The last step completed (an integer) 
        :param total: The total number of steps before the task is complete
        :param description: A string describing the last step completed
        '''
        self._progress = {'percent':percent, 'current':current, 'total':total, 'description': description}
        
        return self._progress 
    
    def abort(self, task_id):
        '''
        Given an instance of celery.Task (self) and a specific task_id will send
        an abort request to the task of that id.
        
        To do so we open a kombu simple queue on the connection:
        
            current_app.conf.broker_write_url
        
        using the standard name for interactve task queues, uniquely
        attributed to the task of that id (built in self.queue_name).
     
        :param self: An instance of of this class
        :param task_id: The unique id of the task we are asking to abort
        '''
        print(f'Connecting to: {current_app.conf.broker_write_url}')
        with Connection(current_app.conf.broker_write_url) as conn:
            q = conn.SimpleQueue(self.queue_name(task_id))
            instruction = 'abort'
            q.put(instruction)
            print(f'Sent: {instruction} to {self.queue_name(task_id)}')
            q.close()
    
    def check_abort(self, progress=None): 
        '''
        Given an instance of celery.Task (self) and optionally a progress indicator,
        will checkif th task has an associates message queue in self.q, and if so,
        check if a message is there. If not will simply return None, else if the 
        message payload requests an abort it will request an abort, else it will 
        return the message payload as an instruction to the caller.
         
        :param self: An instance of Interactive
        :param progress: a progress indicator in form of self.progress()
        '''
        aborted = False
        if not progress:
            progress = self._progress
        
        q = getattr(self, 'q', None)
        assert isinstance(q, SimpleQueue),  "A SimpleQueue must be provided via self.q to check for abort messages."
        #name = q.name
        
        try:
            message = q.get_nowait()            # Raises an Empty exception if the Queue is empty
            
            instruction = message.payload       # get an instruction if available
            aborted = instruction == 'abort'    # Was the message an abort instruction?
            message.ack()                       # remove message from queue
            q.close()
            
            if aborted:
                progress['description'] = f"Aborted"
                self.update_state(state="ABORTED", meta={'result': 'unfinished result', 'progress': progress})
                raise Interactive.Abort
            else:
                return instruction
        
        except q.Empty:
            return None

def Connected(task_function):
    '''
    A decorator to wrap a Celery task in a connection for interacting with
    the task. The task is provided with a SimpleQueue that it can read for
    messages from the Producer (that wants to communicate with this task).
    
    It is stored in the attribute q and the code in the task function
    can access self.q if needed. But if the task is decorated with
    base=Interactive, then the task will have the function:
    
        self.check_abort()
        
    ehcih will check self.q for abort messages.
    
     
    :param task_function: A function that implements a Celery task
    '''
    def connected(task):
        '''
        A wrapper around a Celery task function that puts it inside
        a Connection hased on the broker read URL and providing a 
        SimpleQueue called q to the task on which it looks for 
        messages from someone (possibly a Pulse wrapped Django view) 
        
        :param task:
        '''
        my_id = task.request.id

        try:
            logger.info(f'Connecting to: {current_app.conf.broker_read_url}')
            with Connection(current_app.conf.broker_read_url) as conn:
                logger.info(f'Getting queue name')
                name = task.queue_name(my_id)
                logger.info(f'Got queue name: {name}')
                q = conn.SimpleQueue(name)
                q.name = name
                task.q = q
                result = task_function(task)
        except Ignore:
            # Trikcle the Celery Ignore exception upwards
            q.close()
            raise Ignore() 
        except Exception as e:
            logger.info(f'ERROR: {e}')
            task.update_state(state="FAILURE", meta={'result': 'result to date', 'reason': str(e)})
            q.close()

        return result
    
    # Preserver the name of the task function so we can look it up 
    connected.__name__ = task_function.__name__
    return connected

def DjangoPulseCheck(view_function):
    '''
    A decorator that wraps a django view function making it a useful
    pulse to check at intervals. This means essentially it will:
    
    Given a http request that contains a `task_id` will check status 
    on it returning a progress indication if it provides one, and if
    `cancel` is supplies will request the task abort.
        
    :param view_function: a Django view function that we will decorate
    '''
    def get_task(name):
        '''
        Given the name of task will return an instance of a task of that name
        provided it's been registered with Celery and the name identifies a 
        unique task.  
         
        :param name: The name fo the task desired.
        '''
        print()
        r = re.compile(fr"\.{name}$")
        task_fullnames = list(filter(r.search, list(current_app.tasks.keys())))
        assert len(task_fullnames) == 1, f"check_task_and_respond must have an unambiguous task to start. Provided: {name}, Found: {task_fullnames}."
        task = current_app.tasks[task_fullnames[0]]
        return task
    
    def check_task_and_respond(request, task_name):
        '''
        Replaces a django view function that has the same signature.
        
        Requires either a task (to start) or the task_id (of an already 
        started task) in the GET request. Optionally the `cancel` keyword 
        in the GET request if a task_id is provided will ask that task to 
        abort.

        :param request: An HTTM request which optionally profives `task_id` and `abort`
        :param task: A string that identifies a registered celery task. If provided
                     will check status of the task and respond as needed. If not 
                     provided will start the task. 
        
        '''
        task = get_task(task_name)
        task_id = getattr(request,"GET", {}).get("task_id", None)
        abort = "cancel" in getattr(request,"GET", {})
        
        # The name of a session key in which to store the count of steps the task reports.
        # Every time we receive a PROGRESS report it will contain a "total" number of steps.
        # it's nice to know that when the task is completed successfully, but Celery Tasks 
        # don't report progress metadata on completion. So we just keep the latest value in
        # session store so we have a sesnible figure to report when the task is completed. 
        total_store = f"task_{task_name}_{task_id}_total_steps"
    
        print(f'\ncheck_task_and_respond: task {task_name} with id {task_id}. abort = {abort}')
    
        if task:
            if not task_id:
                # Start the task
                print("About to start debug task")
                r = task.delay()
                print("Kick started the debug task")
                task_id = r.task_id
                state = r.state # "STARTED"
                print(f"Got task id: {task_id}, state: {state}")
                progress = task.progress()
                result = None
            else:
                # Check pulse of the task
                
                # r.state is a string and constrained to be:
                # "PENDING" - The task is waiting for execution.
                # "STARTED" - The task has been started.
                # "RETRY" - The task is to be retried, possibly because of failure.
                # "FAILURE" - The task raised an exception, or has exceeded the retry limit.  The result attribute then contains the exception raised by the task.
                # "SUCCESS" - The task executed successfully. The result attribute then contains the tasks return value.
                #
                # Custom states introduced here:
                # "PROGRESS" - The task is running
                # "ABORTED" - The task was cancelled 
                #
                # See: https://docs.celeryproject.org/en/latest/reference/celery.result.html
                #      https://www.distributedpython.com/2018/09/28/celery-task-states/      
        
                if abort:
                    task.abort(task_id)
    
                print(f"Fetching tasks result for task: {task_id}")
                r =  AsyncResult(task_id)
                print(f"Got result: {r.state}, {r.info}")
        
                state = r.state
                if (state == "PENDING"):
                    # We get this back if celery wasn't running nay workers for example
                    # So we need to sensibly provide feedback here if that happens. Easy
                    # to test as we just run tthe web site but not celery and start the task
                    # it will come back as pending. 
                    # TODO: implement default handling in progress.js 
                    print("Task is PENDING")
                    progress = task.progress()
                    result = None
                elif (state == "PROGRESS"):
                    progress = r.info['progress']
                    result = None
                elif (state == "ABORTED"):
                    progress = r.info['progress']
                    result = r.info['result'] 
                elif (state == "SUCCESS"):
                    steps = request.session.pop(total_store, 0)
                    progress = task.progress(100, steps, steps, "Done")
                    result = r.result
                    
                print(f"STATE: {state}, RESULT: {result}, PROGRESS: {progress}")
            
    
        # ProgressBar expects:
        #
        # id - if the task is running
        # progress - a dict containing percent, current, total and description
        # complete - a bool, true when done
        # success - a bool, false on error, else true
        # canceled - a bool, true when canceled  
        # result - the result of the tast if complete and success
        
        if (state == "PENDING" or state == "STARTED" or state == "PROGRESS"):        
            response = {'id': task_id, 'progress': progress}
            request.session[total_store] = progress.get("total", 0) 
        elif (state == "ABORTED"):
            response = {'id': task_id, 'canceled': True, 'result': result}
        else:
            response = {'id': task_id, 'result': str(result), 'complete': True, 'success': True}
         
        return HttpResponse(json.dumps(response))
            
    return check_task_and_respond

@DjangoPulseCheck
def DjangoCeleryInteractiveAJAX(request, task_name):
    pass

