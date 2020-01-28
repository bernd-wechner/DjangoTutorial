#    https://ask.github.io/celery/tutorials/clickcounter.html
#    https://docs.celeryproject.org/projects/kombu/en/stable/userguide/examples.html
#    https://docs.celeryproject.org/projects/kombu/en/stable/userguide/simple.html
import re, json

from celery import current_app 
from celery.result import AsyncResult
from celery.exceptions import Ignore
from celery.utils.log import get_logger
from kombu import Connection
from kombu.simple import SimpleQueue
from django.http.response import HttpResponse

logger = get_logger(__name__)

class Interactive():
    def Task(task_function):
        '''
        A decorator to wrap a Celery task in a connection for interacting with
        the task. The task is provided with a SimpleQueue that it can read for
        messages from the Producer (that wants to communicate with this task)
         
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
                    name = Interactive.queue_name(my_id)
                    q = conn.SimpleQueue(name)
                    q.name = name
                    result = task_function(task, q)
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
    
    def Pulse(view):
        '''
        A decorator that wraps a django view function making it a useful
        pulse to check at intervals. This means essentially it will:
        
        Given a http request that contains a `task_id` will check status 
        on it returning a progress indication if it provides one, and if
        `cancel` is supplies will request the task abort.
            
        :param view: a Django view function that we will decorate
        '''
        def check_task_and_respond(request, task=None):
            '''
            Replaces a django view function that has the same signature.
            
            Requires either a task (to start) or the task_id (of an already 
            started task) in the GET request. Optionally the `cancel` keyword 
            in the GET request if a task_id is provided will ask that task to 
            abort.

            :param request: An HTTM request which optionally profives `task_id` and `abort`
            :param task: A string that identifies a registered celery task. 
            
            '''
            task_id = getattr(request,"GET", {}).get("task_id", None)
            abort = "cancel" in getattr(request,"GET", {})
        
            print(f'\ncheck_task_and_respond: task {task} with id {task_id}. abort = {abort}')
        
            if task_id:
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
                    print(f'Connecting to: {current_app.conf.broker_write_url}')
                    with Connection(current_app.conf.broker_write_url) as conn:
                        q = conn.SimpleQueue(Interactive.queue_name(task_id))
                        instruction = 'abort'
                        q.put(instruction)
                        print(f'Sent: {instruction} to {Interactive.queue_name(task_id)}')
                        q.close()

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
                    progress = {'percent':0, 'current':0, 'total':0, 'description': ""}
                    result = None
                elif (state == "PROGRESS"):
                    progress = r.info['progress']
                    result = None
                elif (state == "ABORTED"):
                    progress = r.info['progress']
                    result = r.info['result'] 
                elif (state == "SUCCESS"):
                    progress = {'percent':100, 'current':100, 'total':100, 'description': "Done!"}
                    result = r.result
                    
                print(f"RESULT: {result}")
            else:
                print()
                r = re.compile(fr"\.{task}$")
                Task = list(filter(r.search, list(current_app.tasks.keys())))
                assert len(Task) == 1, f"check_task_and_respond must have an unambiguous task to start. Provided: {task}, Found: {Task}."
                TASK = current_app.tasks[Task[0]]
                print("About to start debug task")
                r = TASK.delay()
                print("Kick started the debug task")
                task_id = r.task_id
                state = r.state # "STARTED"
                print(f"Got task id: {task_id}, state: {state}")
                progress = {'percent':0, 'current':0, 'total':0, 'description': ""}
                result = None
        
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
            elif (state == "ABORTED"):
                response = {'id': task_id, 'canceled': True, 'result': result}
            else:
                response = {'id': task_id, 'result': str(result), 'complete': True, 'success': True}
             
            return HttpResponse(json.dumps(response))
                
        return check_task_and_respond


    class Abort(Ignore):
        """A task can raise this to Abort execution."""

    @classmethod
    def queue_name(self, task_id):
        '''
        Builds and returns the name of a SimpleQueue that the Interactive task
        will open and listen on for messsages. Ensures writer and reader use 
        a queue of same name.
          
        :param task_id: The task id, ensuring every unique task has its own queue name
        '''
        return f'Interactive_{task_id}'
    
    @classmethod
    def check_abort(self, task): #, task, i, n, q, name):
        aborted = False
        
        assert task.interactive, "check_abort should be provided with a task instance that has a dictionary attyribute called `interactive`." 
        i = task.interactive.get('i', 0)
        n = task.interactive.get('n', 0)
        q = task.interactive.get('q', None)
        assert isinstance(q, SimpleQueue),  "A SimpleQueue must be provided via self.interactive.q to check for abort messages."
        name = q.name
        
        try:
            logger.info(f'KOMBU checking queue: {name}')
            message = q.get_nowait()
            instruction = message.payload
            logger.info(f'KOMBU Received: {instruction}')
            aborted = instruction == 'abort'
            logger.info(f'KOMBU Acknowledging instruction (emptying queue)')
            message.ack() # remove message from queue
            q.close()
            task.update_state(state="ABORTED", meta={'result': 'unfinished result', 'progress': {'percent':100*(i+1)/n, 'current':i+1, 'total': n, 'description': f"Task was aborted"}})
            raise self.Abort
        
        except q.Empty:
            logger.info(f'KOMBU Queue is empty.')
