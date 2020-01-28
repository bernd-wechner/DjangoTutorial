# A simple Celery Turorial based on:
#  http://docs.celeryproject.org/en/latest/django/first-steps-with-django.html
from celery import Celery, Task
from celery.utils.log import get_logger
from celery.contrib.abortable import AbortableTask
import os
import time
from multiprocessing.shared_memory import ShareableList
from builtins import all

logger = get_logger(__name__)

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'DjangoTutorial.settings')

app = Celery('Library', broker='pyamqp://CoGs:ManyTeeth@localhost/CoGs', backend='rpc://')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    time.sleep(1)
    self.update_state(state="PROGRESS", meta={'progress': 50})
    time.sleep(1)
    self.update_state(state="PROGRESS", meta={'progress': 90})
    time.sleep(1)
    return 'hello world' 
    
# @app.task(bind=True, base=AbortableTask)
# def debug_task2(self):
#     try:
#         for i in range(100):
#             if self.is_aborted():
#                 return "Aborted"
#             else:
#                 self.update_state(state="PROGRESS", meta={'progress': {'percent':i, 'current':i, 'total':100, 'description': f"Description number {i}"}})
#                 time.sleep(0.1)
#                 
#         return 'hello world' 
#     except Exception as e:
#         return str(e)
#         
# from celery.worker.control import Panel

# ShareableList is quite special in that it's of fixed size, and doesn't support
# length changes like an ordinary Python List. To wit if we want to provide a 
# queue of requests in such a list we need to extend ShareableList a little.
class ShareableQueue(ShareableList):
    '''
    A ShareableList that uses the first item as an item count, and provides methods
    to add and remove items from the list. 
    '''
    
    def __init__(self, size=None, default=None, name=None):
        '''
        Initializes a ShareableList (super) with a list that is of the specified size
        with one extra entry for the count at the start. The values are all initialized
        with None.
        
        :param size: The number of items the Queue will support
        :param name: The name of the Queue
        '''
        if size:
            queue = [0] + [default] * size
            super().__init__(queue, name=name)
        else:
            super().__init__(name=name)
            
    def __repr__(self):
        if self[0] > 0:
            return str(list(self)[1:self[0]+1])
        else:
            return str([])

    def push(self, item):
        '''
        Add an item to the end of the Queue.
        :param item: The item to add to the end of the queue
        '''
        count = self[0]
        count += 1
        assert count < len(self), f"Memory Overrun Error: Shareable Queue of {len(self)-1} items was full when a push was attempted."

        self[0] = count
        self[count] = item
    
    def insert(self, item):
        '''
        Add an item to the start of the Queue.
        :param item: The item to add to the start of the queue
        '''
        count = self[0]
        count += 1
        assert count < len(self), f"Memory Overrun Error: Shareable Queue of {len(self)-1} items was full when an insert was attempted."
        if count > 1:
            for i in range(count, 2, -1):
                self[i] = self[i-1]

        self[0] = count
        self[1] = item

    def pop(self):
        '''
        Pull an item off the end of the Queue and return it.
        '''
        count = self[0]
        if count < 0:
            return None
        else:
            count -= 1
            self[0] = count
            return self[count+1]

    def pull(self, item, all=False): # @ReservedAssignment
        '''
        Pull an the provided item off the the Queue if it exist and return True if doing so, else False.
        
        :param item: The item to pull off
        :param all: If True all items with that value are pulled off, else just the first one
        '''
        count = self[0]
        
        found = 0
        for i in range(1,count+1):
            if self[i] == item:
                found = i
                break

        if found:
            if all:
                for i in range(count, 0, -1):
                    if self[i] == item:
                        count -= 1
                        for j in range(i, count+1):
                            self[j] = self[j+1]
            else:
                count -= 1
                if count:
                    for j in range(i, count+1):
                        self[j] = self[j+1]

            self[0] = count
            return True
        else:
            return False

# Expermient with kombu a la: 
#    https://ask.github.io/celery/tutorials/clickcounter.html
#    https://docs.celeryproject.org/projects/kombu/en/stable/userguide/examples.html
#    https://docs.celeryproject.org/projects/kombu/en/stable/userguide/simple.html
from kombu import Connection, Producer
from celery.exceptions import Ignore

def instruction_queue_name(task_id):
    return f'instructions_for_{task_id}'

def InteractiveTask(task):
    '''
    A decorator to wrap a Celery task in a connection for interacting with
    the task. The task is provided with a SimpleQueue that it can read for
    messages from the Producer (that wants to communicate with this task)
     
    :param task: A celery task
    '''
    def connected(self):
        pass

# Tasks run in a child process of the worker process
@app.task(bind=True)
def debug_task2(self):
    my_id = self.request.id
   
    try:
        logger.info(f'Connecting to: {app.conf.broker_read_url}')
        with Connection(app.conf.broker_read_url) as conn:
            q = conn.SimpleQueue(instruction_queue_name(my_id))
            loop = 10
            for i in range(loop):
                logger.info(f'XDEBUG debug_task2, Working... {i+1} of {loop}')
                aborted = False
    
                try:
                    logger.info(f'KOMBU checking queue: {instruction_queue_name(my_id)}')
                    message = q.get_nowait()
                    instruction = message.payload
                    logger.info(f'KOMBU Received: {instruction}')
                    aborted = instruction == 'abort'
                    logger.info(f'KOMBU Acknowledging instruction (emptying queue)')
                    message.ack() # remove message from queue
                except q.Empty:
                    logger.info(f'KOMBU Queue is empty.')
                     
                if aborted:
                    self.update_state(state="ABORTED", meta={'result': 'unfinished result', 'progress': {'percent':100*(i+1)/loop, 'current':i+1, 'total': loop, 'description': f"Description number {i+1}"}})
                    q.close()
                    # Leave the state intact (do not overwrite it with SUCCESS or FAILURE)
                    # See: https://www.distributedpython.com/2018/09/28/celery-task-states/
                    raise Ignore() 
                else:
                    self.update_state(state="PROGRESS", meta={'progress': {'percent':100*(i+1)/loop, 'current':i+1, 'total': loop, 'description': f"Description number {i+1}"}})
                    time.sleep(1)    
            
            q.close()
            # This implicitly sets state to SUCCESS
            return 'final result'
                
    except Ignore:
        # Tirkcle the Clery Ignore exception upwards
        raise Ignore() 

    except Exception as e:
        logger.info(f'ERROR: {e}')
        self.update_state(state="FAILURE", meta={'result': 'result to date', 'reason': str(e)})
        
        q.close()
        # TODO: Work out what's going on here with return. Does it implcity set a ew state of SUCCESS? I think so!
        return 'result to date'

