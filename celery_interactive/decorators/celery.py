from .. import log
from ..contexts.kombu import InteractiveConnection

from celery import Task as Celery_Task
from celery.exceptions import Ignore

import functools, uuid, os

class ArgumentError(Exception):
    pass

class ConnectedCall:
    '''
    Wraps the __call__ function of a task, making sure that when the function runs, it 
    has a connection and all the queues it needs for an Interactive life.
    '''
    def __init__(self, call_function):
        self.call_function = call_function
        #functools.update_wrapper(self, call_function)

    def __get__(self, obj, owner=None):
        '''
        This is some rather advanced profound Python trickery.
        
        Specifically, if a Class based decorator is to decorate a function or a 
        method freely, we need some way of handling the self argument that methods
        receive and functions do not. 
        
        It is documented here:
        
            https://stackoverflow.com/a/46361833/4002633
            https://docs.python.org/2/library/functools.html#functools.partial
            https://docs.python.org/2/library/functools.html#partial-objects

        If a function is decorated, this is never called.
        If a method is decorated it is called and provides the obj as the first argument to the method.
        '''
        return functools.partial(self, obj)        
    
    def __call__(self, *args, **kwargs):
        '''
        This is the entry point for Task.delay() or Task.apply_async(). We can (in fact MUST)
        call Task.run()  to run the actual Task decorated function. But we wrap that here 
        around the connection required to support Interactive communications.
        '''
        log.debug(f'Interactive task:')
        
        log.debug(f'\tGot {len(args)} args:')
        for v in args:
            log.debug(f'\t\t{v}')

        log.debug(f'\tGot {len(kwargs)} kwargs:')
        for k, v in kwargs.items():
            log.debug(f'\t\t{k}: {v}')

        task = args[0]
            
        if not isinstance(args[0], Celery_Task):
            raise ArgumentError("ConnectedCall decorator can only decorate a Celery Task method.")
                
        log.debug(f'Task: {task.fullname}')

        # First, take note of the queuue_name_root
        task.queue_name_root = kwargs.get("queue_name_root", task.default_queue_name_root())
        
        # A unique string to flag this result should be ignored.
        # It should simply have no chance of overlapping with an
        # actual task result. So we throw in a uuid for good measure.
        # We do this so that we can catch the Ignore exception, to
        # cleanly destroy the Queue this task was using before the 
        # final exit. 
        IGNORE_RESULT = f"__ignore_this_result__{uuid.uuid1()}"

        # Wrap the task in a connection (used for both reading from a instruction queue
        # and writing to an updates queue if configured to.
        with InteractiveConnection(task) as conn:  # @UnusedVariable
            try:
                # Before calling the decorated function report that we're STARTED and
                # provide the PID of the Worker Pool Process we're running in. 
                task.send_update(state="STARTED", meta={'pid': os.getpid()})
                
                # This is what super().__call does to wind up. So we do it it here too.
#                 _task_stack.push(task)
#                 task.push_request(args=args, kwargs=kwargs)

                result = self.call_function(*args, **kwargs)

                # Leave the exchange alone (it's reusable for other Interactive tasks)
            except Ignore:
                result = IGNORE_RESULT
            except Exception as e:
                log.error(f'TASK __CALL__ ERROR: {e}')
                import traceback
                log.error(traceback.format_exc())
                # TODO: result to date?
                task.send_update(state="FAILURE", meta={'result': 'result to date', 'reason': str(e)})
                result = None
#             finally:
                # This is what super().__call does to unwind. So we do it it too.
#                 task.pop_request()
#                 _task_stack.pop()
            
            # Wind down code can go here.
            # We explicitly do not delete the Queues that InteractiveConnection created.
            # We let the client do that when it's noticed the the task is done. 

        if result == IGNORE_RESULT:
            raise Ignore() 
            
        return result    
