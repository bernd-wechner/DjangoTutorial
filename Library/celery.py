# A simple Celery Turorial based on:
#  http://docs.celeryproject.org/en/latest/django/first-steps-with-django.html
from celery import Celery, Task
from celery.utils.log import get_logger
from celery.contrib.abortable import AbortableTask
import os
import time
from multiprocessing.shared_memory import ShareableList
from builtins import all
from abc import abstractstaticmethod

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
    
# Tasks run in a child process of the worker process
# A task being bound means the first argument to the task will always be the task 
# instance (self), just like Python bound methods:
from .interactive import Interactive

@app.task(bind=True)
@Interactive.Task
def debug_task2(self, q):
    n = 10
    logger.info(f'XDEBUG self dir {dir(self)}')
    for i in range(n):
        logger.info(f'XDEBUG debug_task2, Working... {i+1} of {n}')
        
        # Attach some attributes to the task instance that help with the check.
        self.interactive = {'i':i, 'n':n, 'q':q}
        # Willr aise an Abort error after setting state to 'ABORTED'
        Interactive.check_abort(self)
        
        self.update_state(state="PROGRESS", meta={'progress': {'percent':100*(i+1)/n, 'current':i+1, 'total': n, 'description': f"Description number {i+1}"}})
        time.sleep(1)    
    
    # This implicitly sets state to SUCCESS
    return 'final result'


