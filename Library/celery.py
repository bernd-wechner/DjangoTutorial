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
from celery_interactive import Interactive

@app.task(bind=True, base=Interactive)
@Interactive.Connected
def debug_task2(self):
    n = 10
    for i in range(n):
        logger.info(f'XDEBUG debug_task2, Working... {i+1} of {n}')
        
        #progress = {'percent':100*(i+1)/n, 'current':i+1, 'total': n, 'description': f"Description number {i+1}"}
        progress = self.progress(100*(i+1)/n, i+1, n, f"Description number {i+1}")

        self.update_state(state="PROGRESS", meta={'progress': progress})

        # Will raise an Abort error after setting state to 'ABORTED'
        instruction = self.check_for_abort()

        if instruction:
            logger.info(f'XDEBUG debug_task2, Instructed to: "{instruction}"')
        
        time.sleep(1) 
    
#     logger.info(f'XDEBUG debug_task2, Sleeping for an hour')
#     time.sleep(60*60)
    # This implicitly sets state to SUCCESS
    return 'final result'


