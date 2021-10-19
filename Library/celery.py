# A simple Celery Turorial based on:
#  http://docs.celeryproject.org/en/latest/django/first-steps-with-django.html
from celery import Celery
#from celery.utils.log import get_logger
import os
import time
import json

from celery_interactive import Interactive
from celery.exceptions import Ignore
from django.db import transaction

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

import logging
log = logging.getLogger('celery_interactive')

#logger = get_logger(__name__)

# @app.task(bind=True)
# def debug_task(self):
#     time.sleep(1)
#     self.update_state(state="PROGRESS", meta={'progress': 50})
#     time.sleep(1)
#     self.update_state(state="PROGRESS", meta={'progress': 90})
#     time.sleep(1)
#     return 'hello world' 
#     
# # Tasks run in a child process of the worker process
# # A task being bound means the first argument to the task will always be the task 
# # instance (self), just like Python bound methods:
# 
# @app.task(base=Interactive, bind=True)
# @transaction.atomic
# def debug_interactive_task(self, *args, **kwargs):
#     log.info(f'XDEBUG debug_interactive_task, starting with {args} and {kwargs}')
#     test_confirm = kwargs.get("test_confirm", args[0] if args else False)
#     
#     n = 10
#     for i in range(n):
#         log.info(f'XDEBUG debug_interactive_task, Working... {i+1} of {n}')
#         
#         progress = self.progress(100*(i+1)/n, i+1, n, f"Description number {i+1}")
# 
#         self.update_state(state="PROGRESS", meta={'progress': progress})
# 
#         # Will raise an Abort error after setting state to 'ABORTED'
#         instruction = self.check_for_abort()
# 
#         if instruction:
#             log.info(f'XDEBUG debug_interactive_task, Instructed to: "{instruction}"')
#         
#         time.sleep(1) 
#     
#     if test_confirm:
#         log.info(f'XDEBUG debug_interactive_task, Waiting for Confirmation ...')
# 
#         # Confirmation request
#         instruction = self.wait_for_instruction(self.ASK_CONFIRM, "This is an interim result")
#         
#         if instruction == self.COMMIT:
#             log.info(f'XDEBUG debug_interactive_task, COMMITING.')
#             return f'final result: COMMIT'
#         elif instruction == self.ROLLBACK:
#             log.info(f'XDEBUG debug_interactive_task, ROLLING BACK.')
#             raise self.Exceptions.Rollback
#         else:
#             log.info(f'XDEBUG debug_interactive_task, ROLLING BACK (implicitly).')
#             raise self.Exceptions.Rollback
#     else:
#         log.info(f'XDEBUG debug_interactive_task, Waiting for Instruction ...')
# 
#         # Generic instruction
#         instruction = self.wait_for_instruction("Please instruct (button above).")
#         
#     
# #     log.info(f'XDEBUG debug_interactive_task, Sleeping for an hour')
# #     time.sleep(60*60)
#     # This implicitly sets state to SUCCESS
#     return f'final result: {instruction}'

def configure_add_book(task, *args, **kwargs):  # PyDev @UnusedVariable
    task.initial_monitor_title = f"First pass test for {task.shortname}"
    #task.django.templates.monitor = "monitor.html"
    #task.django.templates.confirm = "confirm.html"
    task.django.templates.aborted = None #"aborted.html"
    #task.django.templates.committed = "committed.html"
    #task.django.templates.rolledback = "rolledback.html"

@app.task(bind=True, base=Interactive, conf=configure_add_book)
@transaction.atomic
def add_book(self, *args, **kwargs):
    '''
    A transaction manager for adding books. Testing Celery Interactive.
    '''
    self.send_update(state="STARTING")

    step_sleep = 0.5
    
    packed_form = kwargs.get("form", {})
    
    log.info(f'Starting with {args} and {kwargs}')
    log.info(f'Packed Form: {json.dumps(packed_form, indent=4)}')
    
    form = self.django.unpack_form(packed_form)
    
    if form.is_valid():
        log.info(f'FORM is VALID')
    else:
        log.info(f'FORM is INVALID')
        for i, e in enumerate(form.errors):
            log.info(f'ERROR: {i} {e}')
        self.bail_with_fail(reason="Form Errors", form_errors=form.errors) # Does not return (raises an exception)
  
    n = 10

    self.progress.configure(n, 3)
    
    for i in range(n):
        log.info(f'Working... {i+1} of {n}')
         
        self.progress.send_update(i+1, 1, f"First pass, step {i+1}", f"Interim result at step {i+1} of stage 1")
 
        time.sleep(step_sleep) 
     
    log.info(f'Done First Pass: Waiting for Approval to Continue to Second Pass (continue or abort) ...')
 
    # Confirmation request
    # Will either return (self.CONTINUE) or raise a self.ABORT exception
    self.progress.send_update(n, 1, "Done stage 1", "This is an interim result after stage 1")
    
    self.wait_for_continue_or_abort(f"Second pass test for {self.shortname}")
    
    for i in range(n):
        log.info(f'Working... {i+1} of {n}')
        
        self.progress.send_update(i+1, 2, f"Second pass, step {i+1}", f"Interim result at step {i+1} of stage 2")

        time.sleep(step_sleep) 

    log.info(f'Done Second Pass: Waiting for Confirmation (commit or rollback), before Third Pass  ...')

    self.progress.send_update(n, 2, "Done stage 2", "This is an interim result after stage 2")

    # Confirmation request
    # Will either return (self.COMMIT) or raise a self.Exceptions.Rollback exception
    try:
        self.wait_for_commit_or_rollback(f"Third (and Final) pass test for {self.shortname}")
    except self.Exceptions.Rollback:
        pass

    for i in range(n):
        log.info(f'Working... {i+1} of {n}')
        
        self.progress.send_update(i+1, 3, f"Third pass, step {i+1}", f"Interim result at step {i+1} of stage 3")

        time.sleep(step_sleep) 

    log.info(f'Done Third (Final) Pass: Waiting for Confirmation  (commit or rollback) ... beforce completion.')

    # Confirmation request
    # Will either return (self.COMMIT) or raise a self.ROLLBACK exception
    self.wait_for_commit_or_rollback()

    log.info(f'Task Committed. Completing...')

    return f'final result: COMMITTED'


