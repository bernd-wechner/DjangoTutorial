# A simple Celery Turorial based on:
#  http://docs.celeryproject.org/en/latest/django/first-steps-with-django.html
from celery import Celery
import os
import time

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
    
@app.task(bind=True)
def debug_task2(self):
    for i in range(100):
        self.update_state(state="PROGRESS", meta={'progress': {'percent':i, 'current':i, 'total':100, 'description': f"Description number {i}"}})
        time.sleep(0.1)
    return 'hello world' 
    
