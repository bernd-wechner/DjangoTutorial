'''
Task defintions. Currently Two tiers of task defintion for Interactive.

base.py:
    InteractiveBase defines the basic structure of an InteractiveTask

kombu.py:
    InteractiveKombu adds the communications tools it needs based on kombu libraries

celery.py:
     Task is just a function that returns a Celery Task instance from the Celery Task register.
'''