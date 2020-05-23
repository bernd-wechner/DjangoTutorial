# -*- coding: utf-8 -*-
#
# TODO: Need to establish the life to states wit an rpc backend
#       There seems to be some overwriting, i.e. consuming of 
#       messages by someone otehr than us? Really. Confirm this 
#       hypthesis or not. Do we need a new queue as our ow task 
#       feedback queue? TO ensure no-one consumes messages on it?
#
#        https://docs.celeryproject.org/en/stable/userguide/tasks.html#states
#
#        "When a task moves into a new state the previous state 
#        is forgotten about, but some transitions can be deduced, 
#        (e.g., a task now in the FAILED state, is implied to have 
#        been in the STARTED state at some point)."
#
#        Ouch. Sugegsts a rewrite moving away fromuse fo states
#        to provide Ack messages for messages sent to the task.
#        How does Ack in AMQP work? Is supect it's jjust "message
#        consumed". We want ack for "did the thing you asked for".

"""Interactive Tasks.

An Interactive Overview
=======================

A Task extension that provides a means for communicating
with a running task.
 
A classic application is to request a task to abort execution. 
But more general instructions can easily be sent, as long as 
they have responses implemented in the task itself. if the
task does not check the provided Queue for instructions and
act on them, nothing will happen. 

Interactive is a class based on celery.Task and used in 
a task decorator akin to:

    @app.task(bind=True, base=Interactive)
    def my_task_function(self):
        pass
    
Always use bind=True, so that the task receives an instance of itself 
in the first argument (self).

"base=Interactive", means the task will have the methods defined 
here-in and very importantly the attribute:

    self.instruction_queue
        A Kombu Queue object to listen to for instructions

The task need never access this Queue itself of course, as there are 
two key helper methods also provided:

    self.check_for_instruction() 
        - which is non blocking and returns an instruction if present on 
          self.instruction_queue
           
    self.wait_for_instruction() 
        - which is blocking and doesn't return until an instruction is 
          present on self.instruction_queue

The outside world (Client) is given access to:

    task.instruct(instruction)
        - which will sent instruction to task (using self.instruction_queue)

There are a few extra methods supplied for standard use cases.

The Interactive Context
========================

To understand the internals of this package it's best to 
brooch some standard terms used in internal documentation.
There are no universal terms in this space and quite some 
variance among writers and documenter, but this package will
try to adhere to these distinctions:

User -      A human being interacting with a website through a ...
Browser -   A web browser, which can request pages in one of 
            two forms:
                - as a simple page request (which expects a web page back)
                - as an AJAX request (which will expect data back) 
Server -    A web server, which runs the software that responds to a 
            browser's requests.
View -      A Django concept, but fairly portable (to other Python web 
            frameworks like Flask or Bottle say). A Python function
            (or class) whose job it is to provide the response that the 
            Browser is asking for. The server runs this function, and
            chooses which function to run and with what arguments based
            of the Browser's request.
Task -      A Celery class, generally created by decorating a Python 
            function with @appplication.task (which produces the class,
            the decorated function becoming its run() method).
Worker -    A process that runs Tasks. A View will ask the worker to 
            run a task. 
Client -    Asked the Worker to run Task. The Client is in the context
            of a web site, and a Django website in particular likely to
            be the View above.
            
At this point, it's worth noting that:

** To finish its job, a Task may need input from a User **

This is the reason for the Interactive class of Celery tasks, to facilitate 
user input to the task when it's needed.

An idiosyncratic feature of Celery is that once a task is defined, it is 
visible in two very different contexts, that of the View and that of the 
Worker. It is important to the working herein it remember that these two
views are not of the same instance of the Task. Call them Task/Client and
Task/Worker. They are in fact separated by: 

a Broker -  A celery broker, being the means by which Task/Client can 
            communicate with a Worker and ask it to run Task/Worker.
            The Broker is used to send instructions to a Worker, and
            in an Interactive task for Task/Client to send instructions 
            to Task/Worker.
                         
a Backend - A Celery Results Backend, being the means by which Task/Worker
            can send updates back to Task/Client. The Backend is used to
            send updates from the running task (Task/Worker) to any
            watcher but notably the User, via View and Task/Client.
            
See: 

    https://docs.celeryproject.org/en/stable/getting-started/brokers/index.html
    https://docs.celeryproject.org/en/stable/userguide/configuration.html#task-result-backend-settings
            
Task/Client and Task/Worker are defined by the same Python code (Client and Worker
both load the same Python file in which you defined Task) and are the same 
class but not the same instance and may not even be running on the same machine let
alone in the same process. 

Task/Client instead calls Task.delay() or Task.apply_async() in the standard Celery 
paradigm, or Task.start() in the Interactive task paradigm. Task.start() is just a 
pseudonym for Task.delay() because ... well, it makes more sense to us.

Task/Worker is the one that actually runs the decorated function - by calling 
Task.__call__(). Task/Client does not run the decorated function (typically, 
it could of course, but that sort of defeats the purpose). 

Task/Worker has a unique id, a UUID, that identifies the running instance of the task. 
Task/Client does not, but ... Interactive tasks might ... The id of a running task is 
returned  when it's started (i.e. when Client asks Worker to run the task) and 
Interactive tools seek to keep that id at hand and ensure Task/Client has that id too.

The technical details are:

    Celery tasks are started with task.delay() or task.apply_async()
    See: https://docs.celeryproject.org/en/stable/userguide/calling.html
    Interactive tasks can be started with task.start() - a simple shortcut
    to task.delay() with a more useful name (we think).
    
    The id of the running task is one of the things returned when it 
    starts. It is present in task.request.id in Task/Worker, and in
    Task/Client it is present if you start the task using 
    task.django.start().

The whole point of Celery is that the work can be farmed out to Workers
anywhere (via the Broker) and results returned (via the Backend).

The basic Celery Task class provides tools for starting Tasks in Workers
and controlling them to a degree, and getting results back, but not for
communicating directly with a running Task. That is what Interactive
provides.

Starting Interactive Tasks
==========================

You can start an Interactive task using any of the following means:

task.delay()
    Standard Celery function. 
    See: https://docs.celeryproject.org/en/latest/userguide/calling.html

task.apply_async()
    Standard Celery function. 
    See: https://docs.celeryproject.org/en/latest/userguide/calling.html
    
task.start()
    A small wrapper around task.delay() that stores the task_id in
    task.request.id on the off chance that's useful to you. It's 
    available in result.task_id regardless. A mere convenience 
    wrapper with a nicer name than delay() (we think).
    
task.django.start(request, form)
    A Django convenience method which packs request and form into
    a way that can be serialized and sent to the Worker and hence
    your task (which must accept a kwarg named 'form'). The task
    can unpack this form using:
        self.django.unpack_form(kwargs['form'])
    and have a the same form that the Django view had (i.e.
    it can be validated and saved etc, all in the same way
    that it could be in the view).
    
    This is an extreme convenience for writing a transaction
    manager as a Celery Task. the task function can use a 
    Django transaction and keep that transaction open while 
    it solicts user input using methods available for that
    (see Communicating with Running Tasks below).
    
task.django.start_and_monitor(request, form)
    A django convenience method that calls task.django.start(request, form)
    then task.django.monitor() which returns a the monitor that
    task.django.templates.monitor defines. A default monitor template is 
    provided that displays a simple progress bar.

Communicating with Running Tasks
================================

A View can instantiate an Interactive task with:

    task = Interactive.Task(name, id)
    
and if it does then it will have convenience methods available
for sending instructions to the running Task/Worker.

The communication is sent using Celery's broker_write_url to
an exchange named celery.interactive using the task ID as a routing 
key.

The communication arrives in Task/Worker on self.instruction_queue.

Convenient methods available for completing communicating are as follows:

    Task/Client (i.e. View) -> Task/Worker:
    
        View:
            task.instruct(instruction)
            
        Worker:
            self.check_for_instruction()
            self.wait_for_instruction()


The running task can send messages back to the client on the
standard Backend provided by Celery:        

    Task/Worker -> Task/Client (i.e. View)
    
        Worker:
            self.update_state(state, meta)
            
        View:
            result = AsyncResult(task_id)
            
    See:
        https://www.distributedpython.com/2018/09/28/celery-task-states
        https://docs.celeryproject.org/en/stable/userguide/tasks.html#states
        https://docs.celeryproject.org/en/stable/reference/celery.states.html        
        https://docs.celeryproject.org/en/latest/reference/celery.app.task.html#celery.app.task.Task.update_state
        
        https://docs.celeryproject.org/en/stable/getting-started/first-steps-with-celery.html#keeping-results
        https://docs.celeryproject.org/en/stable/faq.html#results
"""

__author__ = 'Bernd Wechner'
__email__ = 'bwechner@yahoo.com'
__version__ = '0.1'

__all__ = ['config', 'base', 'celery', 'kombu', 'django', 'context', 'decorators']

import logging
 
log = logging.getLogger(__package__)


# The default Interactive class (based on kombu communications between Task/Client and Task/Worker)
# Specifically subclassed here so that a different communications class can be easily hooked in if
# ever desired. Given Kombu is the way Celery communications it's hard to imagine justifying a different
# communications tool than Kombu, but the option is there with all Kombu specific.
#
# Kombu   
 
from celery_interactive.tasks import kombu 
class Interactive(kombu.InteractiveKombu):
    pass


