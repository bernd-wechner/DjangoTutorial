# -*- coding: utf-8 -*-
"""Interactive Tasks.

Interactive tasks overview
==========================

A slightly tangled web of decorators, a Task extension and 
support functions that provides a celery broker-derived queue 
for communication between a client (typically the Boss) and a 
running Task.

Classic application is to request a task to abort execution. 
But more general instructions can easily be sent, as long as 
they have responses implemented in the task itself. 

Interactive is a class based on cleery.Task and best used in a task
decorator akin to:

    @app.task(bind=True, base=Interactive)
    def my_task_function(self):
        pass
    
Always use bind=True, so that the task receives an instance of itself 
in the first argument (self).

"base=Interactive", means the task will have the methods defined 
here-in. 

It is also necessary to decorate the task with Interactive.Connected,
which must be done before it's decorated with app.task, a la:

    @app.task(bind=True, base=Interactive)
    @Interactive.Connected
    def my_task_function(self):
        pass
        
What this does is wrap the task in the context of a connection and 
message queue that it can listen to for instructions from a client. 

This is provided in form of an attribute self.instruction_queue 
which contains a Kombu Queue. It need never access this Queue itself 
of course, as there are two key helper methods also provided:

    self.check_for_instruction() 
        - which is non blocking and returns an instruction if present on 
          the queue
    self.wait_for_instruction() 
        - which is blocking and doesn't return until an instruction is 
          present on the queue.

The outside world is given access to"

    task.instruct(instruction)
        - which will sent instruction to task.

There are a few extra methods supplied for standard
use cases.


To understand the internals of this package it's best to 
brooch some standard terms used in internal documentation.
There are no universale terms in this space and quite some 
variance among writers and documenter, but this package will
try to adhere to these distinctions:

User -      A human being interacting with a website through a
Browser -   A web browser, which can requets pages in one of 
            two forms:
                - as a simple page request (which expects a web page back)
                - as an AJAX request (which will expect data back) 
Server -    A web server, which runs the software that responds to a 
            browser's requests.
View -      A Django concept, but fairly portable, a python function
            (or class) whose job it is to provide the repsonse the 
            Browser is asking for. The server runs this function, and
            chooses which function to run and with what arguments based
            of the Browser's request.
Task -      A Celery class, generally created by decorating a Python 
            function with @appplication.task (which produces the class,
            the decorated function becoming its run() method)        
Worker -    A process that uns Tasks. A View will ask the worker to 
            run a task. 
Client -    Asked the Worker to run Task. The Client is in the context
            of a web site, and a Django website in particular likely to
            be the View above.
            
At this point, it's worth noting that:

** A Task may need input from a User to finish its job **

This is the reason for the Interactive class of Celery tasks, to facilitate 
user input to the task when it's needed.

An idiosynchartic feature of Celery is that once a task is defined, it is 
visible in two very different contexts, that of the View and that of the 
Worker. It is important to the working herein it remember that these two
views are not of the same instance of the Task. Call them Task/Client and
Task/Worker. They are in fact separated by: 

a Broker -  A celery broker, being the means by which Task/Client can 
            communicate with a Worker and ask it to run Task/Worker.
            The Broker is used to send instructions to a Worker, and
            in an Interactive task for Task/Client to send instructions 
            to the Task/Worker.
                         
a Backend - A Celery Results Backend, being the means by which Task/Worker
            can send updates back to Task/Client. The Backend is used to
            send updates from the running task (Task/Worker) to any
            watcher but notably the User, via View and Task/Client.
            
See: 

    https://docs.celeryproject.org/en/stable/getting-started/brokers/index.html
    https://docs.celeryproject.org/en/stable/userguide/configuration.html#task-result-backend-settings
            
Task/Client and Task/Worker are defined by the same python code (Client and Worker
both load the same Python file in which you defined Task) and are the same 
class but not the same instance and may not even be running on the same machine.

Task/Worker is the one that actually runs the decorated function. Task/Client does
not run the decorated function (typically, it could of course, but that sort of
defeats the purpose). Task/Worker has a unique id a UUID that identifies the 
running instance of the task. Task/Client does not, but ... Interactive tasks
might ... The id of a running task is returned when it's started (i.e. when 
Client ask Worker to run the task) and Interactive tools seek to keep that id at 
hand and ensure Task/Client has that id too.

The technical details are:

    Celery tasks are started with task.delay() or task.apply_async()
    See: https://docs.celeryproject.org/en/stable/userguide/calling.html
    
    The id of the running task is one of the things returned when it 
    starts. In fact Interactive provides some start methods that take
    care of all this.

The whole point of Celery is that the work can be farmed out to Workers
anywere (via the Broker) and results returned (via the Backend).

The basic Celery Task class provides tools for starting Tasks in Workers
and contorlling them to a degree, and getting rsults back, but not for
commmunicating directly with a running Task. That is what Interactive
provides.

COMMUNICATING WITH RUNNING TASKS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If a task is decorates with @Interactive.Connected the the running
task (Task/Worker) is equiped with self.instruction_queue which is 
is based on Celery's broker_read_url and the view can instantiate 
a task with Interactive.Task(name, id) will have a method 
task.instruct(instruction) which sends a message using Celery's 
broker_write_url. 

The methods thus available for communicating are as follows:

Provided here:
    Task/Client (i.e. View) -> Task/Worker:
    
        View:
            task.instruct(instruction)
            
        Worker:
            self.check_for_instruction()
            self.wait_for_instruction()

Provided by Celery:        
    Task/Worker -> Task/Client (i.e. View)
    
        Worker:
            self.update_state(state, meta)
            
        View:
            AsyncResult(task_id)
            
    See:
        https://www.distributedpython.com/2018/09/28/celery-task-states
        https://docs.celeryproject.org/en/stable/userguide/tasks.html#states
        https://docs.celeryproject.org/en/stable/reference/celery.states.html        
        https://docs.celeryproject.org/en/latest/reference/celery.app.task.html#celery.app.task.Task.update_state
        
        https://docs.celeryproject.org/en/stable/getting-started/first-steps-with-celery.html#keeping-results
        https://docs.celeryproject.org/en/stable/faq.html#results
"""
import re, json, time, uuid, functools 

# Celery and Kombu imports
from celery import current_app, Task as Celery_Task
from celery.result import AsyncResult
from celery.exceptions import Ignore
from celery.signals import task_prerun, before_task_publish
from celery.utils.log import get_task_logger

from kombu import Connection, Queue, Exchange, Producer, Consumer
from kombu.simple import SimpleQueue

from amqp.exceptions import NotFound
 
# Django Imports
from django.http.response import HttpResponse
from django.http.request import HttpRequest
from django.shortcuts import redirect
from django.template import loader
from django.apps import apps
from django.forms.models import modelform_factory

# For methods see:
#     https://docs.python.org/3.8/library/logging.html
logger = get_task_logger(__name__)

# A Debug flag to turn on debuuging output
debug = False

def is_ajax(request):
    pass

def Django_PulseCheck(view_function):
    '''
    A decorator that wraps a django View function making it a useful pulse checker 
    at intervals. This means essentially it will request an update from a running
    task and retrun a JSON dict conveying latest status to the Browser,

    This wrapper 

    It will also respond sensibly to other states that the task sends 
    back including:
    
        PENDING - which happens if the task hasn't started yet, e.g. no worker is available
                  and, when a task goes into the WAITING state, and then calls a wait_for...()
                  method Celery overwrites WAITING with PENDING. WAITING being our custom state
                  PENDING being a Celery built in State that it clearly assocated with a blocking
                  read on a queue.   
  
        WAITING - a custom state here issued by any of the Interactive.wait_for_...() methods

        STILL_WAITING - a pseudo custom state, used internally only when we notices that the last state
                        was WAITING and are now PENDING, we switch to STILL_WAITING and continue.  That 
                        is because Celery overwrites our WAITING state with a PENDING state when it
                        stops to wait on a queue read. We'd like to know the difference between ordinary
                        PENDING (task not started yet) and STILL_WAITING.

        ABORTED - a custom state here issued by Interactive.abort()

        ROLLEDBACK - a custom state here issued by Interactive.rollback()
        
        PROGRESS - a custom state here issued by the task itself (must be coded)
                   to do so it simply call:
                       self.update_state(state="PROGRESS", 
                            meta={'progress': self.progress(percent, step, total, description)})
                            
        SUCCESS    - a standard Celery state set when the task function returns a result.
        
    :param view_function: a Django view function that we will decorate
    '''
    
    def check_task_and_respond(*args, **kwargs):
        '''
        Replaces a django view function that has the same signature.
        
        Requires a task_name and optionally a task_id (of an already 
        started task) in request. Optionally the `cancel` keyword in the request 
        if a task_id is provided will ask that task to abort and the 'instruction'
        keyword in the request can request that we instruct the task as suggested. 

        Warning: if not task_id is provided in request this will start a task. This
        can lead to multiple task initiations if a Pulse Checking view fails to 
        provide a task_id!

        :param request:   An HTTP request which optionally provides `task_id` and `abort`
                          This must be the first or second arg. 
                          
        :param task_name: A string that identifies a registered celery task. Accepted
                          only as a kwarg or in the request (i.e. not as an arg)

        :param task_id: Optionally a task ID as a UUID or sting that identifies a 
                        running celery task. Accepted only as a kwarg or in the 
                        request (i.e. not as an arg)
        '''
       
        # The first argument might be "self" from a class method. Interactive.Django 
        # certaintly provides a method decorated with this decorator and so arg[0]
        # will be an instance of Interactive.Django. But it doesn't really matter if
        # it's Interactive.Django or some other class method we're decorating, class
        # methods all pass the class instant ins arg[0], canonically called 'self'.
        if debug:
            print(f"\nCHECKING PULSE: {args=} {kwargs=}")

        if len(args) > 1 and isinstance(args[1], HttpRequest):
            args = list(args)
            self = args.pop(0)
        
        # A Django view is passed an HttpRequest a the first arg
        request = args[0]
        
        # A Django view revieves named URL parameters as kwargs
        #
        # We expect the task name and id to be passed as kwargs 
        # to the view in this manner. But if they are not we're 
        # happy to accept them in the GET paramters or even the 
        # POST parameters.
        #
        # task_id is optional, if not provided we'll start a task
        # running, but if it's provided we'll check the pulse on the
        # rtunning task of that ID. The id might arrrive as a UUID and
        #  we need it as a string not a UUI object.
        task_name = kwargs.pop("task_name", args[0] if len(args)>0 else Interactive.get_request_param(request, "task_name"))
        task_id = str(kwargs.pop("task_id", args[1] if len(args)>1 else Interactive.get_request_param(request, "task_id")))
        
        if not task_name:
            raise Exception("NO task_name provided")

        # Note up front whether we're wanting to deliver JSON
        # (i.e. have been called via AJAX) or render a page 
        # (i.e. got here througha button rpess on another page)
        #
        # Django's is_ajax() method just implements and old 
        # on-line defacto standard checking for an HTTP header
        # called X-Requested-With and that it has the value 
        # XMLHttpRequest, so any Javascript fecthing this that
        # uses plain Javascript wants to consider this and set 
        # the header.
        deliver_json = request.is_ajax()

        # Get any instructions that may have been submitted that we support    
        instruction = Interactive.get_request_param(request, "instruction")
        notify = Interactive.get_request_param(request, "notify")
        
        please_confirm = not Interactive.get_request_param(request, "confirm") is None
        please_continue = not Interactive.get_request_param(request, "continue") is None
        please_abort = not Interactive.get_request_param(request, "abort") is None
        please_cancel = not Interactive.get_request_param(request, "cancel") is None
        please_commit = not Interactive.get_request_param(request, "commit") is None
        please_rollback = not Interactive.get_request_param(request, "rollback") is None
        
        # Get an instance of the task wtih this name and id
        task = Interactive.Task(task_name, task_id)

        # Start the JSON reponse dict we'll send to the caller on an ordinary pulse check.
        # We always want to keep the browser appraised of the task id so it can pass that
        # back in on susequent pulses and know this response is still for the same running
        # task. 
        response = {'name':      task.name, 
                    'shortname': task.shortname, 
                    'id':        task.request.id, 
                    'result':    None}
        
        if task:
            if not task_id: # then start a task and note its id and state
                r = task.start(*args, **kwargs)
                task.request.id = r.task_id
                task.state = r.state # should be "STARTED"
                response['id'] = r.task_id
                if debug:
                    print(f"Started {task.shortname} -> {task.name} with id: {task.request.id}, state: {task.state}")

            # The name of a session keys in which to store data we wish to persist
            store_last_progress = f"task_{task.name}_{task.request.id}_last_progress"
            store_is_waiting_on = f"task_{task.name}_{task.request.id}_is_waiting_on"
            store_notify_with = f"task_{task.name}_{task.request.id}_notify_with"

            ####################################################################
            # First up if we've been asked to notify the user then cut to the 
            # chase and do that
            if notify:
                response = request.session.get(store_notify_with, response)

                state = response.get('state', '')
                if  state == "ABORTED":
                    template = task.django.templates.aborted
                elif state == "ROLLEDBACK":
                    template = task.django.templates.rolledback

                template = loader.get_template(template)
                return HttpResponse(template.render(response, request))
            
            ####################################################################
            # Now we know have a running task with an id and state and can 
            # send it any instructions we've been asked to and check it's 
            # state and act on that.

            # If we received an (arbitrary) instruction in the request parameters, 
            # send it to the running task.
            if instruction:
                task.instruct(instruction)
                response["instructed"] = instruction
                if debug:
                    print(f"Instructed task to abort: {task.name}-{task.request.id}")

            # if we receiveed a request to abort or cancel the task in the request parameters
            # send it on to the running task.
            if please_abort or please_cancel:
                task.please_abort()
                if debug:
                    print(f"Asked task to abort: {task.name}-{task.request.id}")
            
            # task.state is a string and constrained to be:
            # "PENDING" - The task is waiting for execution.
            # "STARTED" - The task has been started.
            # "RETRY" - The task is to be retried, possibly because of failure.
            # "FAILURE" - The task raised an exception, or has exceeded the retry limit.  The result attribute then contains the exception raised by the task.
            # "SUCCESS" - The task executed successfully. The result attribute then contains the tasks return value.
            #
            # Custom states introduced here:
            # "PROGRESS" - The task is running
            # "ABORTED" - The task was cancelled 
            # "WAITING" - The task is waiting for an instruction 
            # "QUEUE"   - The task is informing us of a Queue Name to use to send it instructions
            #
            # See: https://docs.celeryproject.org/en/latest/reference/celery.result.html
            #      https://www.distributedpython.com/2018/09/28/celery-task-states/      

            # Now fetch the current status of the task 
            r = AsyncResult(task_id)
            # NOTE: r.info is an alias for r.result - they are the same thing
            #       https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.AsyncResult
            #       https://docs.celeryproject.org/en/stable/_modules/celery/result.html#AsyncResult
            # Here we tend tend to use r.result when the task is complete and r.info for status updates.
            # They both contain the meta argument to task.update_state() 
            if debug:
                print(f"Got result: state: {r.state}, result/info: {r.result}, Is waiting on: {request.session.get(store_is_waiting_on, 'Not WAITING')}")

            # Associate the observed state with the task at hand and add it to the response
            task.state = r.state
            response['state'] = task.state 
            
            # Then we want to check if the task is waiting on us for something. If it
            # asked us to wait some time prior, we put the request into 
            #     request.session[store_is_waiting_on]
            # and if we're WAITING again let downstream know by updating state to 
            # STILL_WAITING, so it can tell the difference between first WAIT and
            # subsequent ones.
            waiting_info = request.session.get(store_is_waiting_on, None)

            # First we check if we're STILL_WAITING or WAITING on something new
            if task.state == "WAITING" and r.info == waiting_info:
                task.state = "STILL_WAITING"
                response['state'] = task.state
                if debug: 
                    print(f"WAITING on same thing so STILL_WAITING on: {waiting_info}")
            
            # Else, if we have waiting_info then we know we were WAITING on something
            # but maybe we are stull waiting or maybe not. 
            elif waiting_info:
                # Sometimes Celery overwrites the state of a WAITING task with PENDING
                # Not sure exactly why or when, but when it does, we know we're 
                # STILL_WAITING because we have waiting_info in the session_store
                if task.state == "PENDING":
                    task.state = "STILL_WAITING"
                    response['state'] = task.state 
                    r.info = waiting_info
                    if debug: 
                        print(f"PENDING while WAITING so STILL_WAITING on: {waiting_info}")
                
                # If task.state == "WAITING" it's because the new info does not match
                # the old info, implying we're now waiting on something else. The task 
                # has changed it's mind about what it's waiting for. That being the 
                # case we maintain the "WAITING" state unaltered.
                
                # Finally if the state is something other than WAITING or PENDING
                # then we're not waiting any more and must remove the waiting_info
                # from the session store. 
                else:
                    request.session.pop(store_is_waiting_on)
                    if debug:
                        print(f"NO LONGER WAITING on: {waiting_info}, now {task.state} with {r.info}")

            ###################################################################
            # Now we can check the state the task is in an act accordingly.
            
            # The classic update is PROGRESS which provides informatiom that will 
            # help the client update  a progress bar. 
            #
            # If we were WAITING we not longer are so pop the record of that out 
            # of the session store (i.e. erase it there). 
            #
            # if this is not a PROGRESS update, but some other status update, 
            # grab the last known  progress for other views
            if task.state == "PROGRESS":
                request.session.pop(store_is_waiting_on, None)
                progress = r.info.get('progress', task.progress())
                request.session[store_last_progress] = progress
                response["progress"] = progress
                if debug:
                    print(f"PROGRESS: {progress}")

            # STARTED is the state of the task just after we started it.
            # Given we get an state update immediately after it may still
            # be STARTED or have updated state by then. 
            if task.state == "STARTED":
                print(f"Task is STARTED")
                response["progress"] = task.progress()
                if debug:
                    print(f"STARTED: {progress}")

            # PENDING generally means that celery can't reach a worker to run
            # the task. Classically because no Celery workers are running. It
            # Can crop up other times too, a bit mysteriously. Notably when
            # a task is WAITING sometimes the worker will overwrite that with
            # PENDING for some reason. That is handled above and switched to
            # STILL_WAIITNG if we got PENDING after we started WAITING so won't
            # land here.
            if task.state == "PENDING":
                if r.info:
                    response["progress"] = r.info.get('progress', request.session.get(store_last_progress, task.progress()))
                else:
                    response["progress"] = request.session.get(store_last_progress, task.progress())
                if debug:
                    print(f'PENDING: {response["progress"]}')
                
            # If the task received an istruction to ABORT or ROLLBACk it will update
            # state to tell us it did that! In which case again it is no longer WAITING
            # so if it was so we clear that from session store, and can return and a page
            # defined by a configured template. We can do this because we're no longer
            # monitoring.                 
            elif task.state in ["ABORTED", "ROLLEDBACK"]:
                winfo = request.session.pop(store_is_waiting_on, {})
                response['canceled'] = True
                response["progress"] = r.info.get('progress', request.session.get(store_last_progress, task.progress()))
                response['interim_result'] = str(r.info.get('interim_result', r.info.get('result', '')))
                response['result'] = str(r.info.get('result', ''))
                
                if task.state == "ABORTED":
                    template = task.django.templates.aborted
                    # Aborting the task precludes any continuation or need to monitor such.  
                    continue_monitoring = False
                elif task.state == "ROLLEDBACK":
                    template = task.django.templates.rolledback
                    # Rolling back a task might abort the task (for a task managing a single 
                    # database transaction) or it might want to continue. Only the task knows
                    # and it informs us when it goes into the wait whether it wants to coninue
                    # monitoring or not and that is stored session so we don't forget.
                    continue_monitoring = winfo.get('continue_monitoring', False)
                 
                if debug:
                    print(f'{task.state}: Template: {template}, continue_monitoring: {continue_monitoring}')

                if template and not continue_monitoring:
                    response['interim_result'] = str(r.info.get('interim_result', r.info.get('result', '')))
                    response['result'] =  str(r.info.get('result', ''))

                    if not deliver_json:
                        # Display a page defined by a configured template,
                        if debug:
                            print(f"Return page: {template}, with context: {response}")
                            
                        template = loader.get_template(template)
                        # Provide the response as context to the template
                        return HttpResponse(template.render(response, request))
                    elif template:
                        request.session[store_notify_with] = response
                        response["notify"] = True
                        if debug:
                            print(f"Request that the AJAX caller, redirect back here with ?notify so we can render a page.")
                    else:
                        if debug:
                            print(f"Return to AJAX caller the data: {response}")
                
            # If the task throws an exception then Celery will return a FAILURE status with
            # the excpeion explained in the result. It's no longer WAITING if it was, and 
            # can fall back to a JSON response to the pulse checker (monitor) that got us here.
            elif task.state == "FAILURE":
                request.session.pop(store_is_waiting_on, None)
                response["failed"] = True
                response["result"] = str(r.result)  # Contains the error
                response["progress"] = r.info.get('progress', request.session.get(store_last_progress, task.progress()))
                if debug:
                    print(f"FAILURE: Return to AJAX caller the data: {response}")
                # TODO: Support a template option here
                
            # When the task is complete it will return the "SUCCESS" status and
            # deliver a result. As ever if it was WAITING it no longer is so
            # clear our record of that.
            elif task.state == "SUCCESS":
                request.session.pop(store_is_waiting_on, None)
                total_steps = request.session.pop(store_last_progress, {}).get("total", 0)
                 
                response["progress"] = task.progress(100, total_steps, total_steps, "Done")
                response["result"]   = str(r.result) # Contains the returned value of the task function
                
                if debug:
                    print(f"SUCCESS: Return to AJAX caller the result: {response}")
                
            # Finally, if the task is WAITING on a response from us, we better deliver one,
            # that means a round trip to the web browser. We expect a prompt which is either
            # ASK_CONTINUE or ASK_COMMIT, which will determine the template we want to render.
            elif task.state in ["WAITING", "STILL_WAITING"]:
                # Persist the waiting_info in the session store
                # But only on WAITING, not STILL WAITING
                # i.e. on the first notification we're WAITING 
                if task.state == "WAITING":
                    request.session[store_is_waiting_on] = r.info
                    if debug:
                        print(f"WAITING (for first time) on: {r.info}")
                    
                response["progress"]       = request.session.get(store_last_progress, None)
                response["prompt"]         = r.info.get('prompt', '')
                response["interim_result"] = str(r.info.get('interim_result', r.info.get('result', '')))
                response["result"]         = str(r.info.get('result', ''))
                response["waiting"]        = True

                continue_monitoring = r.info.get('continue_monitoring', False)
                
                if response["prompt"] == task.ASK_CONTINUE:
                    if debug:
                        print(f"ASK_CONTINUE from task: {task.name}-{task.request.id}")

                    # When the task is WAITING state with ASK_CONTINUE 
                    # prompt we either need to present the user with the 
                    # question, or we've received a response to that 
                    # question from the user. 
                    #
                    # Presenting the user with a question lands here 
                    # twice the sequence is as follows:
                    #
                    # 1. ASK_CONTINUE - returns JSON with "confirm" in response
                    #    Javascript should then reload this same URL but with "confirm" in the request
                    #
                    # 2. ASK_CONTINUE and please_confirm
                    #    Javascript either redirected here for a page render or 
                    #    came back here with an AJAX call for a JSON response. 
                    #
                    #    In the former case we render task.django.templates.confirm
                    #    In the latter case the Javascript is asking to for enough
                    #    information to render its own conrimation form to solicit 
                    #    user intput. So we send it that.
                    #
                    # 3. positive_URL or negative_URL is requested
                    #    Only positive_URL is handled here, negative_URL
                    #    request thetask to abort and is handled above.
                    #    positive_URL is a request to continue from the user.
                    #    If it comes as a page request we  reload the 
                    #    configured monitor, else if it arrives as an AJAX
                    #    request we simply return a PROGRESS report. 
                    
                    if please_confirm:
                        # We've been asked to render to the user a confirmation form. We'll offer them
                        # two choices, the positive (cntinue) and the negative (abort)
                        #
                        # If we've been asked via a page request we'll return the page defined by
                        #     task.django.templates.confirm
                        # but if we've been asked via an AJAX request we'll just return the 
                        # data needed in the AJAS response so the requester can build their own 
                        # form to solict user input. 
                        #
                        # The idea is we come back here by requesting postive_URL (in which case
                        # we have please_continue) of negative_URL (in which case the abort is 
                        # already handled above).  
                        response.update(
                                {                                        
                                  'positive_lbl':   "Continue", 
                                  'negative_lbl':   "Abort",
                                  'positive_URL':   request.build_absolute_uri('?') + f"?continue&task_id={task.request.id}", 
                                  'negative_URL':   request.build_absolute_uri('?') + f"?abort&task_id={task.request.id}",
                                })
                            
                        if not deliver_json:
                            if debug:
                                print(f"please_confirm (continue or abort) with {task.django.templates.confirm}: {response}")
                            template = loader.get_template(task.django.templates.confirm)
                            return HttpResponse(template.render(response, request))
                        else:
                            if debug:
                                print(f"please_confirm (continue or abort) returns to AJAX caller this data: {response}")

                    elif please_continue:
                        # Implies postive_URL was requested. 
                        # So we let the task know that it should continue
                        # a page or a just
                        task.please_continue()

                        # If it's a page request, we render the monitor configured in
                        #      task.django.templates.monitor
                        if not deliver_json:
                            if debug:
                                print(f"please_continue with new monitor, titled '{continue_monitoring}': {response}")
                            task.monitor_title = continue_monitoring
                            return task.django.monitor(response, request)
                        else:
                            if debug:
                                print(f"please_continue with existing monitor: {response}")
                    
                    else: 
                        # Fall through to standard JSON pulse delivery with a request to bounce back here with
                        # ?confirm in the request so we can render a page. The monitor has the option of course
                        # of bouncing back with an AJAX request or a page request (dealt with above)
                        response["confirm"] = True
                        if debug:
                            print(f"Request that the AJAX caller, redirect back here with ?confirm so we can render a page.")
                    
                elif response["prompt"] == task.ASK_COMMIT:
                    if debug:
                        print(f"ASK_COMMIT from task: {task.name}-{task.request.id}")

                    # Very simmilar to ASK_CONTINUE above, the key differences being:
                    #
                    # We're aksing for commit or rollback instruction.
                    # We have the option to continue monitoriing even after 
                    # a rollback, in support of tasks implementing mutliple 
                    # transactions - this is an extension on the abort option
                    # under ASK_CONTINUE which asks the task to end compleley.
                    #
                    # When the task is WAITING state with ASK_COMMIT 
                    # prompt we either need to present the user with the 
                    # question, or we've received a response to that 
                    # question from the user. 
                    #
                    # Presenting the user with a question lands here 
                    # twice the sequence is as follows:
                    #
                    # 1. ASK_COMMIT - returns JSON with "confirm" in response
                    #    Javascript should then reload this same URL but with "confirm" in the request
                    #
                    # 2. ASK_COMMIT and please_confirm
                    #    Javascript either redirected here for a page render or 
                    #    came back here with an AJAX call for a JSON response. 
                    #
                    #    In the former case we render task.django.templates.confirm
                    #    In the latter case the Javascript is asking to for enough
                    #    information to render its own conrimation form to solicit 
                    #    user intput. So we send it that.
                    #
                    # 3. positive_URL or negative_URL is requested
                    #    positive_URL is a request to commit a transaction,
                    #    and optionally to continue.
                    #    negative_URL is a request to roll back a transaction
                    #    and optionally to continue.
                    #
                    #    If it comes as a page request we reload the 
                    #    configured monitor, else if it arrives as an AJAX
                    #    request we simply return a PROGRESS report. 
                     
                    if please_confirm:
                        print(f"PLEASE CONFIRM: {r.info}.")
                        print(f"Loading template view from {task.django.templates.confirm}.")
                        # No JSON response required. Instead we want to 
                        # render a confirmation view
                        response.update(
                                {                                        
                                  'positive_lbl':   "Commit", 
                                  'negative_lbl':   "Discard",
                                  'positive_URL':   request.build_absolute_uri('?') + f"?commit&task_id={task.request.id}", 
                                  'negative_URL':   request.build_absolute_uri('?') + f"?rollback&task_id={task.request.id}",
                                })
                            
                        if not deliver_json:
                            if debug:
                                print(f"please_confirm (commit or rollback) with {task.django.templates.confirm}: {response}")
                            template = loader.get_template(task.django.templates.confirm)
                            return HttpResponse(template.render(response, request))
                        else:
                            if debug:
                                print(f"please_confirm (commit or rollback) returns to AJAX caller this data: {response}")
                    
                    elif please_commit or please_rollback:
                        if please_commit:
                            # Implies postive_URL was requested. 
                            # So we let the task know that it should commit the transaction
                            task.please_commit()
                            template = task.django.templates.committed
                            if debug:
                                print(f"please_commit:")

                        elif please_rollback:
                            # Implies negative_URL was requested. 
                            # So we let the task know that it should roll back the transaction
                            task.please_rollback()
                            template = task.django.templates.rolledback
                            if debug:
                                print(f"please_rollback:")

                        # If it's a page request, we render the monitor configured in
                        #      task.django.templates.monitor
                        #           or
                        #      task.django.templates.committed
                        if not deliver_json:
                            if continue_monitoring:
                                task.monitor_title = continue_monitoring
                                if debug:
                                    print(f"\tcontinue with new monitor, titled '{task.monitor_title}': {response}")
                                return task.django.monitor(response, request)
                            else:
                                if debug:
                                    print(f"\tDeliver landing page: '{template}': {response}")
                                template = loader.get_template(template)
                                return HttpResponse(template.render(response, request))
                        # else fall through to the standard JSON delivery.
                        
                    else:
                        # Fall through to standard JSON pulse delivery with a request to bounce back here with
                        # ?confirm in the request so we can render a page. The monitor has the option of course
                        # of bouncing back with an AJAX request or a page request (dealt with above)
                        response["confirm"] = True
                        if debug:
                            print(f"Request that the AJAX caller, redirect back here with ?confirm so we can render a page.")
    
        #######################################################################
        # RETURN a JSON dict capable of feeding a progress bar 
        #######################################################################

        # Call the decorated view function 
        contrib = view_function(request, task_name)

        # PulseChecker in pulse_check.js checks:
        #
        # id         - if the task is running
        # progress   - a dict containing percent, current, total and description
        # complete   - a bool, true when done
        # success    - a bool, false on error, else true
        # canceled   - a bool, true when canceled
        # waiting    - a bool, true when the task is waiting  
        # intructed  - notifying it that an instruction was sent to the task
        # result     - the result of the task if complete and success
        # notify     - a request to redirect here with ?notify
        # confirm    - a request to redirect here with ?confirm
        #
        # We reserve all the keys in the response. 
        reserved = ["id", "progress", "complete", "success", "canceled", 
                    "waiting", "instructed", "result", "notify", "confirm"]
         
        # If the decorated function returns a dict complement our 
        # response with what it provides, but don't let it clobber 
        # (override) existing values unless it specifically asks to.
        if isinstance(contrib, dict):
            if debug:
                print(f"Decorated task contributes: {contrib}")
                
            clobber = contrib.pop("__overwrite__", False)
                 
            for k,v in contrib.items():
                if clobber or not k in reserved:
                    response[k] = v
        elif contrib:
            raise Exception("Configuration error: Django Pulse Check decorator wraps view function that does not return a dict.")
          
        # Return the dictionary as JSON string (intended to be used 
        # in Javascript at the client side, to drive a progress bar
        # and/or other feedback) 
        if debug:
            print(f"RESPONSE to Ajax called: {response}")
            
        return HttpResponse(json.dumps(response))
            
    return check_task_and_respond

@Django_PulseCheck
def Django_Pulse(request, *args, **kwargs):
    '''
    A Django view, provided, pre-decorated that does only the standard 
    pulse checks. If they are all that is needed then this is fine. It
    is provided in Interactive.Django.Progress as well for convenience.
     
    If you want to add things to the response, just decorate your own
    view with Django_PulseCheck.
     
    :param request:   A Django request object, as Django provides to view functinos
    :param task_name: The name of a task, that Django provides as a kwarg from the 
                      urlpatterns. That is, you need to invoke this view with 
                      something like:
                       
                      urlpatterns += [
                          path('progress/<task_name>', Interactive.Django.Progress)
                      ]
                       
                      so that DJango provides task_name as a kwarg to this view function.
    '''
    pass

def Django_Instruct(request, task_name):
    '''
    A basic Django view function that will take a task_name, and provided 
    a task_id and instruction are provided in the request will send that 
    instruction to the identified task.
     
    :param request:   A Django request object, as Django provides to view functinos
    :param task_name: The name of a task, that Django provides as a kwarg from the 
                      urlpatterns. That is, you need to invoke this view with 
                      something like:
                      
                      urlpatterns += [
                          path('instruct/<task_name>', Interactive.Django.Instruct)
                      ]
                      
                      so that DJango provides task_name as a kwarg to this view function.  
    '''
    task = Interactive.Task(task_name)
    task_id = Interactive.get_request_param(request, "task_id")
    instruction = Interactive.get_request_param(request, "instruction")

    if task and not task_id is None and not instruction is None:
        task.instruct(task_id, instruction)
    
class InteractiveBase(Celery_Task):
    '''
    Augments the Celery.Task with some aditional functions, and a few decorators
    that make Interactive Celery tasks a breeze.  
    
    The problem as at Celery 4.4, is that it offers us the means to start tasks 
    and get feedback from them, but it provides us with no documented or easy means 
    to send running tasks any further instructions.
    
    Celery rests upon, works with and provides two directions of communication between 
    a Client (who wants the task performed) and a Worker (who will perform the task):
    
    Client -> Worker: broker. 
        The broker is by default an amqp provider like RabbitMQ or Redis.
        In fact as at Celery 4.4 only amqp brokers are supported:
            http://docs.celeryproject.org/en/latest/getting-started/brokers/
        
    Worker -> Client: backend
        Many more backends are supported than brokers:
            https://docs.celeryproject.org/en/stable/userguide/configuration.html#task-result-backend-settings
        The backend is intended for storing results upon completion but for long
        running tasks can also be used to send updates as the task progresses 
        (to drive progress bars for example).
            https://www.distributedpython.com/2018/09/28/celery-task-states/
            
    Workers though can be confgured and are by default configured to to support concurrency, 
    that is to run more than one task at a time and does this by setting up an execution pool:
        https://www.distributedpython.com/2018/10/26/celery-execution-pool/
        
    The consequence is, not least in the default pooling strategy which sets up a worker 
    (which client can send task requests to via the broker) and forks a number of child 
    processes to actually execute the tasks. It holds comms with its children close to its 
    chest though and the broker only provides open means of communicating with the worker, 
    not its children. 
    
    Common reason to send a request (via the broker) to a worker are:
    
    starting tasks:
        https://docs.celeryproject.org/en/latest/userguide/calling.html
    contolling the worker:
        https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
        https://medium.com/@djsmith42/creating-custom-celery-commands-1f0692d01918
    
    But there are at least two common use cases in which further instructions to a 
    running task NOT the worker (that started the task in its execution pool) are 
    very useful:
    
    1) When desiring to terminate/abort a running task.
    
        Celery provides a revoke mechanism:
            in celery.app.control:    https://docs.celeryproject.org/en/stable/reference/celery.app.control.html
            in celery.result:         https://docs.celeryproject.org/en/stable/reference/celery.result.html
        but revoke only cancels a task BEFORE it starts running, 
        that is if it's still in a worker's queue scheduled to start.
        So, not very useful for long running tasks we'd like to terminate.
        
        Celery also provides in its contrib module an AbortableTask variant:
            in celery.contrib.abortable: https://docs.celeryproject.org/en/latest/reference/celery.contrib.abortable.html
        but this is poorly implemented:
            a) it abuses the result backend to implement a client -> worker 
               communciation (recall the result backend is intended for 
               worker -> client comms).
            b) as a consequence it only works for database backends (which permit this
               abuse) that many of us aren't using and don't wish to use (in fact rpc 
               backends come highly recommended - and utilize the same amqp provider that
               the broker uses).
            
    2) To provide user input to a complicated task
    
        There is no currently doumented means for doing this, only expressed desires 
        to see one:
            https://stackoverflow.com/questions/30481996/interact-with-celery-ongoing-task
            https://stackoverflow.com/questions/59796397/celery-interact-communicate-with-a-running-task
        
        A basic use case for this is in processing very complicated data submission with far 
        reaching consequences and a good deal of data cleaning and validation overhead. In 
        such a case we can perform all validation and impact assesment inside of a database 
        transaction. In the Django case:
            https://docs.djangoproject.com/en/3.0/topics/db/transactions/#controlling-transactions-explicitly
        which can then be committed or rolled back. 
        
        Having performed this validation and impact assessment we now seek input from the 
        user to confirm intent, that the transaction should be commited. The database 
        transaction has to remain open while we send a response to the user declaring 
        the impact, and requesting confirmation. If that can all happen in one process 
        all good and fine but in a web service this demands particular attention as
        the processes responding to HTTP requests often have a lifespan of one request 
        (cannot hold a database transaction open between two requests). Particularly in 
        the Django case with the transaction context manager there is no way within that 
        context to round trip a confirmation request to the user.
        
        The most common work around cited, is to save the original request, permorm whatever 
        impact assesments we need, consult the user, and then if requested to commit, load
        the saved request and commit it. That is in fact a very sensible and sound solution
        provided the impact assessment is simple and clean. If the impact assessment is best
        performed by saving Django objects (easily the case for very coimplicated far reaching 
        submission, say changing a setting that impacks a great many django objects/database 
        tuples). In such cases it can be much easier to assess impact inside of a transaction,
        perform all the required changes to the database, summarise impacts, request permission
        to commit, and then commit if granted. If we implement such a transaction inside a
        Django view it must be committed or rolled back before it's done.
        
        And so a transaction manager is needed. A daemon process that can open and hold 
        open a database connection and transaction and keep it open while the web server 
        round trips a confirmation request. So that we can decide on committing or rolling
        back with one or more round trips to the user (i.e. across multiple Views). 
        
        For this celery tasks are well suited as they run in workers that meet this 
        criterion and provide all of the comms needed to start the task and 
        get results back (an impact assesment along with request to confirm). But 
        there is no means by which the web server can send the users response back
        to the waiting task. This class provides a plug and play means to do so.
        
    These references were used in building this solution:
        https://ask.github.io/celery/tutorials/clickcounter.html
        https://docs.celeryproject.org/projects/kombu/en/stable/userguide/examples.html
        
    Provided are two decorators:
    
        Connected: 
                which decorates a Celery task. It should be applied before app.task
                and a celery task can be made interactive as follows:
                
                @app.task(bind=True, base=Interactive)
                @Interactive.Connected
                def my_task(self):
                   ...
                
                app.task is the celery decorator that produces a Task class for 
                Celery. bind=true is a request on the decorator to provide the 
                task instance as the first argument (self). We need this here, 
                so that we can get the instantiated task's task_id. It is used 
                herein to ensure task can create a Queue that canbe reached with 
                the tasks ID as a routing_key, which the client can write to. 
                The client knows the task ID from result returned by Task.start() 
                - which starts the task.
                
                Connected, adds an attribute named instruction_queue to the Task, 
                which is an instance of kombu.Queue/

                This is needed by some of the functions provided herein but can 
                also be used by the code in the task through self.instruction_queue
        
        Django.PulseCheck: 
                which decorates a Django view function.
                
                Intended for use by a progress bar that sends back an AJAX request 
                to the function thus decorated, checking the pulse of the task. It 
                checks the status of the task and returns a response the progress 
                bar can use, and if a cancel has been requested sends an abort message 
                to the task and if an instruction was submitted (in the request), sends 
                the instruction to the task.
                
                It is up the task to check the queue for cancel requests or instuctions
                and act on them. If the task ignores them they will have no effect.
                The task can call methods like self.check_for_abort(), or 
                self.check_for_instruction() and act on the results - these check
                self.q quietly and act (raise Abort) or report as required.

    and provides two Django views:
    
        django.pulse:
                An empty Django.PulseCheck decorated view. Saves you writing 
                a view if all youw nat is the standard features. If you want 
                to add any information to repsonse, you can write your own 
                view and decorate it with Django.PulseCheck.
                
        django.instruct:
                A simple view which sends an instruction to a running task.
                Entirely up to the task if it checks for and acts on it it.
                All this does is put the instruction into the queue for that
                task. Expects the instruction in the request. 
                
    '''

    ############################################################################################    
    # START of TASK extensions
    ############################################################################################    
    
    _progress = None

    # An alias for delay (because delay sucks)
    start = Celery_Task.delay
    
    # Use indexed queue names? If True adds a small overhead in finding a free 
    # indexed queue name on the broker when the task runs. This can make things
    # a little easier to minitor if needed than using that UUID task_id in the
    # queue name. But the UUID is available without consultingt he broker so 
    # marginally more efficient to use.
    #
    # Can of course be configured in any function decorated with  @Interactive.Config
    use_indexed_queue_names = True
    
    # The name of the instuction Queue the task will listen to
    instruction_queue_name = None
    
    # Define some standard strings used for prompts (Task -> CLient) and
    # instructions  (Client -> Task).
    #
    # These need not be user friendly and the idea is that they don't 
    # clash with any strings a user might want to use.
    ASK_CONTINUE = "__continue?__"
    CONTINUE = "__continue__"
    ABORT = "__abort__"

    ASK_COMMIT = "__commit?__"
    COMMIT = "__commit__"
    ROLLBACK = "__rollback__"
    
    def __init__(self):
        self.django = self.Django(self)
    
    @property
    def shortname(self):
        return self.name.split('.')[-1]
        
    def default_queue_name(self, task_id=None):
        '''
        Builds a default queue name that an Interactive task can use.
        
        If use_indexed_queue_names is True, we'll use indexed queue names
        and not this default. That name must be communicated to a client 
        with a "QUEUE" state update. If it is false we can use this queue
        name, and don't need to communicate it to a client as it can be
        constructed from the task's name and running task_id alone.
        
        To wit, a client can use this queue name until a QUEUE state 
        update arrives but it must accept that the queue of this name 
        may not exist. 
        '''
        # Can be called with no arguments from the celery worker side
        # but from the client side need to provide a task_id either as
        # and argument or plugged into self.request.id.
        if not task_id:
            task_id = self.request.id
            
        return f'{self.name}.{task_id}'
        
    def queue_exists(self, name):
        '''
        Return True if a Queue of that name exists on the Broker 
        and False if not. 
        
        :param name: The name of the Queue to test
        '''
        with Connection(current_app.conf.broker_url) as conn:
            try:
                # Not especially well documented, but"
                #
                # https://docs.celeryproject.org/projects/kombu/en/stable/reference/kombu.transport.virtual.html#channel
                # https://docs.celeryproject.org/projects/kombu/en/stable/_modules/amqp/channel.html
                #
                # the queue_declare method of channel can be used to test
                # if a queue exists using passive=True which will not make any 
                # changes to the server (broker). From the channel docs above:   
                #
                #     passive: boolean
                # 
                #     do not create queue
                # 
                #     If set, the server will not create the queue.  The
                #     client can use this to check whether a queue exists
                #     without modifying the server state.
                # 
                #     RULE:
                # 
                #         If set, and the queue does not already exist, the
                #         server MUST respond with a reply code 404 (not
                #         found) and raise a channel exception.                
                #
                # The exception raised is amqp.exceptions.NotFound
                conn.default_channel.queue_declare(name, passive=True)
                exists = True
            except NotFound:
                exists = False
            except Exception as E:
                exists = False
                logger.error(f"Unexpected exception from Channel.queue_declare(): {E}")
                    
        return exists    
    
    def indexed_queue_name(self, i):
        return f'{self.name}.{i}'
    
    def first_free_indexed_queue_name(self):
        '''
        Returns the first free indexed queue name available on the Broker.
        '''
        i = 1
        while self.queue_exists(self.indexed_queue_name(i)):
            i += 1
        
        return self.indexed_queue_name(i)
            
    def progress(self, percent=0, current=0, total=0, description=""):
        '''
        Trivially, builds a consistent JSONifieable data structure to describe task progress
        to a progress bar running in Javascript on a web browser.
        
        Called with no arguments returns a 0,0,0 progress indicator, useful at
        outset or to simply create the dictionary.
        
        :param percent: 0 to 100 indication %age complete
        :param current: The last step completed (an integer) 
        :param total: The total number of steps before the task is complete
        :param description: A string describing the last step completed
        '''
        self._progress = {'percent':percent, 'current':current, 'total':total, 'description': description}
        
        return self._progress 
    
    def please_continue(self):
        '''
        Sends an instruction to continue to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to ask a running task instance to continue, in response 
        to its request for such an instruction. It's entirely up to the task whether it's 
        even checking for let along acting on such instructions. 
        '''
        self.instruct(self.CONTINUE)

    def please_abort(self):
        '''
        Sends an instruction to abort to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to abort a running task instance. It's entirely up 
        to the task whether it's even checking for let along acting on such 
        instructions. The client  can't force it to stop with this method, in fact 
        there is not clean easy way in Celery to do that (though this:
        
            app.control.revoke(task_id, terminate=True, signal='SIGKILL')
            
        will kill the worker process).  But Celery docs note:
        
            The terminate option is a last resort for administrators when a 
            task is stuck. It’s not for terminating the task, it’s for 
            terminating the process that’s executing the task, and that 
            process may have already started processing another task at 
            the point when the signal is sent, so for this reason you must 
            never call this programmatically.
            
        So not want you want for clean exit. For a clean exit, alas, all
        we can do is ask the taks nicely to abort. This method does that.
        '''
        self.instruct(self.ABORT)

    def please_commit(self):
        '''
        Sends an instruction to commit to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.
        
        Used by a client wanting to ask a running task instance to commit results 
        to a database, in response to its request for such an instruction. It's 
        entirely up to the task whether it's  even checking for let along acting 
        on such instructions.
        '''
        self.instruct(self.COMMIT)

    def please_rollback(self):
        '''
        Sends an instruction to rollback to the task. Requires that the task have
        request.id to identify the running instance of the task which is used as
        a routing_key.

        Used by a client wanting to ask a running task instance to rollback an
        open databse transaction, in response to its request for such an instruction. 
        It's  entirely up to the task whether it's  even checking for let along acting 
        on such instructions.
        '''
        self.instruct(self.ROLLBACK)

    def send_progress(self, progress, check_for_abort=True):
        '''
        Sends a simple progress report back to the Client.
        
        :param progress:
        '''
        self.update_state(state="PROGRESS", meta={'progress': progress})
        if check_for_abort:
            self.check_for_abort()

    def wait_for_continue_or_abort(self, interim_result=None, progress=None, continue_monitoring=None):
        '''
        Waits for an instruction from the user, prompting to continue or abort
        '''
        # Outcome is always either an Abort exception or we continue, and if continuing we continue
        # any monitoring that was happening when we were called. 
        if not continue_monitoring:
            if getattr(self, "monitor_title", None):
                continue_monitoring = self.monitor_title
            else: 
                continue_monitoring = "<No Title Provided>"

        instruction = self.wait_for_instruction(self.ASK_CONTINUE, interim_result, continue_monitoring)
        if instruction == self.ABORT:
            self.abort(progress, interim_result)
        else:
            assert instruction==self.CONTINUE, f"Invalid instruction {instruction} received by wait_for_continue_or_abort()"
            return instruction

    def wait_for_commit_or_rollback(self, interim_result=None, progress=None, continue_monitoring=None):
        '''
        Waits for an instruction from the user, prompting for a commit or rollback.
        '''
        instruction = self.wait_for_instruction(self.ASK_COMMIT, interim_result, continue_monitoring)
        if instruction == self.ROLLBACK:
            self.rollback(progress, interim_result)
        else:
            assert instruction==self.COMMIT, f"Invalid instruction {instruction} received by wait_for_continue_or_abort()"
            return instruction
            
    def check_for_abort(self, progress=None, result=None): 
        '''
        Checks for an instruction and if it's an abort instruction will 
        abort the task (by raising self.Abort) else will just return the
        instruction. 
         
        :param progress: a progress indicator in form of self.progress()
                         optional, and will provide it to a client with 
                         the state update to ABORTED. 
        :param result:   a result indicator. Given the task is aborted 
                         and won't run to completion an opportunity to
                         return the partial result here. Provided to 
                         clients along with the sate update to ABORTED
        '''
        instruction = self.check_for_instruction()
        if instruction == self.ABORT:
            self.abort(progress)
        else:
            return instruction

    def abort(self, progress=None, result='unfinished result'):
        # If no progress indicator is provided use the last one that self.progress 
        # was used for. Because this is called by the running task instance it has
        # persistence during execution and self._progress is up to date.
        if not progress:
            progress = self._progress
            
        progress['description'] = f"Aborted"
        self.update_state(state="ABORTED", meta={'result': result, 'progress': progress})
        raise self.Exceptions.Abort

    def rollback(self, progress=None, result='unfinished result'):
        # If no progress indicator is provided use the last one that self.progress 
        # was used for. Because this is called by the running task instance it has
        # persistence during execution and self._progress is up to date.
        if not progress:
            progress = self._progress
            
        progress['description'] = f"Rolledback"
        meta = {'result': result, 'progress': progress}
        
        self.update_state(state="ROLLEDBACK", meta=meta)
        raise self.Exceptions.Rollback

    ############################################################################################    
    # END of TASK extensions
    #
    # BEGINNING of DJANGO INTERFACE
    ############################################################################################    

    # Group the DJango specific decorator and view functions.
    
    class Django:
        def __init__(self, task):
            self.task = task
            self.templates = self.Templates()

        def __start__(self, request, form):
            packed_form = self.task.django.pack_form(request, form)
            result = self.task.start(form=packed_form)
            return result.task_id

        def __monitor__(self, response, request):
            template = loader.get_template(self.task.django.templates.monitor)
            response["title"] = self.task.monitor_title if self.task.monitor_title else "<No Title Provided>"
            return HttpResponse(template.render(response, request))

        def __start_and_monitor__(self, request, form):
            self.task.request.id = self.task.django.start(request, form)
            response = {'name':      self.task.name, 
                        'shortname': self.task.shortname, 
                        'id':        self.task.request.id, 
                        'result':    None}
            return self.task.django.monitor(response, request) 

        start = __start__
        monitor = __monitor__
        start_and_monitor = __start_and_monitor__

        def __pack_form__(self, request, form):
            model_name = form.Meta.model.__name__
            model_app = form.Meta.model._meta.app_label
            form_fields = form.Meta.fields
            
            post = {}
            for k in request.POST:
                v = request.POST.getlist(k)
                if len(v) == 1:
                    post[k] = v[0]
                else:
                    post[k] = v
            
            return {'model_name': model_name, 'model_app': model_app, "form_fields": form_fields, "post": post}

        def __unpack_form__(self, packed_form):
            model_name = packed_form["model_name"]
            model_app = packed_form["model_app"]
            form_fields = packed_form["form_fields"]
            post = packed_form["post"]
            Model = apps.get_model(app_label=model_app, model_name=model_name)
            Form = modelform_factory(Model, fields=form_fields)
            form = Form(post)
            return form

        pack_form = __pack_form__
        unpack_form = __unpack_form__

        instruct = Django_Instruct

        pulse = Django_Pulse

        class Templates:
            monitor = "monitor.html"
            confirm = "confirm.html"
            aborted = "aborted.html"
            committed = "committed.html"
            rolledback = "rolledback.html"

    @classmethod
    def Config(cls, config_function):
    
        # See https://docs.celeryproject.org/en/stable/userguide/signals.html
        @before_task_publish.connect
        def config(sender, *args, **kwargs):
            task = current_app.tasks[sender]
            config_function(task, *args, **kwargs)
            task.monitor_title = getattr(task, "monitor_title", getattr(task, "initial_monitor_title"))
        
        return config

    class Exceptions:
        class Abort(Ignore):
            """A task can raise this to Abort execution.
            
               It does nothing more than request Celery to ignore the 
               task from here on in (not send an update to state to
               to the client).
            """
    
        class Rollback(Ignore):
            """A task can raise this to trigger a Rollback.
            
               If the rask is wrapped in Django's @transaction.atomic 
               the exception can trigger a rollback. 
               
               A task can be wrritten as one transaction and ignore
               this exeption in which case it will act as a Celery 
               Ignore (i.e. asks Celery not to update state to SUCCESS
               and return a result, but leave the sate alone).
               
               Good practice would be to catch the Rollback exception
               update status to ROLLEDBACK and the raise Ignore. 
               
               If the task is written in multiple transactions, then
               the Rollback exception can be caught and teh task 
               continue to the next transaction.    

               An Interavtive task has a rollback() method which does
               this, namely sets status to ROLLEDBACK then raises this
               exception and so handles that for a task conveniently. 
               The task cna then just catch the exception and move onto
               the next transaction.
            """

    @classmethod
    def get_request_param(cls, request, key):
        '''
        Trivial class method to conveniently check GET or POST params for a 
        key and return its value. Used so that Django views can receive 
        task IDs and instructions in either form flexibly.
        
        :param request: A django request object
        :param key:     A key to look for in the request
        '''
        get = getattr(request,"GET", {}).get(key, None)
        post = getattr(request,"POST", {}).get(key, None)
        return post if get is None else get 

    @classmethod
    def Task(cls, task_name, task_id=None):
        '''
        Given the name of task will return an instance of a task of that name
        provided it's been registered with Celery and the name identifies a 
        unique task.
         
        :param cls:
        :param name: The name fo the task desired.
        '''
        r = re.compile(fr"\.{task_name}$")
        task_fullnames = list(filter(r.search, list(current_app.tasks.keys())))
        assert len(task_fullnames) == 1, f"get_task provided with an ambiguous name. Provided: {task_name}, Found: {task_fullnames}."
        task = current_app.tasks[task_fullnames[0]]
        
        # If a task_id is provided, attach it to the task where it's normally stored
        # task.request is empty but exists in the current_app.tasks, and so we can set
        # the id in it without trouble.
        if task_id:
            task.request.id = task_id 

        return task

class Interactive(InteractiveBase):
    '''
    Implements the 4 key Broker interactions that InteractiveBase needs:
    
        Connected
            - a decoratore for Celery tasks that wraps it in a broker_read_url Connection
            
        instruct(instruction)
            Used by Task/Client (the task in the View context)
            Sends an instruction to a running task instance.
            
        check_for_instruction()
            Used by Task/Worker (the task in the Worker context)
            Checks for an instruction, retuning None if none found. 
            Non blocking. 

        wait_for_instruction()
            Used by Task/Worker (the task in the Worker context)
            Checks for an instruction, waiting until one arrives. 
            Blocking. 
    
    Based in large part on the tutorials here:
    
        https://medium.com/python-pandemonium/talking-to-rabbitmq-with-python-and-kombu-6cbee93b1298
        https://medium.com/python-pandemonium/building-robust-rabbitmq-consumers-with-python-and-kombu-part-1-ccd660d17271

    An effort to implement these using Kombu.SimpleQueue failed: 

        https://stackoverflow.com/questions/60599301/celery-kombu-simplequeue-get-never-returns-even-when-message-appears-in-queue
        
    and so instead a Kombu.Queue is used with the following communciation stack:
    
        Connection using conf.broker_read_url pr conf.broker_write_url
        Channel (barely relevant detail just ask the connection to provide one)
        Exchange (always named 'celery.interactive' and has a routing key to Queue)
            routing_key is just the id of the running task
        Queue (configurable name, but each running instance of a task has it's own unique Queue it can watch)
    '''

    @classmethod
    def Connected(cls, task_function):
        '''
        A decorator to wrap a Celery task in a connection for interacting with
        the task. The task is provided with a Kombu.Queue that it can read for
        messages from a client (that wants to communicate with this task).
        
        It is stored in the attribute instruction_queue and the code in the task 
        function can access self.instruction_queue if needed. But if the task is 
        also decorated with base=Interactive, then the task will have access to 
        various functions that use self.instruction_queue for common use cases, 
        notably:
        
            self.check_for_instruction()
            self.wait_for_instruction()            
            
        :param task_function: A function that implements a Celery task
                              It should be bound (i.e. have bind=True
                              in the app.task decorator) to ensure that
                              an argument is provided with an instance 
                              of the task itself.
        '''
        @functools.wraps(task_function)
        def connected(task, *args, **kwargs):
            '''
            A wrapper around a Celery task function that puts it inside
            a Connection based on the broker read URL and providing a 
            Queue called instruction_queue to the task on which it looks for 
            messages from someone (possibly a Django.PulseCheck wrapped 
            Django view) 
            
            :param task: An instance of this task (aka self).
            '''
            my_id = task.request.id
            
            if debug:
                logger.debug(f'Interactive task: {task.name}-{my_id}')
            
            # First configure the queue name
            if task.use_indexed_queue_names:
                task.instruction_queue_name = task.first_free_indexed_queue_name()
            else:
                task.instruction_queue_name = task.default_queue_name()

            # 
            xname = "celery.interactive"
            qname = task.instruction_queue_name
            
            # A unique string to flag this result should be ignored.
            # It should simply have no chance of overlapping with an
            # actual task result. So we throw in a uuid for good measure.
            # We do this so thatw e can catch the Ignore exception, to
            # cleanly destroy the Queue this task was using before the 
            # final exit. 
            IGNORE_RESULT = f"__ignore_this_result__{uuid.uuid1()}"

            if debug:
                logger.debug(f'Connecting task to: {current_app.conf.broker_read_url}')
                
            with Connection(current_app.conf.broker_read_url) as conn:
                try:
                    # Connection is lazy. Force a connection now.
                    conn.connect()
                    c = conn.connection
                    laddr = c.sock.getsockname()
                    raddr = c.sock.getpeername()
                    c.name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
                    c.name_short = f"{laddr[0]}:{laddr[1]}"
                    if debug:
                        logger.debug(f'\tConnection: {c.name_short}')

                    # Create a channel on the conection and log it in the RabbitMQ webmonitor format                     
                    ch = c.channel()
                    ch.name = f'{c.name} ({ch.channel_id})'
                    ch.name_short = f'{c.name_short} ({ch.channel_id})'
                    if debug:
                        logger.debug(f'\tChannel: {ch.name_short}')
                    
                    x = Exchange(xname, channel=ch)
                    q = Queue(qname, exchange=x, channel=ch, routing_key=my_id, durable=True)

                    if debug:
                        logger.debug(f'\tExchange: {x.name}')
                        logger.debug(f'\tQueue: {q.name}')

                    # Makes the exchange and queue appear on the RabbitMQ web monitor
                    x.declare()
                    q.declare()
                    
                    # We only need to give task the q property as the exchange,channel and 
                    # connection are all known by q on the off chance the task_functon wants 
                    # access to them.
                    #
                    # q.exchange                  holds the exchange
                    # q.echange.name              holds the name of the exchange
                    # q.channel                   holds the channel
                    # q.channel.channel_id        holds the channel ID
                    # q.channel.name              holds the channel name
                    # q.channel.connection        holds the connection
                    # q.channel.connection.name   holds the connection name 
                    
                    task.instruction_queue = q
                    
                    result = task_function(task, *args, **kwargs)

                    # Leave the exchange alone (it's reusable for other Interactive tasks)
                except Ignore:
                    result = IGNORE_RESULT
                except Exception as e:
                    logger.error(f'ERROR: {e}')
                    task.update_state(state="FAILURE", meta={'result': 'result to date', 'reason': str(e)})
                    result = None
    
                # Delete the queue before the task completes
                task.instruction_queue.delete()
                
                if debug:
                    logger.debug(f'Deleted Queue: {q.name}')
                
            if result == IGNORE_RESULT:
                raise Ignore() 
                
            return result
        
        # Preserver the name of the task function so we can look it up 
        connected.__name__ = task_function.__name__
        return connected

    def instruct(self, instruction):
        '''
        Given an instance of celery.Task (self) and an instruction will,
        if the task has an id in its request.id attribute to identify 
        a runnng instance of the task, send it the provided instruction.
        
        Used by a client wanting to instruct a running task instance to 
        do something. It's entirely up to the task whether it's even 
        checking for let alone acting on such instructions.
        
        To do so we open the exchange 'celery.interactive' using the 
        connection:
        
            current_app.conf.broker_write_url
            
        and use the task id as the routing_lkey to identify reach the 
        tasks Queue.
        
        :param instruction: The instruction to send (a string is ideal but kombu has to be able to serialize it)
        '''
        task_id = self.request.id
        
        if debug:
            print(f'Sending Instruction: {instruction} -> {task_id}')
        
        if task_id:
            with Connection(current_app.conf.broker_write_url) as conn:
                with conn.channel() as ch:
                    # Connection is lazy. Force a connection now.
                    conn.connect()
                    c = conn.connection
                    laddr = c.sock.getsockname()
                    raddr = c.sock.getpeername()
                    c.name = f"{laddr[0]}:{laddr[1]} -> {raddr[0]}:{raddr[1]}"
                    c.name_short = f"{laddr[0]}:{laddr[1]}"
                    if debug:
                        print(f'\tConnection: {c.name_short}')
        
                    ch.name = f'{c.name} ({ch.channel_id})'
                    ch.name_short = f'{c.name_short} ({ch.channel_id})'
                    if debug:
                        print(f'\tChannel: {ch.name_short}')
        
                    x = Exchange(name="celery.interactive", channel=ch)
                    if debug:
                        print(f'\tExchange: {x.name}')
                    
                    p = Producer(ch)
                    p.publish(instruction, exchange=x, routing_key=task_id)
                
                    if debug:
                        print(f'\tSent: {instruction} to exchange {x.name} with routing key {task_id}')
    
    def check_for_instruction(self):
        '''
        Performs a non-blocking read on self.instruction_queue 
        (i.e. checks for an instruction).
        
        Returns None if no instruction found.
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.instruction_queue to check for abort messages."

        if debug:
            logger.debug(f'CHECKING queue "{q.name}" for an instruction.')
            
        try:
            message = q.get()
        except Exception as E:
            # TODO: Diagnose why my Queue dies, if it does ... 
            message = None
            logger.error(f'Error checking {getattr(q, "name")}: {E}')

        if message:            
            instruction = message.payload       # get an instruction if available
            message.ack()                       # remove message from queue
            return instruction
        else:
            return None
    
    def wait_for_instruction(self, prompt=None, interim_result=None, continue_monitoring=None):
        '''
        Performs a blocking read on self.instruction_queue 
        (i.e. waits for an instruction).
        
        :param prompt: Optionally, a prompt that will be sent along with 
                       the state update to WAITING, so the View can if
                       it wants, prompt a user (by whatever means it can) 
                       to provide an instruction.
                       
        :param interim_result: Optionally an interim result that the View
                               can present to the user if desired. A task 
                               returns a result when it's complete, but 
                               if it enters into a wait for instructions
                               it may have an interim result that it wants 
                               feedback on (classically the case for a 
                               commit or rollback request).  

        :param continue_monitoring: If a string is provided will be 
        '''
        q = getattr(self, 'instruction_queue', None)
        assert isinstance(q, Queue),  "A Queue must be provided via self.q to check for abort messages."
        
        instruction = None
        
        def got_message(body, message):
            nonlocal instruction
            instruction = body
            message.ack()
        
        meta = {'prompt': prompt, 'interim_result': interim_result, 'continue_monitoring': continue_monitoring}
        self.update_state(state="WAITING", meta=meta)
        
        if debug:
            logger.debug(f'Updated status to WAITING with info: {meta}')
            logger.debug(f'WAITING for an instruction ... (listening to queue: {q.name})')
                
        ch = q.exchange.channel
        c = ch.connection
        with Consumer(ch, queues=q, callbacks=[got_message], accept=["text/plain"]):
            # drain_events blocks until a message arrives then got_messag() is called.  
            c.drain_events()
        
        if debug:
            logger.info(f'RECEIVED instruction: {instruction}')
        
        return instruction