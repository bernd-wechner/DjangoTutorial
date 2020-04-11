import json

from celery.result import AsyncResult

from django.apps import apps
from django.template import loader
from django.forms.models import modelform_factory
from django.http.request import HttpRequest
from django.http.response import HttpResponse

from .config import debug
from .celery import Task

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
            print(f"\nCHECKING PULSE: args:{args} kwargs:{kwargs}")

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
        task_name = kwargs.pop("task_name", args[0] if len(args)>0 else Django.get_request_param(request, "task_name"))
        task_id = str(kwargs.pop("task_id", args[1] if len(args)>1 else Django.get_request_param(request, "task_id")))
        
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
        instruction = Django.get_request_param(request, "instruction")
        notify = Django.get_request_param(request, "notify")
        
        please_confirm = not Django.get_request_param(request, "confirm") is None
        please_continue = not Django.get_request_param(request, "continue") is None
        please_abort = not Django.get_request_param(request, "abort") is None
        please_cancel = not Django.get_request_param(request, "cancel") is None
        please_commit = not Django.get_request_param(request, "commit") is None
        please_rollback = not Django.get_request_param(request, "rollback") is None
        
        # Get an instance of the task wtih this name and id
        task = Task(task_name, task_id)

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
                # Instruction sent to the task. 
                # TODO: Add a new state INSTRUCTED which the task can respond with 
                # once it's done the instruction.

            # if we receiveed a request to abort or cancel the task in the request parameters
            # send it on to the running task.
            if please_abort or please_cancel:
                task.please_abort()
                if debug:
                    print(f"Asked task to abort: {task.name}-{task.request.id}")
                # Instruction to abort sent tot he task, it will respond with state of "ABORTED"
                # on this or a subsequent pulse check. Below we will let the monitor know it 
                # aborted and whther we want it to call back fro a page defined by 
                # task.django.templates.aborted
                # TODO consider adding a configurable sleep time here.
            
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
                # TODO: if monitor not running, then start one.
                #       How do we know if one is running? We got here I guess.
                #       And we got here with deliver_json or not. So if not
                #       deliver_json then no monitor is running and we can start one.

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
                response["progress"] = request.session.get(store_last_progress, task.progress())
                if debug:
                    print(f"FAILURE: Return to AJAX caller the data: {response}")
                # TODO: Support a template option here. If specified and deliver_son doa  round trip/
                
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
            print(f"RESPONSE to AJAX caller: {response}")
            
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
    task = Task(task_name)
    task_id = Django.get_request_param(request, "task_id")
    instruction = Django.get_request_param(request, "instruction")

    if task and not task_id is None and not instruction is None:
        task.instruct(task_id, instruction)

class Django:
    def __init__(self, task):
        self.task = task
        self.templates = self.Templates()

    def __start__(self, request, form):
        packed_form = self.task.django.pack_form(request, form)
        result = self.task.delay(form=packed_form)
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
        #TODO: configurable templates for Instructed, Failed and Success
        instructed = None
        failed = None
        success = None

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

