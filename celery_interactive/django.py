import json, functools

from django.apps import apps
from django.template import loader
from django.forms.models import modelform_factory
from django.http.request import HttpRequest
from django.http.response import HttpResponse
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError

from . import log
from .celery import Task
from .decorators import ConnectedView

class Django:
    '''
    An encapsulation of Django specific settings (attributes) and function (methods).
    
    To differentiate between this class and Django (the web framework) itself, this 
    class will referred to as Interactive.Django.
    
    This class definition is intended to work with Interactive, encapsulating Django
    features within Interactive (one of the prime uses of nested classes in Python).
    
    It is declared here but the definition lives inside Interactive as Interactive.Django.
    
    Literally:
    
        Interactive.Django = Django
        
    It is instantiated to "django" when Interactive instantiates, so that if:
    
        task = Interactive()
        
    task.Django
        is this class
        
    task.django
        is an instance of this class
        
    In a similar manner template settings are encapsulated in the Templates class
    defined herein as, yes, a nested (inner) class and instantiated when Django 
    instantiates in a similar way, so that:
    
    task.Django.Templates
        is the Templates class
        
    task.django.templates
        is an instance of the Templates class
        
    Celery registers Task instances, and so Interactive is instantiated when it is
    loaded and registered with Celery.
    '''
    def __init__(self, task):
        '''
        A little referential Tomfoolery is performed here.
        
        This class doesn't have a traceable relationship to it's parent class.
        Discussed here:
            https://stackoverflow.com/questions/719705/what-is-the-purpose-of-pythons-inner-classes/722175#722175
        
        And so it is passed an instance of Interactive explicitly which it keeps in
        self.task. Throughout this class then, self.task refers to the nominal Outer
        Class (or parent, though be careful to differentiate between Outer classes, 
        that is a class within which this one is defined, and Base classes, that is a class
        from which this one inherits - both are kind of "parents" which isn't a useful Python
        term really, but helps to conceptualise the relationship)
        
        The upshot is, what is "self" in the Task class (Interactive and InteractiveBase),
        is self.task in this class, and they are the self same thing. An attribute set on
        self.task here will be seen on self in the Task class. 
        
        If anywhere in this class we set:
        
            self.task.name = "Testing 1, 2, 3"
            
        then in the context of the Task (Interactive or InteractiveBase):
        
            print(self.name) 
            
        will produce "Testing 1, 2, 3"
        
        And in an outside context where:
        
            @app.task(bind=True, base="Interactive")
            my_task()
                pass
                
            print(my_task.name)
            my_task.django.task.name = "Django name"
            print(my_task.name)
            my_task.name = "Task name"
            print(my_task.name)
            
        produces:
        
            my_task
            Django name
            Task name
            
        app.task instantiates Interactive sort of like:
        
            my_task = Interactive()
        
        :param self: an instance of Django
        :param task: an instance of Interactive (which is an instance of Celery.Task)
        '''
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
        log.debug(f"Starting {self.task.name}")
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

    # Reconfigurable hooks to default packer and unpacker
    pack_form = __pack_form__
    unpack_form = __unpack_form__

    class Templates:
        '''
        An encapsulation of template settings for Interactive.Django.
        
        Simply a list of template names that are used in various contexts
        by Interactive.Django.
        
        If a task instance exists such that:
        
            task = Interactive()
            
        it will be instantiated such that:
        
            task.Django.Templates
                is this class
                
            task.django.templates
                is an instance of this class
            
        and so on a given instance can be configured for example as follows:
        
            task.django.templates.monitor = "my_monitor_template"
            
        Interactive.Django just asks Django to load the named template when 
        needed using:
        
            django.template.loader.get_template(template_name)
            
        and it's up to the Django configuration to ensure Django can find
        the template.
        '''
        # Reconfigurable hooks to specify templates
        monitor = "monitor.html"
        confirm = "confirm.html"
        aborted = "aborted.html"
        committed = "committed.html"
        rolledback = "rolledback.html"
        #TODO: configurable templates for Instructed, Failed and Success
        instructed = None
        failed = None
        success = None
    
    class PulseCheck:
        '''
        A decorator for Django Views that provides basic task pulse checking 
        features.
        '''
        # Note that:
        # __init__() is the only method called to perform decoration, 
        # __call__() is called every time you call the decorated function.
        def __init__(self, view_function):
            self.view_function = view_function
            functools.update_wrapper(self, view_function)
       
        @ConnectedView
        def __process_request__(self, request, task, *args, **kwargs):
            '''
            Processes an incoming request delivering either a JSON string to
            an AJAX caller or a full HTML page to a page requester. 
            '''
            def request_page(request, template, response):
                '''
                A small bit of resueable code (as we use it in a few places here)
                that simply sets a request_page in the response for the mentioned
                template.
                
                That JSON response, picked up by an AJAX caller should trigger it
                to retrun with a page request to this page with the template in the
                request params, or if the template is a valid URL just to issue a 
                page request to that URL.
                
                If template is a URL ending in ? the AJAX caller should add at least the
                task_id and the response_key, so that the URL can if it is backed by
                a server that knows how, load the response using the response key.
                The response of course contains posisbly useful context for the target
                URL, not least the result of the task, and or its failure reason etc.
                
                :param request:    The request object that provides us with a session store
                :param template:   The template we want requested 
                :param response:   The response dictionay wich we'll add the request_page to. 
                '''
                response["request_page"] = template
                
                host_url = request._current_scheme_host
                if not isURL(template) or template.starts_with(host_url):
                    response["response_key"] = store_response
                    
                request.session[store_response] = response
                log.debug(f"Request that the AJAX caller send a page request for {request.get_full_path()} with template={template} and a context of {response}.")
            
            task_name = task.name
            task_id = task.request.id
            
            # Note up front whether we're wanting to deliver JSON
            # (i.e. have been called via AJAX) or render a page 
            # (i.e. got here through a button press on another page)
            #
            # Django's is_ajax() method just implements an old 
            # on-line defacto standard checking for an HTTP header
            # called X-Requested-With and that it has the value 
            # XMLHttpRequest, so any Javascript fetching this that
            # uses plain Javascript wants to consider this and set 
            # that header.
            deliver_json = request.is_ajax()
            
            # If ajax_confirmations is in the request, we will let the monitor 
            # present confirmation requests. Otherwise we will use configured
            # templates. 
            ajax_confirmations = not Django.get_request_param(request, "ajax_confirmations") is None
    
            # Get any instructions that may have been submitted that we support    
            instruction = Django.get_request_param(request, "instruction")
            requested_template = Django.get_request_param(request, "template")
            
            # Has a general request to cancel the task arrived?
            please_cancel = not Django.get_request_param(request, "cancel") is None

            # Responses to ASK_CONTINUE conrimation requests 
            please_continue = not Django.get_request_param(request, "continue") is None
            please_abort = not Django.get_request_param(request, "abort") is None
            
            # Responses to ASK_COMMITconrimation requests 
            please_commit = not Django.get_request_param(request, "commit") is None
            please_rollback = not Django.get_request_param(request, "rollback") is None
    
            # Start the JSON reponse dict we'll send to the caller on an ordinary pulse check.
            # We always want to keep the browser appraised of the task id so it can pass that
            # back in on susequent pulses and know this response is still for the same running
            # task. 
            response = {'name':      task.name, 
                        'shortname': task.shortname, 
                        'id':        task.request.id,
                        'state':     'UNKNOWN', 
                        'result':    None}
            
            if task and task.request.id:
                # The name of a session keys in which to store data we wish to persist
                store_last_progress = f"task_{task.name}_{task.request.id}_last_progress"
                store_is_waiting_on = f"task_{task.name}_{task.request.id}_is_waiting_on"
                store_response  = f"task_{task.name}_{task.request.id}_response"
    
                ####################################################################
                # First up this is a page request and not an AJAX call, with a 
                # template requested then let's cut to the chase and honor that.
                if requested_template and not deliver_json:
                    response = request.session.get(store_response, response)
                    saved_template = response.get('request_page', '')

                    if requested_template != saved_template:
                        # If this happens the Javascript monitor screwed things up. It should receive
                        # a request_page instruction which provides a template that it should send
                        # back in the request as template, and hereing when passing a request_page
                        # back in JSON the template should be saved in the response. This is a simple
                        # integrity check.  
                        log.warning("Warning: A template page request was made with a template that differs from the saved one.")
                    
                    template = loader.get_template(requested_template)
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
                    log.debug(f"Instructed task to abort: {task.fullname}")
                    # Instruction sent to the task. 
                    # TODO: Add a new state INSTRUCTED which the task can respond with 
                    # once it's done the instruction.
    
                # if we received a request to abort or cancel the task in the request parameters
                # send it on to the running task.
                if please_abort or please_cancel:
                    task.please_abort()
                    log.debug(f"Asked task to abort: {task.fullname}")
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
                results = task.get_updates(task_id)
                for r in results:
                    #TODO: Thinks this through. For now, just report them and keep 
                    #      the last one.
                    log.debug(f"Got result: state: {r.state}, result/info: {r.result}, Is waiting on: {request.session.get(store_is_waiting_on, 'Not WAITING')}")
                 
                r = results[-1]
                
                # NOTE: r.info is an alias for r.result and is polulated by the meta argument 
                #       in Celery's update_state - they are all the same thing.
                #
                #       https://docs.celeryproject.org/en/stable/reference/celery.result.html#celery.result.AsyncResult
                #       https://docs.celeryproject.org/en/stable/_modules/celery/result.html#AsyncResult
                #
                # Here we tend tend to use r.result when the task is complete and r.info for status updates.
                # They both contain the meta argument to task.update_state() 
                log.debug(f"Kept FINAL result: state: {r.state}, result/info: {r.result}, Is waiting on: {request.session.get(store_is_waiting_on, 'Not WAITING')}")
    
                # Associate the observed state with the task at hand and add it to the response
                task.state = r.state
                response['state'] = task.state 
                
                # Make a note of progress
                # It's either provided in the result and if not we might have the last one in session store
                # If neither exists, then None so we know none was provided.
                progress = r.info.get('progress', request.session.get(store_last_progress, None))
                
                # TODO: REQUESTED and KILLED custom states have been added.
                
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
                    log.debug(f"WAITING on same thing so STILL_WAITING on: {waiting_info}")
                
                # Else, if we have waiting_info then we know we were WAITING on something
                # but maybe we are still waiting or maybe not. 
                elif waiting_info:
                    # Sometimes Celery overwrites the state of a WAITING task with PENDING
                    # Not sure exactly why or when, but when it does, we know we're 
                    # STILL_WAITING because we have waiting_info in the session_store
                    if task.state == "PENDING":
                        task.state = "STILL_WAITING"
                        response['state'] = task.state 
                        r.info = waiting_info
                        log.debug(f"PENDING while WAITING so STILL_WAITING on: {waiting_info}")
                    
                    # If task.state == "WAITING" it's because the new info does not match
                    # the old info, implying we're now waiting on something else. The task 
                    # has changed it's mind about what it's waiting for. That being the 
                    # case we maintain the "WAITING" state unaltered.
                    elif task.state == "WAITING":
                        pass
                    
                    # Finally if the state is something other than WAITING or PENDING
                    # then we're not waiting any more and must remove the waiting_info
                    # from the session store. 
                    else:
                        request.session.pop(store_is_waiting_on)
                        log.debug(f"NO LONGER WAITING on: {waiting_info}, now {task.state} with {r.info}")
    
                ###################################################################
                # Now we can check the state the task is in and act accordingly.

                if task.state == "PROGRESS":
                    # The classic update is PROGRESS which provides informatiom that will 
                    # help the client update a progress bar. 
                    #
                    # If we were WAITING we no longer are so pop the record of that out 
                    # of the session store (i.e. erase it there). 
                    #
                    # if this is not a PROGRESS update, but some other status update, 
                    # grab the last known  progress for other views
                    request.session.pop(store_is_waiting_on, None)
                    if not progress:
                        progress = task.progress.as_dict()
                    request.session[store_last_progress] = progress
                    response["progress"] = progress
                    log.debug(f"PROGRESS: {progress}")
    
                if task.state == "STARTED":
                    # STARTED is the state of the task just after we started it.
                    # Given we get an state update immediately after it may still
                    # be STARTED or have updated state by then. 
                    log.debug(f"Task is STARTED")
                    response["progress"] = task.progress.as_dict()
                    log.debug(f'STARTED: {response["progress"]}')
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
                    if r.info and hasattr(r.info, 'progress') and isinstance(r.info['progress'], dict):
                        response["progress"] = r.info['progress']
                    else:
                        response["progress"] = request.session.get(store_last_progress, task.progress.as_dict())
                    
                    log.debug(f'PENDING: {response["progress"]}')
                    
                elif task.state in ["ABORTED", "ROLLEDBACK"]:
                    # If the task received an instruction to ABORT or ROLLBACk it will update
                    # state to tell us it did that! In which case again it is no longer WAITING
                    # so if it was so we clear that from session store, and can return and a page
                    # defined by a configured template. We can do this because we're no longer
                    # monitoring.                 
                    winfo = request.session.pop(store_is_waiting_on, {})
                    response['canceled'] = True
                    response["progress"] = r.info.get('progress', request.session.get(store_last_progress, task.progress.as_dict()))
                    
                    if task.state == "ABORTED":
                        template = task.django.templates.aborted
                        # Aborting the task precludes any continuation or need to monitor such.  
                        continue_monitoring = False
                    elif task.state == "ROLLEDBACK":
                        template = task.django.templates.rolledback
                        # Rolling back a task might abort the task (for a task managing a single 
                        # database transaction) or it might want to continue. Only the task knows
                        # and it informs us when it goes into the wait whether it wants to continue
                        # monitoring or not and that is stored in session so we don't forget.
                        continue_monitoring = winfo.get('continue_monitoring', False)
                     
                    log.debug(f'{task.state}: Template: {template}, continue_monitoring: {continue_monitoring}')
    
                    if continue_monitoring:
                        # We have reload the monitor if it's not how we got here. 
                        if not deliver_json:
                            log.debug(f"Rollback complete, continue monitoring with {task.django.templates.confirm}: {response}")
                            template = loader.get_template(task.django.templates.monitor)
                            return HttpResponse(template.render(response, request))
                        else:
                            log.debug(f"Rollback complete, continue monitoring, returns to AJAX caller this data: {response}")
                            
                    else:
                        response['result'] =  str(r.info.get('result', ''))
    
                        if not deliver_json:
                            # If we cannot deliver JSON we need a template defined. It's a critical error
                            # if one isn't and we can't find one.
                            log.debug(f"Return page: {template}, with context: {response}")
                            
                            template = loader.get_template(template)
                            # Provide the response as context to the template
                            return HttpResponse(template.render(response, request))
                        
                        elif template:
                            # If we must deliver JSON and there is a template defined, we respond
                            # with JSON that asks the monitor to redirect here with a page request.
                            response["k"] = template
                            request.session[store_response] = response
                            log.debug(f"Request that the AJAX caller send a page request for {request.get_full_path()} with template={template} and a context of {response}.")
                            
                        else:
                            # If we must deliver JSON and don't have a template it means we are content for the monitor to
                            # notify about therollback. It will know from the result in the response.
                            log.debug(f"Return to AJAX caller the data: {response}")
                    
                elif task.state == "FAILURE":
                    # If the task throws an exception then Celery will return a FAILURE status with
                    # the exception explained in the result. It's no longer WAITING if it was, and 
                    # can fall back to a JSON response to the pulse checker (monitor) that got us here.
                    #
                    # TODO: Add a template option for error reporting. So we end up with same options as
                    #       for rollbacks above. In fact this block cna merger with that one.
                    request.session.pop(store_is_waiting_on, None)
                    response["failed"] = True
                    response["result"] = str(r.result)  # Contains the error
                    # result=info=meta, so if FAILUR writes error message to result we have no info that contains progress 
                    response["progress"] = request.session.get(store_last_progress, task.progress())
                    log.debug(f"FAILURE: Return to AJAX caller the data: {response}")                    
                    
                elif task.state == "SUCCESS":
                    # When the task is complete it will return the "SUCCESS" status and
                    # delivers a result. As ever if it was WAITING it no longer is so
                    # clear our record of that.
                    request.session.pop(store_is_waiting_on, None)
                     
                    response["progress"] = task.progress.done("Done")
                    response["result"]   = str(r.result) # Contains the returned value of the task function
                    
                    log.debug(f"SUCCESS: Return to AJAX caller the result: {response}")
                    
                elif task.state in ["WAITING", "STILL_WAITING"]:
                    # Finally, if the task is WAITING on a response from us, we better deliver one,
                    # that means a round trip to the web browser. We expect a prompt which is either
                    # ASK_CONTINUE or ASK_COMMIT, which will determine the template we want to render
                    # and the kind of response we are seeking.
                    
                    # Persist the waiting_info in the session store
                    # But only on WAITING, not STILL WAITING
                    # i.e. on the first notification we're WAITING 
                    if task.state == "WAITING":
                        request.session[store_is_waiting_on] = r.info
                        log.debug(f"WAITING (for first time) on: {r.info}")
                    
                    # Recalling that info=result=meta
                    # The WAITING state provides a prompt and we expect progress with an interim result
                    # as well as letting us know if it wasnt us to continue monitoring after the response
                    # is delivered.
                    prompt = r.info.get('prompt', '')
                    response["prompt"]         = prompt
                    response["waiting"]        = True
                    response["progress"]       = progress if progress else task.progress.as_dict()
    
                    continue_monitoring = r.info.get('continue_monitoring', False)
                    
                    # We support two standard WAITING prompts:
                    #
                    # ASK_CONTINUE which is asking to continue or abort
                    # ASK_COMMIT   which is asking if a result shoudl be committed or rolled back
                    if prompt in [task.ASK_CONTINUE, task.ASK_COMMIT]:
                        # First check if we are here witha response from the user or not.
                        # please_abort is not relevant here as if we got that resposne we acted on as a 
                        # priority above. But the remaining three repsonses from the two standard
                        # questions ASK_CONTINUE and ASK_COMMIT are handeled here. 
                        got_response = please_continue or please_commit  or please_rollback  
                        
                        log.debug(f"ASK {prompt} from task: {task.fullname}, got_response: {got_response}")
    
                        # If we get here and the the task is WAITING state with 
                        # the ASK_CONTINUE prompt we either need to present the 
                        # user with the question, or we've received a response 
                        # to that question from the user.
                        #
                        # In both cases the task is WAITING and the prompt is
                        # ASK_CONTINUE, we know that a user has responded if
                        # please_continue or please_abort is true.
                        # 
                        # If neither is true we know to pesent the question to the user.
                        #
                        # if ajax_confirmations is true the monitor wants to handle
                        # the presentation of confirmation requests to the user otherwise
                        # we need to have a template to render defined. That requires an 
                        # extra round trip if we notice this during asn AJAX request. We
                        # need to ask the monitor to reload the page as a page request
                        # so we can deliver a template. 
                                               
                        # Step 2
                        #     Takes priority - if we have a response we can act on it, we don't need
                        #     to ask for one. It's step 2 because in step 1we have to ask for a 
                        #     response. 
                        #
                        #     We got a response. If it was please_abort that is handled above, as it
                        #     takes priority and we do that before we even check the task status.
                        #     please_continue, please_commit and please_rollback are all handled here.
                        if got_response:
                            if please_continue:
                                # Implies postive_URL was requested in resposne fo ASK_CONTINUE. 
                                # So we let the task know that it should continue
                                task.please_continue()
                                
                                # We don't support a templated response to please_continue, only continue_monitori
                                template = None
                                if not continue_monitoring:
                                    continue_monitoring = "Continuing as requested"

                                log.debug(f"Asked tast to continue.")

                            elif please_commit:
                                # Implies postive_URL was requested in resposne fo ASK_COMMIT. 
                                # So we let the task know that it should commit the transaction
                                task.please_commit()
                                template = task.django.templates.committed
                                log.debug(f"Asked tast to commit.")
    
                            elif please_rollback:
                                # Implies negative_URL was requested  in resposne fo ASK_COMMIT. 
                                # So we let the task know that it should roll back the transaction
                                task.please_rollback()
                                template = task.django.templates.rolledback
                                log.debug(f"Asked tast to roll back.")

                            if continue_monitoring:
                                # If it's a page request, we render the monitor configured in
                                #      task.django.templates.monitor
                                # Otherwise we just fall back on the standard AJAX respsonse.
                                if not deliver_json:
                                    log.debug(f"Continue monitoring with new monitor, titled '{continue_monitoring}': {response}")
                                    task.monitor_title = continue_monitoring
                                    return task.django.monitor(response, request)
                                else:
                                    log.debug(f"Continue monitoring with existing monitor: {response}")
                                    
                            # If we don't want to continue monitoring and have a template we can just deliver that
                            elif template:
                                # If it's a page request we can just deliver it now
                                if not deliver_json:
                                    log.debug(f"\tDeliver landing page: '{template}': {response}")
                                    template = loader.get_template(template)
                                    return HttpResponse(template.render(response, request))
                                
                                # If it's AJAX request and we must deliver JSON we request a reload instead
                                else:
                                    request_page(request, template, response)
                                    
                            else:
                                log.error(f"Internal Error: After a confirmation request we must continue monitoring or have a template to render.")

                        else:
                            # If we haven't got a response yet (i.e. we  need to ask the question
                            # then we prepare in the response the necessary answer defintions. 
                            thisURL = request.build_absolute_uri(f"?task_id={task.request.id}")
                            
                            if prompt == task.ASK_CONTINUE:
                                response.update(
                                        {                                        
                                          'positive_lbl':   "Continue", 
                                          'negative_lbl':   "Abort",
                                          'positive_URL':   f"{thisURL}&continue", 
                                          'negative_URL':   f"{thisURL}&abort",
                                        })
                            elif prompt == task.ASK_COMMIT:
                                response.update(
                                        {                                        
                                          'positive_lbl':   "Commit", 
                                          'negative_lbl':   "Discard",
                                          'positive_URL':   f"{thisURL}&commit", 
                                          'negative_URL':   f"{thisURL}&rollback",
                                        })
                            
                            # Step 1:
                            #     If we don't have a response yet, we need to ask to present the user with
                            #     a question.
                            #
                            #     if ajax_confirmations are requested by the monitor deliver this as JSON
                            #     if on the other hand this is a page request we render the appropriate template.
                            #        To become a page request we need a prefix step 0 (below) which
                            #        asks the monitor to perform a page request. It can't receive a 
                            #        templated page in the response as it's expecting JSON, we need the 
                            #        browser to load the page properly.      
                            if ajax_confirmations or not deliver_json:
                                # We prepare a response that empowers the recipient to present the 
                                # confirmation request to a user. It will be delivred vi JSON if
                                # ajax_confirmations are demands, or via a template if the monitor 
                                # does not want ajax_confirmations and received instead a request to
                                # load this URL as aapage request, in which case it comes back with
                                # "confirm" in the request.
                                    
                                # If we arrived here with a page request (deliver_json is false) then 
                                # we deliver a rendered tenplate. Else we just deliver the resonse in 
                                # JSON as normal (i.e. pass thru, it's down below). 
                                if not deliver_json:
                                    log.debug(f"ASK {prompt} with {task.django.templates.confirm}: {response}")
                                    template = loader.get_template(task.django.templates.confirm)
                                    return HttpResponse(template.render(response, request))
                                else:
                                    log,debug(f"ASK {prompt} returns to AJAX caller this data: {response}")
                                        
                            # Step 0 only if we need to convert the current AJAX request to a
                            #        page request first. A sort of pre-step asking the monitor 
                            #        to bounce right back here with a page request.
                            elif deliver_json: 
                                template = task.django.templates.confirm
                                request_page(request, template, response)

            #######################################################################
            # RETURN a JSON dict capable of feeding a progress bar 
            #######################################################################
    
            # Call the decorated view function 
            contrib = self.view_function(request, task_name)
    
            # PulseChecker in pulse_check.js checks:
            #
            # id         - if the task is running
            # progress   - a dict containing percent, current, total and description
            # complete   - a bool, true when done
            # success    - a bool, false on error, else true
            # canceled   - a bool, true when canceled
            # waiting    - a bool, true when the task is waiting  
            # instructed   notifying it that an instruction was sent to the task
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
                log.debug(f"Decorated task contributes: {contrib}")
                    
                clobber = contrib.pop("__overwrite__", False)
                     
                for k,v in contrib.items():
                    if clobber or not k in reserved:
                        response[k] = v
            elif contrib:
                raise Exception("Configuration error: Django Pulse Check decorator wraps view function that does not return a dict.")
              
            # Return the dictionary as JSON string (intended to be used 
            # in Javascript at the client side, to drive a progress bar
            # and/or other feedback) 
            log.debug(f"RESPONSE to AJAX caller: {response}")
                
            return HttpResponse(json.dumps(response))

        def __call__(self, *args, **kwargs):
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
           
            log.debug(f"\nCHECKING PULSE with {self.__name__}: args:{args} kwargs:{kwargs}")
    
            # The first argument might be "self" from a class method. 
            #
            # Interactive.Django certainly provides a method decorated with this 
            # decorator and so arg[0] will be an instance of Interactive.Django. 
            # 
            # But it doesn't really matter if it's an Interactive.Django method or some 
            # other  class method we're decorating, class methods all pass the class instance 
            # in arg[0], canonically called 'self'.
            #
            # But if a view function is decorated that is NOI a class method then the
            # first argument will NOT be self. We expect the first argument of a Django 
            # view to be HttpRequest instance. So if the second argument is an HttpRequest
            # we can infer that self was passed as arg[0].
            if len(args) > 1 and isinstance(args[1], HttpRequest):
                # TODO: test that this actually happens.
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
            # running task of that ID. The id might arrrive as a UUID and
            #  we need it as a string not a UUI object.
            task_name = kwargs.pop("task_name", args[0] if len(args)>0 else Django.get_request_param(request, "task_name"))
            task_id = str(kwargs.pop("task_id", args[1] if len(args)>1 else Django.get_request_param(request, "task_id")))
            
            if not task_name:
                raise Exception(f"{self.__name__}: NO task_name provided")

            # Get an instance of the task with this name and ID
            # the ID is optional, if we don't have one we'll start 
            # the task to get one.
            task = Task(task_name, task_id)

            # As we have no task ID we infer a request to start the task and get one.
            if not task_id: 
                r = task.start(*args, **kwargs)
                task.request.id = r.task_id
                task.state = r.state # should be "STARTED"

                log.debug(f"Started {task.fullname}, state: {task.state}")
            
            # Make the request available for processing
            # Process the request
            return self.__process_request__(request, task)
            
    @PulseCheck
    def __pulse_check__(self, request, *args, **kwargs):
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

    # Reconfigurable hook to the default pulse checkeer
    pulse_check = __pulse_check__

    def instruct(self, request, task_name):
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
        #TODO: Test this. It's moved into the Django class since last tested.
        task = Task(task_name)
        task_id = Django.get_request_param(request, "task_id")
        instruction = Django.get_request_param(request, "instruction")
    
        if task and not task_id is None and not instruction is None:
            task.instruct(task_id, instruction)

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

##############################################################################################
## Django helper Functions
##############################################################################################

def isURL(string):
    '''
    Using the Django core URLvalidator tests whether the supplied string is a URL or not. 
    
    :param string: The string to test.
    '''
    validate = URLValidator()
    try:
        validate(string)
        return True
    except ValidationError:
        return False