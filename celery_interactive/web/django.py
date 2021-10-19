from django.apps import apps
from django.template import loader
from django.forms.models import modelform_factory
from django.http.response import HttpResponse
from django.http.request import QueryDict

from .. import log
from ..tasks.celery import Task
from ..decorators import django

import json

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
        log.debug(f"Starting tast with packed form {json.dumps(packed_form, indent=4)}")
        result = self.task.start(form=packed_form)
        log.debug(f"Task is started with id: {result.task_id}")
        return result.task_id

    def __monitor__(self, response, request):
        log.debug(f"Loading template '{self.task.django.templates.monitor}'")
        template = loader.get_template(self.task.django.templates.monitor)
        response["title"] = self.task.monitor_title if self.task.monitor_title else "<No Title Provided>"
        log.debug(f"Loaded template '{template.origin.name}'")
        log.debug(f"Rendering template with context {response}")
        return HttpResponse(template.render(response, request))

    def __start_and_monitor__(self, request, form):
        log.debug(f"Starting {self.task.name}")
        self.task.request.id = self.task.django.start(request, form)
        log.debug(f"Started {self.task.name}, starting Monitor")

        # This attribute means we can include tasks in Django contexts and the context
        # handler will work properly. It tries to call callables (and Task is a callable 
        # class (has an __call__ attribute), and in so doing actually prevents us accessing
        # the class properties. By preventing this call, we can access the class properties
        # here in a template if the task is included in the template. 
        self.task.do_not_call_in_templates = True
        response = {'task':      self.task,
                    'name':      self.task.name, 
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

        # We need a QueryDict to initialise the Form with
        qd = QueryDict('', mutable=True)
        qd.update(post)
        form = Form(qd)
        
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
        instructed = "instructed.html"
                
        #TODO: configurable templates for Instructed, Failed and Success
        failed = None
        success = None
    
    @django.PulseCheckView
    def __pulse_check__(self, request, *args, **kwargs):
        '''
        A Django view, provided, pre-decorated that does only the standard 
        pulse checks. If they are all that is needed then this is fine. It
        is provided in Interactive.Django.Progress as well for convenience.
         
        If you want to add things to the response, just decorate your own
        view with Django_PulseCheck.
         
        :param request:   A Django request object, as Django provides to view functinos
        :param task_name: (in kwargs). The name of a task, that Django provides as a  
                          kwarg from the urlpatterns. That is, you need to invoke this  
                          view with something like:
                           
                          urlpatterns += [
                              path('progress/<task_name>', Interactive.Django.Progress)
                          ]
                           
                          so that Django provides task_name as a kwarg to this view function.
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
        task_id = django.get_request_param(request, "task_id")
        instruction = django.get_request_param(request, "instruction")
    
        if task and not task_id is None and not instruction is None:
            task.instruct(task_id, instruction)

