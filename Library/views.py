from django.views.generic import DetailView, ListView, CreateView, UpdateView, TemplateView
from django.utils import timezone
from django.utils.timezone import activate, deactivate
from django.forms import inlineformset_factory
from django.db import connection, transaction
from django.db.utils import IntegrityError
from django.forms import ValidationError
from django.http.response import HttpResponse, HttpResponseRedirect
from django.urls import reverse,reverse_lazy
from django.shortcuts import get_object_or_404

from dateutil import parser
from datetime import datetime

from Library.models import Author, Book, Chapter, Article, Event

import pytz, json
from .HTML import Table

def get_SQL(query):
    '''
    A workaround for a bug in Django which is reported here (several times):
        https://code.djangoproject.com/ticket/30132
        https://code.djangoproject.com/ticket/25705
        https://code.djangoproject.com/ticket/25092
        https://code.djangoproject.com/ticket/24991
        https://code.djangoproject.com/ticket/17741
        
    that should be documented here:
        https://docs.djangoproject.com/en/2.1/faq/models/#how-can-i-see-the-raw-sql-queries-django-is-running
    but isn't.
    
    The work around was published by Zach Borboa here:
        https://code.djangoproject.com/ticket/17741#comment:4
        
    :param query:
    '''
    sql, params = query.sql_with_params()
    cursor = connection.cursor()
    cursor.execute('EXPLAIN ' + sql, params)
    return cursor.db.ops.last_executed_query(cursor, sql, params).replace("EXPLAIN ", "", 1)        

    
class AuthorDetailView(DetailView):

    model = Author

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context

import sys
class AuthorListView(ListView):

    model = Author
    paginate_by = 100  # if pagination is desired

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context

class BookDetailView(DetailView):

    model = Book

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context

class BookCreate(CreateView):
    model = Book
    fields = '__all__'
    
    ChapterFormSet = inlineformset_factory(Book, Chapter, fields="__all__")
 
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        context['chapters'] = self.ChapterFormSet()
        return context
    
    def post(self, request, *args, **kwargs):
        self.form = self.get_form()
        
        if self.form.is_valid():
            try:
                with transaction.atomic():
                    self.object = self.form.save(commit=True)
                                        
                    self.formset = self.ChapterFormSet(request.POST, request.FILES, instance=self.object)
                    if self.formset.is_valid():
                        self.formset.save()
                        
                    self.object.clean()
     
            except (IntegrityError, ValidationError) as e:
                for field, errors in e.error_dict.items():
                    for error in errors:
                        self.form.add_error(field, error)
                return self.form_invalid(self.form)
                                      
            return self.form_valid(self.form)
        else:
            return self.form_invalid(self.form)
           
    def form_valid(self, form):
        #self.object = form.save()
        return HttpResponseRedirect(reverse('book-detail', kwargs={'pk': self.object.pk}))
  
#     def form_invalid(self, form):
#         context = self.get_context_data(form=form)        
#         response = self.render_to_response(context)
#         return response        
  
class BookUpdateView(UpdateView):

    model = Book
    fields = '__all__' 

    ChapterFormSet = inlineformset_factory(Book, Chapter, fields="__all__")

#     def get_object(self, *args, **kwargs):
#         self.object = get_object_or_404(self.model, pk=self.kwargs['pk'])       
#         self.success_url = reverse_lazy('book-detail', kwargs=self.kwargs)
#         return self.object

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        book = Book.objects.get(pk=self.kwargs['pk'])
        context['chapters'] = self.ChapterFormSet(instance=book)
        return context

    def post(self, request, *args, **kwargs):
        #self.success_url = reverse('book-detail', kwargs=self.kwargs)
        
        # We MUST set self.object to do an Update. Failing to do so, will create a new object!
        # self.get_form().save() checks self.object to decide on a  SQL update or insert!
        # This is set int he defaul post method in BaseUpdateView which does this:
        #    self.object = self.get_object()
        # in order to set it. Then it passes on to the post method in ProcessFormView which
        # simply does:
        #
        #     form = self.get_form()
        #     if form.is_valid():
        #         return self.form_valid(form)
        #     else:
        #         return self.form_invalid(form)
        #
        # Rather simple all up. By overriding post then we need to do this and whatever 
        # else we want in a sense of delegat back up with a call to:
        #
        #     super().post(request, *args, **kwargs)
        #
        # before or after we do our stuff in order to get this default behaviour 
        # stream. BaseCreateView by comparison has one line in the post method:
        #
        #         self.object = None
        #
        # before pasing to ProcessFormView, this is what differentiates them!
        
        self.object=self.model.objects.get(pk=self.kwargs['pk'])
        self.form = self.get_form()

        if self.form.is_valid():
            try:
                with transaction.atomic():
                    self.object = self.form.save(commit=True)
                                        
                    self.formset = self.ChapterFormSet(request.POST, request.FILES, instance=self.object)
                    if self.formset.is_valid():
                        self.formset.save()

            except IntegrityError:
                transaction.set_rollback(True)
                return self.form_invalid(self.form)
            
            return self.form_valid(self.form)
        else:
            return self.form_invalid(self.form)                           
           
    def form_valid(self, form):
        #self.object = form.save()
        return HttpResponseRedirect(reverse('book-detail', kwargs=self.kwargs))
  
    def form_invalid(self, form):
        context = self.get_context_data(form=form)        
        response = self.render_to_response(context)
        return response        
  
class BookListView(ListView):

    model = Book
    paginate_by = 100  # if pagination is desired
    
    def get_queryset(self, *args, **kwargs):
        #qs = self.model.objects.filter(title='My Life')
        qs = self.model.objects.all()
        
        print("Queryset returns {0} items.".format(len(qs)))
        print("The executed SQL was:\n {}".format(connection.queries[0]['sql']))
        sql = str(qs.query)
        print("The SQL that queryset.query returns is:\n {}".format(sql))
        
        SQL = get_SQL(qs.query)
        print("The SQL that EXPLAIN returns is:\n {}".format(SQL))

        # This breaks because sql is broken!
        #raw_qs = self.model.objects.raw(sql)
        #print("Raw queryset returns {0} items.".format(len(raw_qs)))

        # This works because SQL is well formed!
        raw_qs = self.model.objects.raw(SQL)
        print("Raw queryset returns {0} items.".format(len(raw_qs)))
        
        return qs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context 
      
class ArticleDetailView(DetailView):

    model = Article

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context
    
class ArticleListView(ListView):

    model = Article
    paginate_by = 100  # if pagination is desired

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        return context
    
def TimeTestView(request):
    '''
    A view which tests how timezones play out in Django.
    
    We want to create events in this pattern to observe the results:
    
        Venue    Created in    Viewed in
        
        Hobart   Hobart        Hobart
        Hobart   Hobart        Perth
        Hobart   Perth         Hobart
        Hobart   Perth         Perth
        
        Perth    Hobart        Hobart
        Perth    Hobart        Perth
        Perth    Perth         Hobart
        Perth    Perth         Perth
    
    Wew will:
    
        activate a timezone
        create a time in that timezone
        create an event with that time
        activate a timezone
        read the event
        
    There are two events we'll look at at opposite ends of 
    Australia, one in Hobart, one in Perth. Both at the same
    time of day. Perth is 3 hours behind Hobart.
    
    '''
    
    # Set a refernce date and time in local time for the two events. 
    # Both are at this time, thogh the Hobart event takes place 3 hours 
    # before the Perth one of course.   
    Event_date = "1/1/2000"
    Event_time = "19:00"
    Event_date_time_naive = parser.parse(f"{Event_date} {Event_time}")

    TZ_Hobart = pytz.timezone('Australia/Hobart')
    TZ_Perth = pytz.timezone('Australia/Perth')
    
    Event_date_time_Hobart = timezone.make_aware(Event_date_time_naive, TZ_Hobart)
    Event_date_time_Perth = timezone.make_aware(Event_date_time_naive, TZ_Perth)
    
    intro = [
        [
            "Event Time",
            "Value",
            "Is naive?",
            "Is aware?"
        ],
        [
            "Naive",
            str(Event_date_time_naive),
            str(timezone.is_naive(Event_date_time_naive)),
            str(timezone.is_aware(Event_date_time_naive))
        ],
        [
            "Hobart",
            str(Event_date_time_Hobart),
            str(timezone.is_naive(Event_date_time_Hobart)),
            str(timezone.is_aware(Event_date_time_Hobart))
        ],
        [
            "Perth",
            str(Event_date_time_Perth),
            str(timezone.is_naive(Event_date_time_Perth)),
            str(timezone.is_aware(Event_date_time_Perth))
        ]
    ]
    
    table_intro = Table(intro)
    
    ####################################################
    # Now let's create the events (6 in total)
    
    # Naive person creating the event
    Naive_Event = Event.objects.get_or_create(pk=1)[0]
    Naive_Event.creator = "Naive"
    Naive_Event.title = "A Naive Event"
    Naive_Event.venue = "Naive"
    Naive_Event.date_time = Event_date_time_naive
    Naive_Event.save()

    Hobart_Event = Event.objects.get_or_create(pk=2)[0]
    Hobart_Event.creator = "Naive"
    Hobart_Event.title = "A Hobart Event"
    Hobart_Event.venue = "Hobart"
    Hobart_Event.date_time = Event_date_time_Hobart
    Hobart_Event.save()

    Perth_Event = Event.objects.get_or_create(pk=3)[0]
    Perth_Event.creator = "Naive"
    Perth_Event.title = "A Perth Event"
    Perth_Event.venue = "Perth"
    Perth_Event.date_time = Event_date_time_Perth
    Perth_Event.save()

    # Person creating the event is in Hobart
    activate(TZ_Hobart)

    Naive_Event = Event.objects.get_or_create(pk=4)[0]
    Naive_Event.creator = "Hobart"
    Naive_Event.title = "A Naive Event"
    Naive_Event.venue = "Naive"
    Naive_Event.date_time = Event_date_time_naive
    Naive_Event.save()

    Hobart_Event = Event.objects.get_or_create(pk=5)[0]
    Hobart_Event.creator = "Hobart"
    Hobart_Event.title = "A Hobart Event"
    Hobart_Event.venue = "Hobart"
    Hobart_Event.date_time = Event_date_time_Hobart
    Hobart_Event.save()

    Perth_Event = Event.objects.get_or_create(pk=6)[0]
    Perth_Event.creator = "Hobart"
    Perth_Event.title = "A Perth Event"
    Perth_Event.venue = "Perth"
    Perth_Event.date_time = Event_date_time_Perth
    Perth_Event.save()
    
    # Person creating the event is in Perth
    activate(TZ_Perth)

    Naive_Event = Event.objects.get_or_create(pk=7)[0]
    Naive_Event.creator = "Perth"
    Naive_Event.title = "A Naive Event"
    Naive_Event.venue = "Naive"
    Naive_Event.date_time = Event_date_time_naive
    Naive_Event.save()

    Hobart_Event = Event.objects.get_or_create(pk=8)[0]
    Hobart_Event.creator = "Perth"
    Hobart_Event.title = "A Hobart Event"
    Hobart_Event.venue = "Hobart"
    Hobart_Event.date_time = Event_date_time_Hobart
    Hobart_Event.save()

    Perth_Event = Event.objects.get_or_create(pk=9)[0]
    Perth_Event.creator = "Perth"
    Perth_Event.title = "A Perth Event"
    Perth_Event.venue = "Perth"
    Perth_Event.date_time = Event_date_time_Perth
    Perth_Event.save()
    
    ####################################################
    # Now let's read and interpret the saved events

    result = [["Reader", "Creator", "Venue", "Value", "Local Time"],]

    # Naive person reading the events   
    deactivate()
    
    Naive_Event = Event.objects.get(pk=1)
    Hobart_Event = Event.objects.get(pk=2)
    Perth_Event = Event.objects.get(pk=3)
    
    result.append(["Naive", "Naive", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Naive", "Naive", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Naive", "Naive", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])

    Naive_Event = Event.objects.get(pk=4)
    Hobart_Event = Event.objects.get(pk=5)
    Perth_Event = Event.objects.get(pk=6)
    
    result.append(["Naive", "Hobart", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Naive", "Hobart", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Naive", "Hobart", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])
    
    Naive_Event = Event.objects.get(pk=7)
    Hobart_Event = Event.objects.get(pk=8)
    Perth_Event = Event.objects.get(pk=9)
    
    result.append(["Naive", "Perth", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Naive", "Perth", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Naive", "Perth", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])

    # Person reading events is in Hobart
    activate(TZ_Hobart)
    
    Naive_Event = Event.objects.get(pk=1)
    Hobart_Event = Event.objects.get(pk=2)
    Perth_Event = Event.objects.get(pk=3)
    
    result.append(["Hobart", "Naive", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Hobart", "Naive", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Hobart", "Naive", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])

    Naive_Event = Event.objects.get(pk=4)
    Hobart_Event = Event.objects.get(pk=5)
    Perth_Event = Event.objects.get(pk=6)
    
    result.append(["Hobart", "Hobart", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Hobart", "Hobart", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Hobart", "Hobart", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])
    
    Naive_Event = Event.objects.get(pk=7)
    Hobart_Event = Event.objects.get(pk=8)
    Perth_Event = Event.objects.get(pk=9)
    
    result.append(["Hobart", "Perth", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Hobart", "Perth", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Hobart", "Perth", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])

    # Person reading the events is in Perth
    activate(TZ_Perth)

    Naive_Event = Event.objects.get(pk=1)
    Hobart_Event = Event.objects.get(pk=2)
    Perth_Event = Event.objects.get(pk=3)
    
    result.append(["Perth", "Naive", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Perth", "Naive", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Perth", "Naive", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])

    Naive_Event = Event.objects.get(pk=4)
    Hobart_Event = Event.objects.get(pk=5)
    Perth_Event = Event.objects.get(pk=6)
    
    result.append(["Perth", "Hobart", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Perth", "Hobart", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Perth", "Hobart", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])
    
    Naive_Event = Event.objects.get(pk=7)
    Hobart_Event = Event.objects.get(pk=8)
    Perth_Event = Event.objects.get(pk=9)
    
    result.append(["Perth", "Perth", "Naive", str(Naive_Event.date_time), timezone.localtime(Naive_Event.date_time)])
    result.append(["Perth", "Perth", "Hobart", str(Hobart_Event.date_time), timezone.localtime(Hobart_Event.date_time)])
    result.append(["Perth", "Perth", "Perth", str(Perth_Event.date_time), timezone.localtime(Perth_Event.date_time)])
   
    table_result = Table(result)
    
    html = str(table_intro) + "<p>" + str(table_result)
    
    return HttpResponse(html)

from .celery import debug_task, debug_task2, instruction_queue_name, app as celery_app
from celery import app
from celery.result import AsyncResult
from celery.contrib.abortable import AbortableAsyncResult
from kombu import Connection, Exchange, Queue
def CeleryTestView(request):
    '''
    A simple view to test Celery task creation. The task shoudl be defined in tasks.py in same directory as this
    file.
    '''
#     result = debug_task()

    def on_message(body):
        print(body)
    
    print("About to start debug task")
    r = debug_task.apply_async()
    print("Kick started the debug task")
    print(r.get(on_message=on_message, propagate=False))
    print("Done.")
    
    return HttpResponse("Cool")

class CeleryTestView2(TemplateView):
    template_name = 'celery_test.html'

        
def Start_Cancel_Or_GetProgress(request):
    # The celery-progress Javascript collpases initProgressBar and and updateProgress into oen idea
    # That struck me as odd, as we want to kick start a process as distinct from asking what its 
    # progress is. But it occurs to me that if we keep a record of running processes somehow, then 
    # we know if the process is already kick started or not, and this can fall onto the shoulders of
    # one function.
    #
    # Celery of course knows what ptasks are running and we could ask it if this task is running, so
    # we'd only need some waht of comparig a requested task with a running task.
    
    # True if Id is in the get params and the Id is a running task!
    task_id = getattr(request,"GET", {}).get("task_id", None)

    print(f'\nStart_Cancel_Or_GetProgress: task_id is {task_id}')

    if task_id:
        print(f"Fetching id from session")
        task_id = request.session["task_id"]
        print(f"Fetching tasks result for task: {task_id}")
        
        r =  AsyncResult(task_id)
        print(f"Got result: {r.state}, {r.info}")

        # Experiment with Extended Results
#         extended_results = ('name', 'args', 'kwargs', 'worker', 'retries', 'queue', 'delivery_info')
#         setting = "result_extended"
#         print(f"DEBUG: {setting} = {r.app.conf[setting]}")
#         for er in extended_results:
#             print(f"\tExtended Result: {er}: {getattr(r, er, 'not set')}")

        # r.state is a string and constrained to be:
        # "PENDING" - The task is waiting for execution.
        # "STARTED" - The task has been started.
        # "RETRY" - The task is to be retried, possibly because of failure.
        # "FAILURE" - The task raised an exception, or has exceeded the retry limit.  The result attribute then contains the exception raised by the task.
        # "SUCCESS" - The task executed successfully. The result attribute then contains the tasks return value.
        #
        # Custom states introduced here:
        # "PROGRESS" - The task is running
        # "ABORTED" - The task was cancelled 
        #
        # See: https://docs.celeryproject.org/en/latest/reference/celery.result.html
        #      https://www.distributedpython.com/2018/09/28/celery-task-states/      

        abort = "cancel" in getattr(request,"GET", {})
        if abort:
            #celery_app.control.broadcast('abort', arguments= {'task_id': task_id})
            print(f'Connecting to: {celery_app.conf.broker_write_url}')
            with Connection(celery_app.conf.broker_write_url) as conn:
                q = conn.SimpleQueue(instruction_queue_name(task_id))
                instruction = 'abort'
                q.put(instruction)
                print(f'Sent: {instruction} to {instruction_queue_name(task_id)}')

#                 try:                
#                     print(f'Checking queue for already sent {instruction}: queue: {instruction_queue_name(task_id)}')
#                     last_instruction = q.get_nowait().payload
#                     # We never ack() the message we read, we leave it on the queue!
#                     # ack() removes it from the queue, only task should do that, we're
#                     # just sniffing it now so we don't queue the same instruction multiple 
#                     # times  
#                 except q.Empty:
#                     last_instruction = None
#                     
#                 print(f'Last instruction: {last_instruction}')
#                 if not last_instruction == instruction:
#                     q.put(instruction)
#                     print(f'Sent: {instruction} to {instruction_queue_name(task_id)}')
                    
                q.close()

        state = r.state
        if (state == "PENDING"):
            # We get this back if celery wasn't running nay workers for example
            # So we need to sensibly provide feedback here if that happens. Easy
            # to test as we just run tthe web site but not celery and start the task
            # it will come back as pending. 
            # TODO: implement default handling in progress.js 
            print("Task is PENDING")
            progress = {'percent':0, 'current':0, 'total':0, 'description': ""}
            result = None
        elif (state == "PROGRESS"):
            progress = r.info['progress']
            result = None
        elif (state == "ABORTED"):
            progress = r.info['progress']
            result = r.info['result'] 
        elif (state == "SUCCESS"):
            progress = {'percent':100, 'current':100, 'total':100, 'description': "Done!"}
            result = r.result
            
        print(f"RESULT: {result}")
    else:
        print("About to start debug task")
        async_result = debug_task2.delay()
        print("Kick started the debug task")
        task_id = async_result.task_id
        print(f"Got task id: {task_id}")
        request.session["task_id"] = task_id 
        print(f"Stowed id in session")
        state = "STARTED"
        progress = {'percent':0, 'current':0, 'total':0, 'description': ""}
        result = None

    # ProgressBar expects:
    # id - if the task is running
    # progress - a dict containing percent, current, total and description
    # complete - a bool, true when done
    # success - a bool, false on error, else true
    # cancelled - a bool, true when cancelled  
    # result - the result of the tast if complete and success
    
    if (state == "STARTED" or state == "PROGRESS"):        
        response = {'id': task_id, 'progress': progress}
    elif (state == "ABORTED"):
        response = {'id': task_id, 'canceled': True, 'result': result}
    elif (state == "PENDING"):
        response = {'id': task_id, 'result': 'PENDING', 'complete': True, 'success': False}
    else:
        response = {'id': task_id, 'result': str(result), 'complete': True, 'success': True}
     
    return HttpResponse(json.dumps(response))
