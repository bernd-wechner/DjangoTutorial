from django.views.generic import DetailView, ListView, UpdateView
from django.utils import timezone
from django.forms import inlineformset_factory
from django.db import connection
from Library.models import Author, Book, Chapter, Article

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

class BookUpdateView(UpdateView):

    model = Book
    fields = '__all__' 
    paginate_by = 100  # if pagination is desired

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['now'] = timezone.now()
        context['chapters'] = inlineformset_factory(Book, Chapter, fields="__all__")
        return context
    
class BookListView(ListView):

    model = Book
    paginate_by = 100  # if pagination is desired
    
    def get_queryset(self, *args, **kwargs):
        qs = self.model.objects.filter(title='My Life')
        
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