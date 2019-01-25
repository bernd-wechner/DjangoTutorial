from django.views.generic import DetailView, ListView, UpdateView
from django.utils import timezone
from django.forms import ModelForm,inlineformset_factory
from Library.models import Author, Book, Chapter, Article
    
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
        sql = str(qs.query)
        raw_qs = self.model.objects.raw(sql)
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