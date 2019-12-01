from django import forms
from django.shortcuts import render
from django.forms import ModelForm,inlineformset_factory
from django.http.response import HttpResponseRedirect
from django.core.exceptions import NON_FIELD_ERRORS
from django.views.generic.edit import CreateView, DeleteView, UpdateView
from django.urls import reverse, reverse_lazy

from Library.models import Author, Book, Article, TITLE_CHOICES

class AuthorCreate(CreateView):
    model = Author
    fields = '__all__'

    def get_success_url(self):
        return reverse('author-detail', kwargs={'pk': self.object.pk})

class AuthorUpdate(UpdateView):
    model = Author
    fields = '__all__'

class AuthorDelete(DeleteView):
    model = Author
    success_url = reverse_lazy('author-list')


# class BookCreate(CreateView):
#     model = Book
#     fields = '__all__'
# 
#     def get_success_url(self):
#         return reverse('book-detail', kwargs={'pk': self.object.pk})

# class BookUpdate(UpdateView):
#     model = Book
#     fields = '__all__'

class BookDelete(DeleteView):
    model = Book
    success_url = reverse_lazy('book-list')


class ArticleCreate(CreateView):
    model = Article
    fields = '__all__'

class ArticleUpdate(UpdateView):
    model = Article
    fields = '__all__'

class ArticleDelete(DeleteView):
    model = Article
    success_url = reverse_lazy('article-list')

class ArticleForm(ModelForm):
    class Meta:
        error_messages = {
            NON_FIELD_ERRORS: {
                'unique_together': "%(model_name)s's %(field_labels)s are not unique.",
            }
        }
     
def manage_books(request, author_id, type):
    '''
    Based on the manage_books function in the tutorial at: 
        https://docs.djangoproject.com/en/2.1/topics/forms/modelforms/#using-an-inline-formset-in-a-view
    '''
    author = Author.objects.get(pk=author_id)

    print("{} {}: checking parent before save {}={}".format("Author", author.pk, "books", author.books.all()))    

    if type == 'lead':    
        BookInlineFormSet = inlineformset_factory(Author, Book, fields="__all__")
    else:
        # Pydev marks Book.authors.through in red warning "Undefined variable from import: through"
        # But that seems faulty, this runs fine. TODO: What is wrong with pydev that it can't see the
        # through property yet I can break here, and examon Book.authors.through and see it exists and 
        # is ModelBase: <class 'Library.models.Book_authors'> 
        BookInlineFormSet = inlineformset_factory(Author, Book.authors.through, fields="__all__")
        
    if request.method == "POST":
        print("Submitted:")
        for (key, val) in request.POST.items():
            print("\t{}: {}".format(key, val))
                
        formset = BookInlineFormSet(request.POST, request.FILES, instance=author)
        if formset.is_valid():
            print("{} {}: checking parent before save {}={}".format("Author", author.pk, "books", author.books.all()))                    
            instances = formset.save()
            
            print("{} {}: checking parent after save {}={}".format("Author", author.pk, "books", author.books.all()))                    
            for instance in instances:
                print("{} {}: saved {}={}. It has {}={}".format("Author", author.pk, "Book", instance, "author", instance.author.pk))
                print("{} {}: checking parent of instance {}={}".format("Author", author.pk, "books", instance.author.books.all()))
            
            if (len(instances) == 0):
                    print("{} {}: did not save any {}s".format("Author", author.pk, "book"))
                    
            # Do something. Should generally end with a redirect. For example:
            return HttpResponseRedirect(reverse('author-detail', kwargs={'pk': author.pk}))
    else:
        formset = BookInlineFormSet(instance=author)
        
    return render(request, 'Library/manage_books.html', {'formset': formset})
