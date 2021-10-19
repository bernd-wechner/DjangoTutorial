from django.urls import path
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from Library.views import AuthorDetailView, AuthorListView, BookDetailView, BookCreate, TransactionManaged_BookCreate, BookUpdateView, BookListView, ArticleDetailView, ArticleListView, TimeTestView, CeleryTestView, CeleryInteractiveTestView
from Library.forms import AuthorCreate, AuthorDelete, AuthorUpdate, BookDelete, manage_books
from celery_interactive import Interactive

interactive = Interactive()

urlpatterns = [
    path('author/add/', AuthorCreate.as_view(), name='author-add'),
    path('author/edit/<int:pk>/', AuthorUpdate.as_view(), name='author-update'),
    path('author/<int:pk>/delete/', AuthorDelete.as_view(), name='author-delete'),
    path('author/<int:pk>/', AuthorDetailView.as_view(), name='author-detail'),
    path('author/', AuthorListView.as_view(), name='author-list'),
    
    path('author/books/<int:author_id>', manage_books, {'type': ''}, name='author-books'),
    path('author/leadbooks/<int:author_id>', manage_books, {'type': 'lead'}, name='author-leadbooks'),

    path('book/add/', BookCreate.as_view(), name='book-add'),
    path('book/edit/<int:pk>/', BookUpdateView.as_view(), name='book-update'),
    path('book/<int:pk>/delete/', BookDelete.as_view(), name='book-delete'),
    path('book/<int:pk>/', BookDetailView.as_view(), name='book-detail'),
    path('book/', BookListView.as_view(), name='book-list'),

    # Transaction Managed URLs
    path('book/ADD/', TransactionManaged_BookCreate.as_view(), name='tm-book-add'),
    path('book/ADD/save', TransactionManaged_BookCreate.form_commit, name='tm-book-add-save'),
    path('book/ADD/save/pulse', interactive.django.pulse_check, {'task_name': 'add_book'}, name='tm-book-add-save-pulse'),

    path('timetest/', TimeTestView, name='time-test'),
    
    path('celerytest/', CeleryTestView, name='celery-test'),
    
    path('test/<task_name>/', CeleryInteractiveTestView.as_view(), name='task-test'),
    path('tell/<task_name>/', interactive.Django.instruct, name='task-tell'),
    path('pulse/<task_name>', interactive.Django.pulse_check, name='task-pulse'),
    
    path('article/<slug:slug>/', ArticleDetailView.as_view(), name='article-detail'),
    path('article/', ArticleListView.as_view(), name='article-list'),
        
    # Admin site by default (no home page here)
    path(r'', admin.site.urls),   
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
