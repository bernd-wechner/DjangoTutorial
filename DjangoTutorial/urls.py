from django.urls import include, path
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from Library.views import AuthorDetailView, AuthorListView, BookDetailView, BookCreate, BookUpdateView, BookListView, ArticleDetailView, ArticleListView, TimeTestView, CeleryTestView, CeleryTestView2, Start_Cancel_Or_GetProgress
from Library.forms import AuthorCreate, AuthorDelete, AuthorUpdate, BookDelete, manage_books  

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

    path('timetest/', TimeTestView, name='time-test'),
    path('celerytest/', CeleryTestView, name='celery-test'),
    
    path('test/<task>/', CeleryTestView2.as_view(), name='task-test'),
    path('progress/<task>', Start_Cancel_Or_GetProgress, name='task-progress'),
    
    path('article/<slug:slug>/', ArticleDetailView.as_view(), name='article-detail'),
    path('article/', ArticleListView.as_view(), name='article-list'),
        
    path(r'admin/', admin.site.urls),   
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
