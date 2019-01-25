from django.urls import include, path
from django.contrib import admin
from Library.views import AuthorDetailView, AuthorListView, BookDetailView, BookUpdateView, BookListView, ArticleDetailView, ArticleListView
from Library.forms import AuthorCreate, AuthorDelete, AuthorUpdate, BookCreate, BookDelete, manage_books  

urlpatterns = [
    path('author/add/', AuthorCreate.as_view(), name='author-add'),
    path('author/edit/<int:pk>/', AuthorUpdate.as_view(), name='author-update'),
    path('author/<int:pk>/delete/', AuthorDelete.as_view(), name='author-delete'),
    path('author/<int:pk>/', AuthorDetailView.as_view(), name='author-detail'),
    path('author/', AuthorListView.as_view(), name='author-list'),
    
    path('author/books/<int:author_id>/', manage_books, name='author-books'),

    path('book/add/', BookCreate.as_view(), name='book-add'),
    path('book/edit/<int:pk>/', BookUpdateView.as_view(), name='book-update'),
    path('book/<int:pk>/delete/', BookDelete.as_view(), name='book-delete'),
    path('book/<int:pk>/', BookDetailView.as_view(), name='book-detail'),
    path('book/', BookListView.as_view(), name='book-list'),

    
    path('article/<slug:slug>/', ArticleDetailView.as_view(), name='article-detail'),
    path('article/', ArticleListView.as_view(), name='article-list'),
        
    path(r'admin/', admin.site.urls),   
]
