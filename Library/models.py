from django.db import models
from django.utils import timezone
#from django_model_admin_fields import AdminModel
from django.core.exceptions import ValidationError, NON_FIELD_ERRORS

# Note: I am trying to replicate this structure I have in my CoGs project:
#
# Session: a rich object so to speak with
#            one to many relationship to Performance
#            one to many relationship to Rank
# Performance: has ForeignKey to a Session and to a Player
# Rank: has ForeignKey to a Session and to Player and Team 
# Team: has on to many relationship to Rank
# Player: has one to many relationship to Performance and Rank and Team
#
# In this Tutorial context I want to examine a number of things in a much simplified analog.
# The aim is to learn how things work and also to have a code base to share in support questions,
# that is minimal, exemplary and draws on stock tutorial examples. 
#
# Of interest to date:
#
# 1) how a one to many relationship saves  when it's part of a rich object. I have yet to 
#    strike a firm analogy between the tutorial models and the CoGS models but have learned
#    a lot playing around with the tutorial models below anyhow.
#
#    An anttractive analogy involved Chapters below (let -> indicate ForeignKey To):
#
#    If Author is analagous to Session
#    Chapter is analagous to Performance (Performance->Session, Chapter->Author)  
#    Book is analagous to Player (Performance->Player, Chapter->Book)
#
#    Creating a CoGs Session then is akin to creating an Author if I build a rich
#    form in which we can Add an Author, with Books and and Chapters (which exactly
#    what I have in CoGs, the Session form defines a Session with Players and Performances. 
#
# 2) I found an apparent bug the Django 2.0 Query object. In essence what I need to explore is this:
#    qs = Model.objects.filter(texfield=texvalue)
#    print(len(qs))
#    sql = str(qs.query)
#    raw_qs = Model.objects.raw(sql)
#    print(len(raw_qs))
#
#    Should get the same result but I'm finding the last line crashes with SQL error, because the WHERE
#    clause in sql does not quote textvalue. Yet qs works and to the SQL that is actuallye xecuted, by 
#    inference does quote textvalue in the WHERE clause but the str(qs.query) does not! An apparent bug 
#    in the __str__ function of qs.query!
#
#    The aim here is in a very simple context to replicate this and confirm the failed behaviour.
#
#    I already have some seed data for books, and I have a a view for book lists, and book has 
#    text field so I can add a filter to the books list to check this. We can do this by overriding
#    get_queryset(self, *args, **kwargs) in the view.


# The Newspaper Article Library example

class Reporter(models.Model):
    full_name = models.CharField(max_length=70)

    def __str__(self):
        return self.full_name

class Article(models.Model):
    pub_date = models.DateField()
    headline = models.CharField(max_length=200)
    content = models.TextField()
    reporter = models.ForeignKey(Reporter, on_delete=models.CASCADE)

    def __str__(self):
        return self.headline

# The Book Library example  

TITLE_CHOICES = (
    ('MR', 'Mr.'),
    ('MRS', 'Mrs.'),
    ('MS', 'Ms.'),
)   

class Author(models.Model):
    name = models.CharField(max_length=100)
    title = models.CharField(max_length=3, choices=TITLE_CHOICES)
    birth_date = models.DateField(blank=True, null=True)

    def __str__(self):
        return self.name

class Book(models.Model):
    title = models.CharField(max_length=100) 
    authors = models.ManyToManyField(Author, related_name="books")
    lead_author = models.ForeignKey(Author, on_delete=models.CASCADE)
    
    def clean(self):
        if (self.id is None):
            # Validate all the model fields that are not One2Many or Many2Many
            pass
        else:
            # Validate all the One2Many or Many2Many fields
            errors = {"title": ["Title is bad", "No not really"], "authors": ["But authors are", "Nah, kidding you again"], NON_FIELD_ERRORS: ["One error", "Two errors"]}
            raise ValidationError(errors)            

    def __str__(self):
        return self.title
    
    class Meta():
        ordering = ['pk']    

class Chapter(models.Model):
    title = models.CharField(max_length=100)
    book = models.ForeignKey(Book, on_delete=models.CASCADE, related_name="chapters")
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="chapters")

    def __str__(self):
        return self.title

class Event(models.Model):
    title = models.CharField(max_length=100)
    venue = models.CharField(max_length=100)
    creator = models.CharField(max_length=100)
    date_time = models.DateTimeField(default=timezone.now)
    
    def __str__(self):
        return self.title
