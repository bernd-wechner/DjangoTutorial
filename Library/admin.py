from django.contrib import admin
from django.apps import apps
from django.contrib.admin.sites import AlreadyRegistered

admin.site.site_title = "Django Tutorial"
admin.site.site_header = "Django Tutorial Administration"

# Registers all known models 
# (probably more than we want on a production site
# but awesome for development).
for model in apps.get_models():
    try:
        admin.site.register(model)
    except AlreadyRegistered:
        pass