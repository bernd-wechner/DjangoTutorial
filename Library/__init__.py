# This will make sure the app is always imported when
# Django starts so that shared_task will use this app.
from .celery import app as celery_app

# By default celery_app is made available to "import *"
#    https://stackoverflow.com/questions/44834/can-someone-explain-all-in-python
# We can add it to __all__ instead which if defined defines the things imported
# when this package is imported.
#
#__all__ = ('celery_app',)