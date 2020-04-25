import os, sys, logging
from celery.utils.log import get_task_logger

# For methods see:
#     https://docs.python.org/3.8/library/logging.html
logger = get_task_logger(__name__)

# A provisional hack to turn on debug logging in the PyDev debugger when 
# debugging the Django side. If this is done by the worker then there end 
# up being two log handlers pointing to the console and everything is doubled.
# In the Django context no log handlers are defined and the logs disappear 
# unless we attach one.
if not os.path.basename(sys.argv[0]) == "celery":
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.debug(f"Logger config for: {os.path.basename(sys.argv[0])}")

# A Debug flag to turn on debuugging output
debug = True

