import re
from celery import current_app

def Task(task_name, task_id=None):
    '''
    Given the name of task will return an instance of a task of that name
    provided it's been registered with Celery and the name identifies a 
    unique task. Will populate Task.request.id with an id if supplied.
    
    This is useful for making registry fetched client side instances 
    look like worker side requested instances from the perspective of 
    internal methods that need a task id.
     
    :param task_name: The name of the task desired.
    :param task_id:   The id of the task (optional) 
    '''
    r = re.compile(fr"\.{task_name}$")
    task_fullnames = list(filter(r.search, list(current_app.tasks.keys())))
    assert len(task_fullnames) == 1, f"get_task provided with an ambiguous name. Provided: {task_name}, Found: {task_fullnames}."
    task = current_app.tasks[task_fullnames[0]]
    
    # If a task_id is provided, attach it to the task where it's normally stored
    # task.request is empty but exists in the current_app.tasks, and so we can set
    # the id in it without trouble.
    if task_id:
        task.request.id = task_id 

    return task
