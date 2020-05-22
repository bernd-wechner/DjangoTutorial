######################################################################
# The task we are monitoring can divide its work into stages
# and for each stage run the progress bar from 0 to 100%.
#
# The client side pulse checker (javascript) can either show two 
# progress bars one for the stages and one for the steps in the 
# current stage or just one with the stage conveyed with text.  

default_steps = 100
default_stages = 1

class Progress:
    task = None
    
    # The task can divide progress in stages and steps
    stages = default_stages
    stage = 0
    
    steps = default_steps
    step = 0
    
    # Current status fo task
    status = ''  
    
    # The result, or if stage<stages and step<steps an interim result of course (as it's not done yet)
    result = ''  
    
    def __init__(self, task, steps=default_steps, stages=default_stages):
        self.task = task
        self.steps = steps
        self.stages = stages
        
    def configure(self, steps=default_steps, stages=default_stages):
        self.steps = steps
        self.stages = stages

    def as_dict(self):
        d = { 'steps': self.steps,
              'step': self.step,
              'status': self.status,
              'result': self.result,
            }
        
        if self.stages and self.stages > 1:
            d.update({'stages': self.stages, 'stage': self.stage})
            
        return d

    def update(self, step, stage=None, status=None, result=None):
        self.step = step
        
        if stage and self.stages:
            self.stage = stage

        if status:
            self.status = status
            
        if result:
            self.result = result
        elif status:
            self.result = status

    def done(self, status=None, result=None):
        self.step = self.steps
        self.stage = self.stages
        
        if status:
            self.status = status
            
        if result:
            self.result = result
        elif status:
            self.result = status
    
    def send(self, check_for_abort=True):
        '''
        Sends a simple progress report back to the Client.
        '''
        self.task.send_update(state="PROGRESS", meta={'progress': self.as_dict()})
        if check_for_abort:
            self.task.check_for_abort()
        
    def send_update(self, step, stage=None, description=None, result=None, check_for_abort=True):
        self.update(step, stage, description, result)
        self.send(check_for_abort)
