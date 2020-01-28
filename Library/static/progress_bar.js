'use strict';

// A simple progress bar that was pilfered shamelessly from:
//
// https://github.com/czue/celery-progress/blob/master/celery_progress/static/celery_progress/celery_progress.js
//
// And described here:
//
// https://buildwithdjango.com/projects/celery-progress/


class Binder {}

Binder.getAllMethods = function(instance, cls) {
  return Object.getOwnPropertyNames(Object.getPrototypeOf(instance))
    .filter(name => {
      let method = instance[name];
      return !(!(method instanceof Function) || method === cls);
    });
}

Binder.bind = function(instance, cls) {
	Binder.getAllMethods(instance, cls)
	.forEach(mtd => {
	  instance[mtd] = instance[mtd].bind(instance);
	})
}

class ProgressBar {
    onProgressDefault(progressBarElement, progressBarMessageElement, progress) {
        progressBarElement.style.backgroundColor = '#68a9ef';
        progressBarElement.style.width = progress.percent + "%";
        const description = progress.description || "";
        progressBarMessageElement.innerHTML = progress.current + ' of ' + progress.total + ' processed. ' + description;
    }

    onCancelDefault(progressBarElement, progressBarMessageElement) {
        progressBarElement.style.backgroundColor = '#76ce60';
        progressBarMessageElement.innerHTML = "Canceled!";
    }

    onSuccessDefault(progressBarElement, progressBarMessageElement) {
        progressBarElement.style.backgroundColor = '#76ce60';
        progressBarMessageElement.innerHTML = "Success!";
    }

    onErrorDefault(progressBarElement, progressBarMessageElement) {
        progressBarElement.style.backgroundColor = '#dc4f63';
        progressBarMessageElement.innerHTML = "Uh-Oh, something went wrong!";
    }

    onResultDefault(resultElement, result) {
        if (resultElement) {
            resultElement.innerHTML = result;
        }
    }

    constructor(progressUrl, options) {
    	this.progressUrl = progressUrl;
    	
    	// We have no ID yet, until we start the task
        this.taskId = null;				        
    	
        options = options || {};        

        this.progressBarId = options.progressBarId || 'progress-bar';
        this.progressBarMessage = options.progressBarMessageId || 'progress-bar-message';
        this.resultElementId = options.resultElementId || 'celery-result';

        this.progressBarElement        = options.progressBarElement        || document.getElementById(this.progressBarId);
        this.progressBarMessageElement = options.progressBarMessageElement || document.getElementById(this.progressBarMessage);
        this.resultElement             = options.resultElement             || document.getElementById(this.resultElementId);
        
        this.onProgress = options.onProgress || this.onProgressDefault;
        this.onCancel   = options.onCancel   || this.onCancelDefault;
        this.onSuccess  = options.onSuccess  || this.onSuccessDefault;
        this.onError    = options.onError    || this.onErrorDefault;
        this.onResult   = options.onResult   || this.onResultDefault;
        
        this.pollInterval = options.pollInterval || 500;
        
        Binder.bind(this, ProgressBar)
    }
    
    start() { this.cancelTask = false; this.pollURL(); }
    cancel() { this.cancelTask = true; }

    got_data(data) {
    	// If the AJAX call to taskProgressUrl returns an id, remember it
    	if (data.id) this.taskId = data.id;

    	if (data.progress) {
            this.onProgress(this.progressBarElement, this.progressBarMessageElement, data.progress);
        }
    	
        if (data.complete || data.canceled) {
            if (data.canceled)
            	this.onCancel(this.progressBarElement, this.progressBarMessageElement);
            else if (data.success)
            	this.onSuccess(this.progressBarElement, this.progressBarMessageElement);
            else
            	this.onError(this.progressBarElement, this.progressBarMessageElement);

            // reset the task ID, so that if we call start we are in fact starting a new task
            this.taskId = null;
            
            if (data.result)
            	this.onResult(this.resultElement, data.result);
        } else {
        	// setTimeout is vanilla JS and runs updateProgress after pollInterval
        	// setTimeout(function, milliseconds, param1, param2, ...)
        	// where param's are passed to function
        	// This recurses of course but setTiemout schedules the call to
        	// updateProgress in the global context later, so doesn't add to
        	// the stack.
            setTimeout(this.pollURL, this.pollInterval);
        }
    }

    got_response(response) {
    	console.log(response);
        response.json().then(this.got_data);
    }

    pollURL() {
        // fetch is vanilla JS returning a promise
        // .then defined the function called when the promise is fulfilled
        // The arrow function can be used todefine thet function too, so
        //   fetch(progressUrl).then(function(response) {
        // should also work as:
        //   fetch(progressUrl).then(response => {
        
        // response should be a a JSON dict with elements progress, complete, success and/or result
        // response.progress is itself a dict with three elements current, total, percent and optionally 
        // description.
        
        // To track progress we want to pass the task ID to the server and if we've received 
        // a cancel request then we should pass it on to the server.
        const taskProgressUrl = this.progressUrl 
        			          + (this.taskId     ? "?task_id=" + this.taskId : "") 
        			          + (this.cancelTask ? "&cancel" : "");
        			          
        // Cancel the task only once (i.e. if this.cancelTask was true we now have
        // cancel in the URL, but rest this.CancelTask so we don't keep requesting 
        // cancel on each poll, rather, wait for the server to wind up and report 
        // it cancelled the task. 
        this.cancelTask = false;  
        
        fetch(taskProgressUrl).then(this.got_response);        
    }
};