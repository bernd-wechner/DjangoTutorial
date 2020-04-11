'use strict';

// A simple progress bar that was pilfered shamelessly from:
//
// https://github.com/czue/celery-progress/blob/master/celery_progress/static/celery_progress/celery_progress.js
//
// And described here:
//
// 	

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

class PulseChecker {
    onProgressDefault(elements, labels, progress) {
    	const progressElement = elements.length > 0 ? elements[0]: null;
    	const messageElement  = elements.length > 1 ? elements[1]: null;
    	const resultElement   = elements.length > 2 ? elements[2]: null;

    	const progressLabel = labels.length > 0 ? labels[0]: null;
    	const messageLabel  = labels.length > 1 ? labels[1]: null;
    	const resultLabel   = labels.length > 2 ? labels[2]: null;
    
    	if (this.progressBar && progressElement) {
	        progressElement.style.backgroundColor = '#68a9ef';
	        progressElement.style.width = progress.percent + "%";
	    }
	    
	    if (messageElement) {
	        const description = progress.description || "";
	        messageElement.innerHTML = progress.current + ' of ' + progress.total + ' processed. ' + description;
        }
        
        if (progress.result && resultElement) {
        	resultElement.innerHTML = progress.result;
        	if (resultLabel) resultLabel.style.display = progress.result ? 'Block' : 'None';  
        }
    }

    onCancelDefault(elements, labels, result) {
    	const progressElement = elements.length > 0 ? elements[0]: null;
    	const messageElement  = elements.length > 1 ? elements[1]: null;
    	const resultElement   = elements.length > 2 ? elements[2]: null;
    	
    	if (this.progressBar && progressElement) progressElement.style.backgroundColor = '#76ce60';
        if (messageElement) messageElement.innerHTML = "Canceled!";
	    if (result && resultElement) {
        	resultElement.innerHTML = result;
        	if (resultLabel) resultLabel.style.display = result ? 'Block' : 'None';  
        }
    }

    onWaitDefault(elements, labels, result, prompt) {
    	const progressElement = elements.length > 0 ? elements[0]: null;
    	const messageElement  = elements.length > 1 ? elements[1]: null;
    	const resultElement   = elements.length > 2 ? elements[2]: null;
    	
    	if (this.progressBar && progressElement) progressElement.style.backgroundColor = '#76ce60';
	    if (messageElement) messageElement.innerHTML = "Waiting... " + (prompt ? prompt : "");
	    if (result && resultElement) {
        	resultElement.innerHTML = result;
        	if (resultLabel) resultLabel.style.display = result ? 'Block' : 'None';  
        }
    }

    onSuccessDefault(elements, labels, result) {
    	const progressElement = elements.length > 0 ? elements[0]: null;
    	const messageElement  = elements.length > 1 ? elements[1]: null;
    	const resultElement   = elements.length > 2 ? elements[2]: null;
    	
    	if (this.progressBar && progressElement) progressElement.style.backgroundColor = '#76ce60';
	    if (messageElement) messageElement.innerHTML = "Success!";
	    if (result && resultElement) {
        	resultElement.innerHTML = result;
        	if (resultLabel) resultLabel.style.display = result ? 'Block' : 'None';  
        }
    }

    onErrorDefault(elements, labels, errormessage) {
    	const progressElement = elements.length > 0 ? elements[0]: null;
    	const messageElement  = elements.length > 1 ? elements[1]: null;
    	
    	if (this.progressBar) progressElement.style.backgroundColor = '#dc4f63';
		if (messageElement) messageElement.innerHTML = errormessage ? errormessage : "Uh-Oh, something went wrong!";
    }

    constructor(URL, options) {
    	this.URL = URL;
    	
        options = options || {};        

        this.taskId 		  = options.taskId           || null;				        
		this.progressBar      = options.progressBar      || false;

        this.progressElementId = options.progressElementId  || 'pulse-check-progress-bar';
        this.messageElementId  = options.messageElementId   || 'pulse-check-message';
        this.resultElementId   = options.resultElementId    || 'pulse-check-result';

        this.progressLabelId    = options.progressLabelId  || 'pulse-check-progress-label';
        this.messageLabelId     = options.messageLabelId   || 'pulse-check-message-label';
        this.resultLabelId      = options.resultLabelId    || 'pulse-check-result-label';

        this.progressElement = options.progressElement || document.getElementById(this.progressElementId);
        this.messageElement  = options.messageElement  || document.getElementById(this.messageElementId);
        this.resultElement   = options.resultElement    || document.getElementById(this.resultElementId);

        this.progressLabel = options.progressLabel || document.getElementById(this.progressLabelId);
        this.messageLabel  = options.messageLabel  || document.getElementById(this.messageLabelId);
        this.resultLabel   = options.resultLabel   || document.getElementById(this.resultLabelId);
        
        this.onProgress = options.onProgress || this.onProgressDefault;
        this.onWait     = options.onWait     || this.onWaitDefault;
        this.onCancel   = options.onCancel   || this.onCancelDefault;
        this.onSuccess  = options.onSuccess  || this.onSuccessDefault;
        this.onError    = options.onError    || this.onErrorDefault;
        
        this.pollInterval = options.pollInterval || 500;
        
        this.timerID = null;
        
      	// An sort of defacto standard on-line for flagging an AJAX request is to set
      	// the X-Requested-With header to XMLHttpRequest. The JS fetch implementation
      	// doesn't set it, though many JS libraries (like jQuery) do. So when fetching
      	// in JS we set it explictly, to let the server know this is an AJAX request 
      	// (and we expect a JSON repsonse not an HTML page).
      	//
      	// Any fetch call then like fetch(URL, this.isajax) will have the header set.
      	this.is_ajax = {headers: {'X-Requested-With': 'XMLHttpRequest'}};
        
        Binder.bind(this, PulseChecker)
    }
    
    start() { 
    	console.log("Starting Progress Bar ...");
    	this.cancelTask = false; 
    	this.instruction = null; 
    	this.resultElement.innerHTML = ""; 
    	this.messageElement.innerHTML = ""; 
    	this.pollURL(); 
    }
    
    // These requests will be sent when the next pollURL fires (next time pollInterval elapses)
    // if pollURL is called explicitly weird stuff happens because we set off another setTimeout
    // chain of call backs.
    cancel(c_element) { c_element.style.visibility = "hidden"; this.cancelTask = true; this.pollURL(); }
    instruct(i_element) { this.instruction = document.getElementById(i_element).value; this.pollURL(); }

    got_data(data) {
    	console.log("Got Data Back: " + JSON.stringify(data));
    	
    	// If the AJAX call to taskPulseCheckerUrl returns an id, remember it
    	if (data.id) this.taskId = data.id;

    	const elements = [this.progressElement, this.messageElement, this.resultElement];
    	const labels   = [this.progressLabel,   this.messageLabel,   this.resultLabel];

        if (data.notify) {
    		window.location.href = this.URL + "?notify&task_id=" + this.taskId;
        }
        else if (data.waiting) {
        	if (data.confirm && this.taskId) {
    	    	console.log("Redirecting to confirmation page.");
        		window.location.href = this.URL + "?confirm&task_id=" + this.taskId;
        	} else {
    	    	console.log("Waiting.");
        		this.onWait(elements, labels, data.result, data.prompt);
        	}
        }
        else if (data.complete || data.canceled) {
        	clearTimeout(this.timerID);
        	
        	console.log("Completed: " + data.complete + "  Canceled: " + data.canceled);
            if (data.canceled)
            	this.onCancel(elements, labels, data.result);
            else if (data.success)
            	this.onSuccess(elements, labels, data.result);
            else
            	this.onError(elements, labels, data.result);

            // reset the task ID, so that if we call start we are in fact starting a new task
            this.taskId = null;
        }
        else if (data.instructed) {
        	clearTimeout(this.timerID);
        	console.log("Instructed: " + data.instructed);
        } else {
        	// setTimeout is vanilla JS and calls pollURL after pollInterval
        	// setTimeout(function, milliseconds, param1, param2, ...)
        	// where param's are passed to function
        	// This recurses of course but setTiemout schedules the call to
        	// pollURL in the global context later, so doesn't add to
        	// the stack.
            this.timerID = setTimeout(this.pollURL, this.pollInterval);
        }
        
    	if (data.progress && !(data.waiting && data.confirm)) {
        	console.log("Rendering Progress: " + data.progress.percent);
        	
            this.onProgress(elements, labels, data.progress);
        }
    }

    got_response(response) {
        response.json().then(this.got_data);
    }

    pollURL() {
        // fetch is vanilla JS returning a promise
        // .then defined the function called when the promise is fulfilled
        // The arrow function can be used todefine thet function too, so
        //   fetch(URL).then(function(response) {
        // should also work as:
        //   fetch(URL).then(response => {
        
        // response should be a a JSON dict with elements progress, complete, success and/or result
        // response.progress is itself a dict with three elements current, total, percent and optionally 
        // description.
        
        // To track progress we want to pass the task ID to the server and if we've received 
        // a cancel request then we should pass it on to the server.
        const PulseCheckUrl = this.URL 
        			          + (this.taskId     ? "?task_id=" + this.taskId : "") 
        			          + (this.cancelTask ? "&cancel" : "")
        			          + (this.instruction ? "&instruction=" + this.instruction : "");
        			          
        // Cancel the task only once (i.e. if this.cancelTask was true we now have
        // cancel in the URL, but rest this.CancelTask so we don't keep requesting 
        // cancel on each poll, rather, wait for the server to wind up and report 
        // it cancelled the task. 
        this.cancelTask = false;  
        
        // Send an instruction only once if suppplied.
        this.instruction = null
  
      	console.log("Fetching: " + PulseCheckUrl);
      
        fetch(PulseCheckUrl, this.is_ajax).then(this.got_response);
    }
};