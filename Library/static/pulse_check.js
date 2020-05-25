'use strict';
/* A Pulse Checker for Celery Interactive tasks.
 * 
 * Instantiate PulseChecker with an AJAX URL that must respond with a
 * A JSON data structure bebiug the pulse of the task.
 * 
 * Based on a simple progress bar that was pilfered shamelessly from:
 * https://github.com/czue/celery-progress/blob/master/celery_progress/static/celery_progress/celery_progress.js
 * 
 * And heavily modified to handle more general pulse checking on a Celery tasks's status.
 * 
 */
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
    constructor(URL, options) {
    	this.URL = URL;
    	
        options = options || {};        

        this.taskId 		  = options.taskId   || null;				        
		this.stepBar          = options.stepBar  || false;
		this.stageBar         = options.stageBar || false;
		
		// If true status text will be prefixed with a default progress string.  
		this.prefixStatus     = options.prefixStatus     || false;

		// If true requests we hand confirmations here, else we let the server decide.
		//
		// If we wish to own confirmations we expect an AJAX response that 
		// supplies a positive_lbl, negative_lbl, positive_URL and negative_URL
		// so we can create postive and negative buttons.  
		this.ownConfirmations = options.ownConfirmations || false;

        this.stepProgressElementId  = options.stepProgressElementId   || 'pulse-check-step-bar';
        this.stageProgressElementId = options.stageProgressElementId  || 'pulse-check-stage-bar';
        this.statusElementId        = options.statusElementId         || 'pulse-check-status';
        this.resultElementId        = options.resultElementId         || 'pulse-check-result';

        this.stepProgressLabelId  = options.progressLabelId  || 'pulse-check-step-label';
        this.stageProgressLabelId = options.progressLabelId  || 'pulse-check-stage-label';
        this.statusLabelId        = options.statusLabelId    || 'pulse-check-status-label';
        this.resultLabelId        = options.resultLabelId    || 'pulse-check-result-label';

        this.stepProgressElement  = options.stepProgressElement  || document.getElementById(this.stepProgressElementId);
        this.stageProgressElement = options.stageProgressElement || document.getElementById(this.stageProgressElementId);
        this.statusElement        = options.statusElement        || document.getElementById(this.statusElementId);
        this.resultElement        = options.resultElement        || document.getElementById(this.resultElementId);

        this.stepProgressLabel = options.stepProgressLabel   || document.getElementById(this.stepProgressLabelId);
        this.stageProgressLabel = options.stageProgressLabel || document.getElementById(this.stageProgressLabelId);
        this.statusLabel        = options.statusLabel        || document.getElementById(this.statusLabelId);
        this.resultLabel        = options.resultLabel        || document.getElementById(this.resultLabelId);
        
        this.onProgress = options.onProgress || this.onProgressDefault;
        this.onWait     = options.onWait     || this.onWaitDefault;
        this.onCancel   = options.onCancel   || this.onCancelDefault;
        this.onSuccess  = options.onSuccess  || this.onSuccessDefault;
        this.onError    = options.onError    || this.onErrorDefault;
        
        this.pollInterval = options.pollInterval || 500;
        
        this.timerID = null;
        
      	// A sort of defacto, standard on-line for flagging an AJAX request is to set
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
    	this.statusElement.innerHTML = ""; 
    	this.resultElement.innerHTML = ""; 
    	this.pollURL(); 
    }
    
	/**********************************************************************************************
	 * AJAX result fetcher
	 **********************************************************************************************/    

    pollURL() {
        // fetch is vanilla JS returning a promise
        // .then defined the function called when the promise is fulfilled
        // The arrow function can be used todefine thet function too, so
        //   fetch(URL).then(function(response) {
        // should also work as:
        //   fetch(URL).then(response => {
        
        // response should be a a JSON dict with elements:
    	// 		progress, 
    	// 		complete,
    	//		canceled,
    	//	    failed,
    	//		instructed,
    	//		waiting,
    	//		confirm,
    	//
        // progress is itself a dict with elements 
    	// 		step, 
    	//		steps, 
    	//		stage,		optional
    	//		stages,		optional
        // 		status.		optional
    	//		result      optional  ... an interim result
    	//
    	// TODO: move all of 
    	//       complete, canceled, instructed, waiting, confirm, success
    	//	to progress.status
    	//
    	// move cancelTast to instructiom=cancel
        
        // To track progress we want to pass the task ID to the server and if we've received 
        // a cancel request or a more general instruction then we should pass that on to the 
    	// server as well.
        const PulseCheckUrl = this.URL 
        			        + (this.taskId            ? "?task_id=" + this.taskId : "") 
        			        + (this.ownConfirmations  ? "&ajax_confirmations" : "") 
        			        + (this.cancelTask        ? "&cancel" : "")
        			        + (this.instruction       ? "&instruction=" + this.instruction : "");
        			          
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

	/**********************************************************************************************
	 * AJAX Response handlers
	 **********************************************************************************************/    
    
    got_response(response) {
        response.json().then(this.got_data);
    }

    got_data(data) {
    	console.log("Got Data Back: " + JSON.stringify(data));
    	
    	// If the AJAX call to taskPulseCheckerUrl returns an id, remember it
    	if (data.id) this.taskId = data.id;

    	const elements = {'step':   this.stepProgressElement, 
    					  'stage':  this.stageProgressElement,
    					  'status': this.statusElement,
    					  'result': this.resultElement};
    	
    	const labels   = {'step':   this.stepProgressLabel, 
				  		  'stage':  this.stageProgressLabel,
				  		  'status': this.statusLabel,
				  		  'result': this.resultLabel};

        if (this.taskId) {
	        if (data.request_page) {
		    	console.log("Redirecting to confirmation page with: " + data.request_page);
		    	
		    	const requestedURL = validURL(data.request_page);
		    	
		    	console.log("Was a URL requested? " + requestedURL);
		    	
		    	let URL = requestedURL ? data.request_page : (this.URL + "?"); 
		    	
		    	console.log("Base URL: " + URL);
		    	
		    	// If the URL ends in "?" we add the task_id at least 
		    	if (URL.charAt(URL.length-1) === "?") {
			        URL += "task_id=" + this.taskId;
			        
			        // If a response key was provided pass it back. 
			        // It's clumsy to package the whole response in
			        // GET params, so we let the target URL know with
			        // what key it can get the reponse in a keys store
			        // where it was stored by the server (that sent us
			        // this reponse_key)
			        if (data.response_key) 
				        URL += "&response_key=" + data.response_key;
			        else {
			        	// We can encode as much of the response as is reasonable
			        	// as GET parmas (more clumsy and less robust but can 
			        	// pass data to URLS that have no access to the keyed store
			        	// in which the response was -possibly- saved)
			        	for (let key in data) {
			        	    let value = data[key];
			        	    if (typeof variable === 'string' ||
			        	    	typeof variable === 'number' ||
			        	    	typeof variable === 'boolean')
						        URL += "&" + key + "=" + value;
			        	}
			        	// data.progress we know is a dict, so we can give
			        	// it special treatement 
			        	for (let key in data.progress) {
			        	    let value = data[key];
			        	    if (typeof variable === 'string' ||
			        	    	typeof variable === 'number' ||
			        	    	typeof variable === 'boolean')
						        URL += "&progress_" + key + "=" + value;
			        	}
			        }
			        
			        // Add request_page as a nominated templated if it wasn't a URL already.
			    	if (!requestedURL) URL += "&template=" + data.request_page
		    	}
		    	
	    		window.location.href = URL
	    	} 
	        else {
	        	// Update any porgress first
	        	// Important so that any other actions like Complete or Canceled
	        	// update last.
		    	if (data.progress && !(data.waiting && data.confirm)) {
		    		const percent = 100 * data.progress.step / data.progress.steps;
		        	console.log("Rendering Progress: " + percent);
		            this.onProgress(elements, labels, data.progress);
		        }
	        	
		        if (data.waiting) {
		        	// We should only be here if this.ownConfirmations is true
		        	// otherwise data.request_page should have arrived and already 
		        	// been dealt with (i.e. the server will manage the Confirmation
		        	// our role is complete. But if we land here we need to manage
		        	// the confirmation. We will have in the data:
		        	//      prompt
		        	// 		positive_lbl, positive_URL
		        	// 		negative_lbl, negative_URL
		        	// to help. We will need to have two buttons to dress up and display
		        	// TODO: implement this.
		        	// Ideal onw_Confirmations is not bool, but a list of two buttons by id 
		        	// or element. Of if just a bool use default names. 
		    		const result = data.progress.result || ""; 
			    	console.log("Waiting.");
		    		this.onWait(elements, labels, result, data.prompt);
		        }
		        
		        else if (data.complete || data.canceled || data.failed) {
		    		const result = data.progress.result || ""; 
		
		    		clearTimeout(this.timerID);
		        	
		        	console.log("Completed: " + data.complete + "  Canceled: " + data.canceled);
		            if (data.canceled)
		            	this.onCancel(elements, labels, result);
		            else if (data.failed)
		            	this.onError(elements, labels, result);
		            else
		            	this.onSuccess(elements, labels, result);
		
		            // reset the task ID, so that if we call start we are in fact starting a new task
		            this.taskId = null;
		        }
		        
		        else if (data.instructed) {
		        	clearTimeout(this.timerID);
		        	console.log("Instructed: " + data.instructed);
		        } 
		        
		        else {
		        	// setTimeout is vanilla JS and calls pollURL after pollInterval
		        	// setTimeout(function, milliseconds, param1, param2, ...)
		        	// where param's are passed to function
		        	// This recurses of course but setTiemout schedules the call to
		        	// pollURL in the global context later, so doesn't add to
		        	// the stack.
		            this.timerID = setTimeout(this.pollURL, this.pollInterval);
		        }	        	
	    	}
        }
    }
    
	/**********************************************************************************************
	 * Default Response handlers
	 **********************************************************************************************/    
    
	onProgressDefault(elements, labels, progress) {
    	if (this.stepBar && elements.step && progress.steps) {
    		const percent = 100 * progress.step / progress.steps;
    		elements.step.style.backgroundColor = '#68a9ef';
    		elements.step.style.width = percent + "%";
	    }

    	if (this.stageBar && elements.stage && progress.stages) {
    		const percent = 100 * progress.stage / progress.stages;
    		elements.stage.style.backgroundColor = '#68a9ef';
    		elements.stage.style.width = percent + "%";
	    }

	    if (elements.status) {
	    	const default_status = progress.step + ' of ' + progress.steps + ' processed' 
	    						 + (progress.stages>1 
	    								 ? (' in stage ' + progress.stage + ' of ' + progress.stages + '.') 
	    								 : '.');
	    								         
	        const status = this.prefixStatus && progress.status 
	        					? default_status + progress.status
	        					: progress.status 
	        						? progress.status
	        						: default_status;
	        
	        elements.status.innerHTML = status;
        }
        
        if (progress.result && elements.result) {
        	elements.result.innerHTML = progress.result;
        	if (labels.result) labels.result.style.display = progress.result ? 'Block' : 'None';  
        }
    }

    onCancelDefault(elements, labels, result) {
    	if (this.stepBar && elements.step) elements.step.style.backgroundColor = '#76ce60';
    	if (this.stageBar && elements.stage) elements.stage.style.backgroundColor = '#76ce60';
    	
        if (elements.status) elements.status.innerHTML = "Canceled!";
        
	    if (result && elements.result) {
	    	elements.result.innerHTML = result;
        	if (labels.result) labels.result.style.display = result ? 'Block' : 'None';  
        }
    }

    onWaitDefault(elements, labels, result, prompt) {
    	if (this.stepBar && elements.step) elements.step.style.backgroundColor = '#76ce60';
    	if (this.stageBar && elements.stage) elements.stage.style.backgroundColor = '#76ce60';
    	
        if (elements.status) elements.status.innerHTML = prompt ? prompt : "Waiting... ";
	    
	    if (result && elements.result) {
	    	elements.result.innerHTML = result;
        	if (labels.result) labels.result.style.display = result ? 'Block' : 'None';  
        }
    }

    onSuccessDefault(elements, labels, result) {
    	if (this.stepBar && elements.step) elements.step.style.backgroundColor = '#76ce60';
    	if (this.stageBar && elements.stage) elements.stage.style.backgroundColor = '#76ce60';
    	
        if (elements.status) elements.status.innerHTML = prompt ? prompt : "Success!";

	    if (result && elements.result) {
	    	elements.result.innerHTML = result;
        	if (labels.result) labels.result.style.display = result ? 'Block' : 'None';  
        }
    }

    onErrorDefault(elements, labels, errormessage) {
    	if (this.stepBar && elements.step) elements.step.style.backgroundColor = '#dc4f63';
    	if (this.stageBar && elements.stage) elements.stage.style.backgroundColor = '#dc4f63';
    	
        if (elements.status) elements.status.innerHTML = "Uh-Oh, something went wrong!";

	    if (errormessage && elements.result) {
	    	elements.result.innerHTML = errormessage;
        	if (labels.result) labels.result.style.display = result ? 'Block' : 'None';  
        }
    }

	/**********************************************************************************************
	 * Button event handlers
	 **********************************************************************************************/    

    // These requests will be sent when the next pollURL fires (next time pollInterval elapses)
    // if pollURL is called explicitly weird stuff happens because we set off another setTimeout
    // chain of call backs.
    cancel(c_element) { c_element.style.visibility = "hidden"; this.cancelTask = true; this.pollURL(); }
    instruct(i_element) { this.instruction = document.getElementById(i_element).value; this.pollURL(); }
};

function validURL(str) {
	  var pattern = new RegExp('^(https?:\\/\\/)'+ 			// protocol (required)
	    '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|'+ // domain name
	    '((\\d{1,3}\\.){3}\\d{1,3}))'+ 						// OR ip (v4) address
	    '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*'+ 					// port and path
	    '(\\?[;&a-z\\d%_.~+=-]*)?'+ 						// query string
	    '(\\#[-a-z\\d_]*)?$','i'); 							// fragment locator
	  return !!pattern.test(str);
	}