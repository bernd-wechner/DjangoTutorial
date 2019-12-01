// A simple progress bar that was pilfered shamelessly from:
//
// https://github.com/czue/celery-progress/blob/master/celery_progress/static/celery_progress/celery_progress.js
//
// And described here:
//
// https://buildwithdjango.com/projects/celery-progress/

var ProgressBar = (function () {
    function onSuccessDefault(progressBarElement, progressBarMessageElement) {
        progressBarElement.style.backgroundColor = '#76ce60';
        progressBarMessageElement.innerHTML = "Success!";
    }

    function onResultDefault(resultElement, result) {
        if (resultElement) {
            resultElement.innerHTML = result;
        }
    }

    function onErrorDefault(progressBarElement, progressBarMessageElement) {
        progressBarElement.style.backgroundColor = '#dc4f63';
        progressBarMessageElement.innerHTML = "Uh-Oh, something went wrong!";
    }

    function onProgressDefault(progressBarElement, progressBarMessageElement, progress) {
        progressBarElement.style.backgroundColor = '#68a9ef';
        progressBarElement.style.width = progress.percent + "%";
        var description = progress.description || "";
        progressBarMessageElement.innerHTML = progress.current + ' of ' + progress.total + ' processed. ' + description;
    }

    function updateProgress (progressUrl, options) {
        options = options || {};
        const progressBarId = options.progressBarId || 'progress-bar';
        const progressBarMessage = options.progressBarMessageId || 'progress-bar-message';
        const progressBarElement = options.progressBarElement || document.getElementById(progressBarId);
        const progressBarMessageElement = options.progressBarMessageElement || document.getElementById(progressBarMessage);
        const onProgress = options.onProgress || onProgressDefault;
        const onSuccess = options.onSuccess || onSuccessDefault;
        const onError = options.onError || onErrorDefault;
        const pollInterval = options.pollInterval || 500;
        const resultElementId = options.resultElementId || 'celery-result';
        const resultElement = options.resultElement || document.getElementById(resultElementId);
        const onResult = options.onResult || onResultDefault;
        const taskId = options.id || null;

        // fetch is vanilla JS returning a promise
        // .then defined the function called when the promise is fulfilled
        // The arrow function can be used todefine thet function too, so
        //   fetch(progressUrl).then(function(response) {
        // should also work as:
        //   fetch(progressUrl).then(response => {
        //
        // But before I rewrite any of this I should test it as celery-progress provides it.
        
        // response should be a a JSON dict with elements progress, complete, success and/or result
        // response.progress is itself a dict with three elements current, total, percent and optionally 
        // description.
        
        taskProgressUrl = progressUrl + (taskId ? "?task_id=" + taskId : ""); 
        
        fetch(taskProgressUrl).then(function(response) {
            response.json().then(function(data) {
            	// If the AJAX call to taskProgressUrl returns an id, add it to the options
            	// for the call back to here. 
            	if (data.id) options.id = data.id;

            	if (data.progress) {
                    onProgress(progressBarElement, progressBarMessageElement, data.progress);
                }
            	
                if (!data.complete) {
                	// setTimeout is vanilla JS and runs updateProgress after pollInterval
                	// setTimeout(function, milliseconds, param1, param2, ...)
                	// where param's are passed to function
                	// This recurses of course but setTiemout schedules the call to
                	// updateProgress in the global context later, so doesn't add to
                	// the stack.
                    setTimeout(updateProgress, pollInterval, progressUrl, options);
                } else {
                    if (data.success) {
                        onSuccess(progressBarElement, progressBarMessageElement);
                    } else {
                        onError(progressBarElement, progressBarMessageElement);
                    }
                    if (data.result) {
                        onResult(resultElement, data.result);
                    }
                }
            });
        });
    }
    
    return {
        onSuccessDefault: onSuccessDefault,
        onResultDefault: onResultDefault,
        onErrorDefault: onErrorDefault,
        onProgressDefault: onProgressDefault,
        updateProgress: updateProgress,
        initProgressBar: updateProgress,  // just for api cleanliness
    };
})();