package telemetry

import (
	"time"
)

func NewReporter(requestsLog *RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool) *Reporter {
	return &Reporter{
		log:       requestsLog,
		interval:  reportInterval,
		url:       reportURL,
		telemetry: telemetryEnabled,
	}
}

type Reporter struct {
	log       *RequestsLog
	interval  int
	url       string
	telemetry bool
	//	if IsEnabled() == true {
	//		// output every X (300) seconds
	//		// --CBOR conversion -- handled by ugorji
	//		// --Failsafe -- TBD
	//		// --get timestamp and apply to output nested objects
	//	}
}

// Reports function calls in the last <provided interval> seconds in CBOR format to the provided url.
// Contains a failsafe mechanism in the case the url is unreachable.
func (r *Reporter) Start() {
	if r.telemetry {
		time.Sleep(time.Duration(r.interval) * time.Second)
		//		extractedLog := r.log.ExtractLoggedRequests(r.telemetry)
	}
}

/*
	Reporter struct collects the logged serviceids every <interval> seconds, converts this data to CBOR format and posts it to <URL>
*/

/*

is func called as goroutine that takes log pointer and url and interval and then reads log and then resets log

*/
