package telemetry

import (
	"time"

	telemetry "github.com/creativesoftwarefdn/weaviate/telemetry"
)

func NewReporter(requestsLog *telemetry.RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool) {
	return &Reporter{
		log:       requestsLog,
		interval:  reportInterval,
		url:       reportURL,
		telemetry: telemetryEnabled,
	}
}

type Reporter struct {
	log              *telemetry.RequestsLog
	interval         int
	url              string
	telemetryEnabled bool
	//	if telemetry.IsEnabled() == true {
	//		// output every X (300) seconds
	//		// --CBOR conversion -- handled by ugorji
	//		// --Failsafe -- TBD
	//		// --get timestamp and apply to output nested objects
	//	}
}

func (r *Reporter) Start() {
	if r.telemetryEnabled {
		time.sleep(r.interval)
		r.log.GetCurrentState(r.telemetryEnabled)
	}
}

/*
	Reporter struct collects the logged serviceids every <interval> seconds, converts this data to CBOR format and posts it to <URL>
*/

// Test if the loop is working by asserting whether the log is reset after <interval> seconds
func TestReportingLoop(t *testing.T) {
	t.Parallel()

	// setup
	log := telemetry.NewLog()

	loggedRequest := telemetry.NewRequestTypeLog("soggy-whale-bread", "POST", "weaviate.something.or.other", 1)
	loggedRequest.When = int64(1550745544)

	telemetryEnabled := true

	log.Register(loggedRequest, telemetryEnabled)

	interval := 1
	url := ""

	reporter := telemetry.NewReporter(log, interval, url)
	go reporter.Start()

	time.Sleep(2)

	// test
	assert.Equals(t, 0, len(log))

	/*
		create log
		add call to log
		call func with interval = 1
		check if log is empty
	*/
}

/*

is func called as goroutine that takes log pointer and url and interval and then reads log and then resets log

*/
