package test

import (
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Test if the loop is working by asserting whether the log is reset after <interval> seconds
func TestLoop(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true

	log := telemetry.NewLog(telemetryEnabled)

	loggedRequest := telemetry.NewRequestTypeLog("soggy-whale-bread", "REST", "weaviate.something.or.other", 1)
	loggedRequest.When = int64(1550745544)

	log.Register(loggedRequest)

	interval := 1
	url := ""

	reporter := telemetry.NewReporter(log, interval, url, telemetryEnabled)

	go reporter.Start()

	time.Sleep(time.Duration(2) * time.Second)

	logsAfterReporting := log.ExtractLoggedRequests()
	// test
	assert.Equal(t, 0, len(*logsAfterReporting))
}

// Register a single request and call the minimization function, then assert whether all fields have been minimized correctly
func TestMinimization(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true

	log := telemetry.NewLog(telemetryEnabled)

	loggedRequest := telemetry.NewRequestTypeLog("tiny-grey-chainsword", "REST", "weaviate.something.or.other", 1)
	loggedRequest.When = int64(1550745544)

	log.Register(loggedRequest)

	transformer := telemetry.NewOutputTransformer()

	minimizedLogs := transformer.MinimizeFormat(&log.Log)

	var miniLog map[string]interface{}

	for _, item := range *minimizedLogs {
		miniLog = item
	}

	// test
	assert.Equal(t, 1, len(*minimizedLogs))
	assert.Equal(t, "tiny-grey-chainsword", miniLog["n"].(string))
	assert.Equal(t, "REST", miniLog["t"].(string))
	assert.Equal(t, "weaviate.something.or.other", miniLog["i"].(string))
	assert.Equal(t, 1, miniLog["a"].(int))
	assert.Equal(t, int64(1550745544), miniLog["w"].(int64))
}

//func TestPostUsage(t *testing.T) {
//	t.Parallel()
//	// Is this a good idea? This test result would be dependant on an external factor (the monitoring endpoint)
//
//	// test the response received on sending a predetermined post
//
//  // maybe as an acceptance test?
//}
