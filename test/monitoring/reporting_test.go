package test

import (
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Test if the loop is working by asserting whether the log is reset after <interval> seconds
func TestReportingLoop(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true

	log := telemetry.NewLog(telemetryEnabled)

	loggedRequest := telemetry.NewRequestTypeLog("soggy-whale-bread", "POST", "weaviate.something.or.other", 1)
	loggedRequest.When = int64(1550745544)

	log.Register(loggedRequest)

	interval := 1
	url := ""

	reporter := telemetry.NewReporter(log, interval, url, telemetryEnabled)

	go reporter.Start()

	time.Sleep(time.Duration(2) * time.Second)

	// test
	assert.Equal(t, 0, len(log.Log))
}

//func TestPostUsage(t *testing.T) {
//	t.Parallel()
//	// Is this a good idea? This test result would be dependant on an external factor (the monitoring endpoint)
//
//	// test the response received on sending a predetermined post
//
//  // maybe as an acceptance test?
//}
