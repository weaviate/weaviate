package test

import (
	"testing"
	"time"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

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

	reporter := telemetry.NewReporter(log, interval, url, telemetryEnabled)
	go reporter.Start()

	time.Sleep(2)

	// test
	assert.Equal(t, 0, len(log.Log))
}

func TestPostUsage(t *testing.T) {
	t.Parallel()
	// Is this a good idea? This test result would be dependant on an external factor (the monitoring endpoint)

	// test the response received on sending a predetermined post
}
