package test

import (
	"encoding/json"
	"fmt"
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
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "soggy-whale-bread"
	calledFunctions.Register("REST", "weaviate.something.or.other")

	interval := 1
	url := ""

	reporter := telemetry.NewReporter(calledFunctions, interval, url, telemetryEnabled)

	go reporter.Start()

	time.Sleep(time.Duration(2) * time.Second)

	logsAfterReporting := calledFunctions.ExtractLoggedRequests()

	// test
	assert.Equal(t, 0, len(*logsAfterReporting))
}

// Register a single request and call the JSON minimization function, then assert whether all fields have been minimized correctly
func TestConvertToMinimizedJSON(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "tiny-grey-chainsword"
	calledFunctions.Register("REST", "weaviate.something.or.other")

	calledFunctions.Log["weaviate.something.or.other"].Name = "tiny-grey-chainsword"
	calledFunctions.Log["weaviate.something.or.other"].When = int64(1550745544)

	transformer := telemetry.NewOutputTransformer()

	jsonLogs := transformer.ConvertToMinimizedJSON(&calledFunctions.Log)

	var miniLog []interface{}

	err := json.Unmarshal([]byte(*jsonLogs), &miniLog)

	if err != nil {
		fmt.Println("error:", err)
	}

	// test
	assert.Equal(t, 1, len(miniLog))

	for _, log := range miniLog {
		mapLog := log.(map[string]interface{})

		name := mapLog["n"].(string)
		logType := mapLog["t"].(string)
		identifier := mapLog["i"].(string)
		amount := mapLog["a"].(float64)
		when := mapLog["w"].(float64)

		assert.Equal(t, "tiny-grey-chainsword", name)
		assert.Equal(t, "REST", logType)
		assert.Equal(t, "weaviate.something.or.other", identifier)
		assert.Equal(t, 1, int(amount))
		assert.Equal(t, int64(1550745544), int64(when))

	}
}

// Create a log containing two logged function types and call the AddTimeStamps function,
// then assert whether both logs now contain the same plausible timestamp.
func TestAddTimestamps(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "iridiscent-damp-bagel"
	calledFunctions.Register("REST", "weaviate.something.or.other1")
	calledFunctions.Register("REST", "weaviate.something.or.other2")

	past := time.Now().Unix()

	time.Sleep(time.Duration(1) * time.Second)

	reporter := telemetry.NewReporter(nil, 0, "", true)
	reporter.AddTimeStamps(&calledFunctions.Log)

	time.Sleep(time.Duration(1) * time.Second)

	future := time.Now().Unix()

	log1Timestamp := calledFunctions.Log["weaviate.something.or.other1"].When
	log2Timestamp := calledFunctions.Log["weaviate.something.or.other2"].When

	notZero := log1Timestamp != 0
	notFromThePast := log1Timestamp > past
	notFromTheFuture := log1Timestamp < future

	// test
	assert.Equal(t, log1Timestamp, log2Timestamp)
	assert.Equal(t, true, notZero)
	assert.Equal(t, true, notFromThePast)
	assert.Equal(t, true, notFromTheFuture)
}
