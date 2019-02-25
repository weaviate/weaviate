package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Register a single request, then assert whether all fields have been stored correctly.
func TestRequestMonitoringBasics(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := telemetry.NewLog()

	postRequestLog := telemetry.NewRequestTypeLog("ginormous-thunder-apple", "POST", "weaviate.something.or.other", 1)
	postRequestLog.When = int64(1550745544)

	telemetryEnabled := true

	calledFunctions.Register(postRequestLog, telemetryEnabled)

	loggedFunc := calledFunctions.Log["weaviate.something.or.other"]

	// test
	assert.Equal(t, 1, len(calledFunctions.Log))

	for funcName := range calledFunctions.Log {
		assert.Equal(t, "weaviate.something.or.other", funcName)
	}

	assert.Equal(t, "ginormous-thunder-apple", loggedFunc.Name)
	assert.Equal(t, "POST", loggedFunc.Type)
	assert.Equal(t, "weaviate.something.or.other", loggedFunc.Identifier)
	assert.Equal(t, 1, loggedFunc.Amount)
	assert.Equal(t, int64(1550745544), loggedFunc.When)
}

// Log two requests of the same type, then assert whether the log contains a single request record with Amount set to two
func TestRequestIncrementing(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := telemetry.NewLog()

	postRequestLog := telemetry.NewRequestTypeLog("grilled-cheese-sandwich", "POST", "weaviate.something.or.other", 1)
	postRequestLog.When = int64(1550745544)

	telemetryEnabled := true

	calledFunctions.Register(postRequestLog, telemetryEnabled)
	calledFunctions.Register(postRequestLog, telemetryEnabled)

	loggedFunctionType := calledFunctions.Log["weaviate.something.or.other"]

	// test
	assert.Equal(t, 2, loggedFunctionType.Amount)
	assert.Equal(t, 1, len(calledFunctions.Log))
}

// Log multiple request types, then assert whether the log contains each of the added request types
func TestMultipleRequestTypes(t *testing.T) {
	t.Parallel()

	// setup
	calledFunctions := telemetry.NewLog()

	postRequestLog1 := telemetry.NewRequestTypeLog("awkward-handshake-guy", "GQL", "weaviate.something.or.other1", 1)
	postRequestLog1.When = int64(1550745544)

	postRequestLog2 := telemetry.NewRequestTypeLog("apologetic-thermonuclear-blunderbuss", "POST", "weaviate.something.or.other2", 1)
	postRequestLog2.When = int64(1550745544)

	postRequestLog3 := telemetry.NewRequestTypeLog("dormant-mechanical-piglet", "POST", "weaviate.something.or.other3", 1)
	postRequestLog3.When = int64(1550745544)

	telemetryEnabled := true

	calledFunctions.Register(postRequestLog1, telemetryEnabled)
	calledFunctions.Register(postRequestLog2, telemetryEnabled)
	calledFunctions.Register(postRequestLog3, telemetryEnabled)
	calledFunctions.Register(postRequestLog3, telemetryEnabled)

	loggedFunctionType1 := calledFunctions.Log["weaviate.something.or.other1"]
	loggedFunctionType2 := calledFunctions.Log["weaviate.something.or.other2"]
	loggedFunctionType3 := calledFunctions.Log["weaviate.something.or.other3"]

	// test
	assert.Equal(t, 1, loggedFunctionType1.Amount)
	assert.Equal(t, 1, loggedFunctionType2.Amount)
	assert.Equal(t, 2, loggedFunctionType3.Amount)
}

// Add a single record and call the extract function (read + reset). Then assert whether the extracted records
// contain 1 record (read) and whether the log contains 0 records (delete).
func TestExtractLoggedRequests(t *testing.T) {
	t.Parallel()

	// setup
	var wg sync.WaitGroup

	calledFunctions := telemetry.NewLog()

	postRequestLog1 := telemetry.NewRequestTypeLog("fuzzy-painted-mug", "GQL", "weaviate.something.or.other1", 1)
	postRequestLog1.When = int64(1550745544)

	telemetryEnabled := true

	calledFunctions.Register(postRequestLog1, telemetryEnabled)

	wg.Add(1)
	//
	requestResults := make(chan *map[string]*telemetry.RequestLog, 1)

	go performExtraction(calledFunctions, &wg, telemetryEnabled, &requestResults)

	wg.Wait()

	close(requestResults)

	for results := range requestResults {
		loggedFunctions := len(*results)

		// test
		assert.Equal(t, 1, loggedFunctions)
		assert.Equal(t, 0, len(calledFunctions.Log))
	}
}

// Use a waitgroup + channel to avoid race conditions in the test case. This isn't necessary in production as processes in goroutines aren't dependent on eachothers' states.
func performExtraction(calledFunctions *telemetry.RequestsLog, wg *sync.WaitGroup, telemetryEnabled bool, requestResults *chan *map[string]*telemetry.RequestLog) {
	defer wg.Done()
	*requestResults <- calledFunctions.ExtractLoggedRequests(telemetryEnabled)
}

// Spawn 10 goroutines that each register 100 function calls, then assert whether we end up with 1000 records in the log
func TestConcurrentRequests(t *testing.T) {
	t.Parallel()

	// setup
	var wg sync.WaitGroup

	calledFunctions := telemetry.NewLog()

	postRequestLog1 := telemetry.NewRequestTypeLog("forgetful-seal-cooker", "GQL", "weaviate.something.or.other1", 1)
	postRequestLog1.When = int64(1550745544)

	postRequestLog2 := telemetry.NewRequestTypeLog("brave-maple-leaf", "POST", "weaviate.something.or.other2", 1)
	postRequestLog2.When = int64(1550745544)

	telemetryEnabled := true

	performOneThousandConcurrentRequests(calledFunctions, postRequestLog1, postRequestLog2, telemetryEnabled, &wg)

	wg.Wait()

	loggedFunctionType1 := calledFunctions.Log["weaviate.something.or.other1"]
	loggedFunctionType2 := calledFunctions.Log["weaviate.something.or.other2"]

	// test
	assert.Equal(t, 500, loggedFunctionType1.Amount)
	assert.Equal(t, 500, loggedFunctionType2.Amount)
}

func performOneThousandConcurrentRequests(calledFunctions *telemetry.RequestsLog, postRequestLog1 *telemetry.RequestLog, postRequestLog2 *telemetry.RequestLog, telemetryEnabled bool, wg *sync.WaitGroup) {
	for i := 0; i < 10; i++ {
		wg.Add(1)

		if i < 5 {
			go performOneHundredRequests(calledFunctions, postRequestLog1, telemetryEnabled, wg)
		} else {
			go performOneHundredRequests(calledFunctions, postRequestLog2, telemetryEnabled, wg)
		}
	}
}

func performOneHundredRequests(calledFunctions *telemetry.RequestsLog, calledFunction *telemetry.RequestLog, telemetryEnabled bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 100; i++ {
		calledFunctions.Register(calledFunction, telemetryEnabled)
	}
}
