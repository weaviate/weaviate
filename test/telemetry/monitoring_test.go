package test

import (
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Register a single request, then assert whether all fields have been stored correctly.
func TestBasics(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "ginormous-thunder-apple"
	calledFunctions.Register("REST", "weaviate.something.or.other")

	calledFunctions.Log["weaviate.something.or.other"].Name = "ginormous-thunder-apple"
	calledFunctions.Log["weaviate.something.or.other"].When = int64(1550745544)

	loggedFunc := calledFunctions.Log["weaviate.something.or.other"]

	// test
	assert.Equal(t, 1, len(calledFunctions.Log))

	for funcName := range calledFunctions.Log {
		assert.Equal(t, "weaviate.something.or.other", funcName)
	}

	assert.Equal(t, "ginormous-thunder-apple", loggedFunc.Name)
	assert.Equal(t, "REST", loggedFunc.Type)
	assert.Equal(t, "weaviate.something.or.other", loggedFunc.Identifier)
	assert.Equal(t, 1, loggedFunc.Amount)
	assert.Equal(t, int64(1550745544), loggedFunc.When)
}

// Log two requests of the same type, then assert whether the log contains a single request record with Amount set to two
func TestRequestIncrementing(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "awkward-handshake-guy"
	calledFunctions.Register("REST", "weaviate.something.or.other")
	calledFunctions.Register("REST", "weaviate.something.or.other")

	loggedFunctionType := calledFunctions.Log["weaviate.something.or.other"]

	// test
	assert.Equal(t, 2, loggedFunctionType.Amount)
	assert.Equal(t, 1, len(calledFunctions.Log))
}

// Log multiple request types, then assert whether the log contains each of the added request types
func TestMultipleRequestTypes(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "insert-ridiculous-name"
	calledFunctions.Register("GQL", "weaviate.something.or.other1")
	calledFunctions.Register("REST", "weaviate.something.or.other2")
	calledFunctions.Register("REST", "weaviate.something.or.other3")
	calledFunctions.Register("REST", "weaviate.something.or.other3")

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
	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "apologetic-thermonuclear-blunderbuss"
	calledFunctions.Register("GQL", "weaviate.something.or.other")

	requestResults := make(chan *map[string]*telemetry.RequestLog)

	go func() {
		requestResults <- calledFunctions.ExtractLoggedRequests()
		close(requestResults)
	}()

	for loggedFuncs := range requestResults {
		for _, requestLog := range *loggedFuncs {

			loggedFunctions := len(*loggedFuncs)

			// test
			assert.Equal(t, 1, loggedFunctions)
			assert.Equal(t, 0, len(*calledFunctions.ExtractLoggedRequests()))

			assert.Equal(t, "GQL", requestLog.Type)
			assert.Equal(t, "weaviate.something.or.other", requestLog.Identifier)
			assert.Equal(t, 1, requestLog.Amount)
		}
	}
}

// Spawn 10 goroutines that each register 100 function calls, then assert whether we end up with 1000 records in the log
func TestConcurrentRequests(t *testing.T) {
	t.Parallel()

	// setup
	var wg sync.WaitGroup

	telemetryEnabled := true
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = "forgetful-seal-cooker"

	type1 := "GQL"
	type2 := "REST"
	id1 := "weaviate.something.or.other1"
	id2 := "weaviate.something.or.other2"

	performOneThousandConcurrentRequests(calledFunctions, type1, id1, type2, id2, &wg)

	wg.Wait()

	loggedFunctions := calledFunctions.ExtractLoggedRequests()

	// test
	for _, function := range *loggedFunctions {
		assert.Equal(t, 500, function.Amount)
	}
}

func performOneThousandConcurrentRequests(calledFunctions *telemetry.RequestsLog, type1 string, type2 string, id1 string, id2 string, wg *sync.WaitGroup) {
	for i := 0; i < 10; i++ {
		wg.Add(1)

		if i < 5 {
			go performOneHundredRequests(calledFunctions, type1, id1, wg)
		} else {
			go performOneHundredRequests(calledFunctions, type2, id2, wg)
		}
	}
}

func performOneHundredRequests(calledFunctions *telemetry.RequestsLog, funcType string, id string, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 100; i++ {
		calledFunctions.Register(funcType, id)
	}
}
