package test

import (
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

func TestRequestMonitoring(t *testing.T) {
	t.Parallel()
	calledFunctions := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
		Log:   make(map[string]*telemetry.RequestLog),
	}

	postRequestLog := telemetry.RequestLog{
		Name:       "ginormous-thunder-apple",
		Type:       "POST",
		Identifier: "weaviate.something.or.other",
		Amount:     1,
		When:       1550745544,
	}

	telemetryEnabled := true

	calledFunctions.Register(&postRequestLog, telemetryEnabled)

	assert.Equal(t, 1, calledFunctions.Log["weaviate.something.or.other"].Amount)
}

func TestRequestIncrementing(t *testing.T) {
	t.Parallel()
	calledFunctions := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
		Log:   make(map[string]*telemetry.RequestLog),
	}

	postRequestLog := telemetry.RequestLog{
		Name:       "grilled-cheese-sandwich",
		Type:       "POST",
		Identifier: "weaviate.something.or.other",
		Amount:     1,
		When:       1550745544,
	}

	telemetryEnabled := true

	calledFunctions.Register(&postRequestLog, telemetryEnabled)
	calledFunctions.Register(&postRequestLog, telemetryEnabled)

	assert.Equal(t, 2, calledFunctions.Log["weaviate.something.or.other"].Amount)
}

func TestMultipleRequestTypes(t *testing.T) {
	t.Parallel()
	calledFunctions := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
		Log:   make(map[string]*telemetry.RequestLog),
	}

	postRequestLog1 := telemetry.RequestLog{
		Name:       "awkward-handshake-guy",
		Type:       "GQL",
		Identifier: "weaviate.something.or.other1",
		Amount:     1,
		When:       1550745544,
	}

	postRequestLog2 := telemetry.RequestLog{
		Name:       "apologetic-thermonuclear-blunderbuss",
		Type:       "POST",
		Identifier: "weaviate.something.or.other2",
		Amount:     1,
		When:       1550745544,
	}

	postRequestLog3 := telemetry.RequestLog{
		Name:       "dormant-artificial-piglet",
		Type:       "POST",
		Identifier: "weaviate.something.or.other3",
		Amount:     1,
		When:       1550745544,
	}

	telemetryEnabled := true

	calledFunctions.Register(&postRequestLog1, telemetryEnabled)
	calledFunctions.Register(&postRequestLog2, telemetryEnabled)
	calledFunctions.Register(&postRequestLog3, telemetryEnabled)
	calledFunctions.Register(&postRequestLog3, telemetryEnabled)

	assert.Equal(t, 1, calledFunctions.Log["weaviate.something.or.other1"].Amount)
	assert.Equal(t, 1, calledFunctions.Log["weaviate.something.or.other2"].Amount)
	assert.Equal(t, 2, calledFunctions.Log["weaviate.something.or.other3"].Amount)
}

func TestConcurrencyHandling(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	calledFunctions := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
		Log:   make(map[string]*telemetry.RequestLog),
	}

	postRequestLog1 := telemetry.RequestLog{
		Name:       "forgetful-seal-cooker",
		Type:       "GQL",
		Identifier: "weaviate.something.or.other1",
		Amount:     1,
		When:       1550745544,
	}

	postRequestLog2 := telemetry.RequestLog{
		Name:       "brave-maple-leaf",
		Type:       "POST",
		Identifier: "weaviate.something.or.other2",
		Amount:     1,
		When:       1550745544,
	}

	telemetryEnabled := true

	performOneThousandConcurrentRequests(&calledFunctions, &postRequestLog1, &postRequestLog2, telemetryEnabled, &wg)

	wg.Wait()

	assert.Equal(t, 500, calledFunctions.Log["weaviate.something.or.other1"].Amount)
	assert.Equal(t, 500, calledFunctions.Log["weaviate.something.or.other2"].Amount)
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

func TestNetworkGQLQueryMonitoring(t *testing.T) {
	t.Parallel()
	// is this interesting at all? Sounds good, but both network and local queries are logged the same.
	// This doesn't differ from the tests above.

	// 2 instances, 1 network GQL query -> monitor 1 network and 1 local Q?
	// needs single instance. network Q -> test out, test in
}

// TO TEST:
/*
what if one of the fields is None?
*/
/*



happy flows



*/
