package test

import (
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

func TestPostRequestMonitoring(t *testing.T) {
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
	}

	telemetryEnabled := true

	calledFunctions.Register(&postRequestLog, telemetryEnabled)

	assert.Equal(t, 1, calledFunctions.Log["weaviate.something.or.other"].Amount)
	// perform/mimic one request, check all 5 fields of the monitoring service object contents (timestamp > current < (current + interval))
}

func TestPostRequestIncrementing(t *testing.T) {
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
	}

	telemetryEnabled := true

	calledFunctions.Register(&postRequestLog, telemetryEnabled)
	calledFunctions.Register(&postRequestLog, telemetryEnabled)

	assert.Equal(t, 2, calledFunctions.Log["weaviate.something.or.other"].Amount)
}

func TestGraphQLRequestMonitoring(t *testing.T) {
	t.Parallel()
	// perform/mimic one request, check all 5 fields of the monitoring service object contents (timestamp > current < (current + interval))
}

func TestNetworkGQLQueryMonitoring(t *testing.T) {
	t.Parallel()
	// 2 instances, 1 network GQL query -> monitor 1 network and 1 local Q?
	// needs single instance. network Q -> test out, test in
}

func TestConcurrencyHandling(t *testing.T) {
	t.Parallel()
	// multithreaded (simultaneous) writes to monitoring service || https://blog.narenarya.in/concurrent-http-in-go.html
}

// TO TEST:
/*
what if one of the fields is None?
*/
/*



happy flows



*/
