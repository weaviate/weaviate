package test

import (
	"testing"
)

func TestPostRequestMonitoring(t *testing.T) {
	t.Parallel()
	// perform/mimic one request, check all 5 fields of the monitoring service object contents (timestamp > current < (current + interval))
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
