package helper

// Internal struct to link the HTTP client logging of the Weaviate API client to the test's logging output.

import "testing"

type testLogger struct {
	t *testing.T
}

func (tl *testLogger) Printf(format string, args ...interface{}) {
	tl.t.Logf("HTTP LOG:\n"+format, args...)
}

func (tl *testLogger) Debugf(format string, args ...interface{}) {
	tl.t.Logf("HTTP DEBUG:\n"+format, args...)
}
