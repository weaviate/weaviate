package test

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// Register a single request, then assert whether it has been correctly stored in the log
func TestEnabled(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := true

	calledFunctions := telemetry.NewLog(telemetryEnabled)

	postRequestLog := telemetry.NewRequestTypeLog("unimpressed-rice-sofa", "POST", "weaviate.something.or.other", 1)
	postRequestLog.When = int64(1550745544)

	calledFunctions.Register(postRequestLog)

	// test
	assert.Equal(t, 1, len(calledFunctions.Log))
}

// Register a single request, then assert whether it has been incorrectly stored in the log
func TestDisabled(t *testing.T) {
	t.Parallel()

	// setup
	telemetryEnabled := false

	calledFunctions := telemetry.NewLog(telemetryEnabled)

	postRequestLog := telemetry.NewRequestTypeLog("aquatic-pineapple-home", "POST", "weaviate.something.or.other", 1)
	postRequestLog.When = int64(1550745544)

	calledFunctions.Register(postRequestLog)

	// test
	assert.Equal(t, 0, len(calledFunctions.Log))
}
