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
	peerName := "unimpressed-rice-sofa"
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = peerName

	postRequestLog := telemetry.NewRequestTypeLog("REST", "weaviate.something.or.other")
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
	peerName := "aquatic-pineapple-home"
	calledFunctions := telemetry.NewLog(telemetryEnabled)
	calledFunctions.PeerName = peerName

	postRequestLog := telemetry.NewRequestTypeLog("REST", "weaviate.something.or.other")
	postRequestLog.When = int64(1550745544)

	calledFunctions.Register(postRequestLog)

	// test
	assert.Equal(t, 0, len(calledFunctions.Log))
}
