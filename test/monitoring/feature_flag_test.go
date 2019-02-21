package test

import (
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/stretchr/testify/assert"
)

// test if the RequestsLog.Reset() function can be called if the feature flag is enabled
func TestFeatureFlagEnabled(t *testing.T) {
	t.Parallel()

	log := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
	}

	result := log.Reset(true)
	assert.Equal(t, telemetry.Succeeded, result)
	/*
		added feature flags to weaviate/weaviate.conf.json, as that is the file the LoadConfig folder defaults to.
		This may cause errors, as it defaults to a `Develop` environment that does not exist in the file (but does in the tools/dev/conf.json)
		If this causes errors then check with Etienne if this can be amended
	*/
}

// test if the RequestsLog.Reset() function can be called if the feature flag is disabled
func TestFeatureFlagDisabled(t *testing.T) {
	t.Parallel()

	log := telemetry.RequestsLog{
		Mutex: &sync.Mutex{},
	}

	result := log.Reset(false)
	assert.Equal(t, telemetry.Failed, result)
}

/* cover these two, and rename to something covering the total
func GetTelemetryUrl() string {
	return serverConfig.Environment.Telemetry.URL
}

func GetTelemetryInterval() int {
	return serverConfig.Environment.Telemetry.Interval
}
*/
