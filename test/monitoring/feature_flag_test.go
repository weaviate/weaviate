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
