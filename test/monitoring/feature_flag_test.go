package test

import (
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/telemetry"
	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/assert"
)

// flag on, test Reset func
func TestFeatureFlagEnabled(t *testing.T) {
	t.Parallel()

	// setup (spoofing some required elements)
	flag := &config.Flags{"a", "b"}
	flags := &swag.CommandLineOptionsGroup{
		Options: flag,
	}

	m := &messages.Messaging{true}

	conf := config.WeaviateConfig{}
	conf.LoadConfig(flags, m)

	log := monitoring.RequestsLog{
		Mutex: &sync.Mutex{},
	}

	// test
	result := log.Reset()
	assert.Equal(t, result, monitoring.Succeeded)
	/*
		added feature flags to weaviate/weaviate.conf.json, as that is the file the LoadConfig folder defaults to.
		This may cause errors, as it defaults to a `Develop` environment that does not exist in the file (but does in the tools/dev/conf.json)
		If this causes errors then check with Etienne if this can be amended
	*/
}

/* cover these two, and rename to something covering the total
func GetTelemetryUrl() string {
	return serverConfig.Environment.Telemetry.URL
}

func GetTelemetryInterval() int {
	return serverConfig.Environment.Telemetry.Interval
}
*/

//func TestFeatureFlagDisabled(t *testing.T) {
//	t.Parallel()
//
//	// setup (spoofing some required elements)
//	flag := &config.Flags{"a", "b"}
//	flags := &swag.CommandLineOptionsGroup{
//		Options: flag,
//	}
//
//	m := messaging.Messaging{true}
//
//	conf := WeaviateConfig{}
//	conf.LoadConfig(flags, m)
//
//	log := monitoring.RequestsLog{
//		Mutex: &sync.Mutex{},
//	}
//
//	// test
//	log.Reset()
//}

/*in package restapi
func GetTelemetryFlag fetches the flag from the extracted


It's in (*WeaviateConfig) LoadConfig() in config_handler.go! yay! It gets written to WeaviateConfig.Environment
*/
