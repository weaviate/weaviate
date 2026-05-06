//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestParseRuntimeConfig_UsageLimitsFields confirms the new YAML keys for
// usage-limit overrides round-trip through ParseRuntimeConfig.
func TestParseRuntimeConfig_UsageLimitsFields(t *testing.T) {
	buf := []byte(`maximum_allowed_objects_count: 10000
maximum_allowed_tenants_per_collection: 2
maximum_allowed_shards_per_collection: 1
usage_limits_error_message: "hit limit of {value} {limit}"
`)
	cfg, err := ParseRuntimeConfig(buf)
	require.NoError(t, err)

	assert.Equal(t, 10000, cfg.MaximumAllowedObjectsCount.Get())
	assert.Equal(t, 2, cfg.MaximumAllowedTenantsPerCollection.Get())
	assert.Equal(t, 1, cfg.MaximumAllowedShardsPerCollection.Get())
	assert.Equal(t, "hit limit of {value} {limit}", cfg.UsageLimitsErrorMessage.Get())
}

// TestUpdateRuntimeConfig_UsageLimitsLogging verifies that runtime updates
// to the new fields produce the same structured-change log lines as the
// existing MaximumAllowedCollectionsCount precedent. Mirrors the assertion
// style at runtimeconfig_test.go:240-279.
func TestUpdateRuntimeConfig_UsageLimitsLogging(t *testing.T) {
	log := logrus.New()
	logs := bytes.Buffer{}
	log.SetOutput(&logs)

	var (
		objects = runtime.NewDynamicValue(0)
		tenants = runtime.NewDynamicValue(0)
		shards  = runtime.NewDynamicValue(0)
		message = runtime.NewDynamicValue("")
	)
	reg := &WeaviateRuntimeConfig{
		MaximumAllowedObjectsCount:         objects,
		MaximumAllowedTenantsPerCollection: tenants,
		MaximumAllowedShardsPerCollection:  shards,
		UsageLimitsErrorMessage:            message,
	}

	buf := []byte(`maximum_allowed_objects_count: 10
maximum_allowed_tenants_per_collection: 2
maximum_allowed_shards_per_collection: 1
usage_limits_error_message: "test {limit} {value}"
`)
	parsed, err := ParseRuntimeConfig(buf)
	require.NoError(t, err)

	require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))

	assert.Equal(t, 10, objects.Get())
	assert.Equal(t, 2, tenants.Get())
	assert.Equal(t, 1, shards.Get())
	assert.Equal(t, "test {limit} {value}", message.Get())

	// Confirm structured logging fired for at least one of the new fields,
	// matching the format the existing collection-count test asserts on.
	assert.Contains(t, logs.String(), "MaximumAllowedObjectsCount")
}
