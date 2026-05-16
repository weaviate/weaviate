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

package vector_index_restrictions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// TestFailToBoot_HfreshOnlyWithCompression verifies the cross-field
// startup validation: setting ALLOWED_VECTOR_INDEX_TYPES=hfresh (only)
// together with any ALLOWED_COMPRESSION_TYPES value is an operator
// mistake — hfresh has no compression knobs. Config.Validate() rejects
// the combination at startup and the container fails to come up.
//
// Owns its own container because the validation runs at boot, before
// any REST endpoint is reachable.
func TestFailToBoot_HfreshOnlyWithCompression(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	c := docker.New().WithWeaviate().
		WithWeaviateEnv("ALLOWED_VECTOR_INDEX_TYPES", "hfresh").
		WithWeaviateEnv("ALLOWED_COMPRESSION_TYPES", "rq-8")

	compose, err := c.Start(ctx)
	// Whether Start succeeded or failed, Terminate any artifacts the
	// harness created (network, transient containers) so the next
	// fail-to-boot test doesn't collide with a leftover network on the
	// fixed TestSubnet (10.99.0.0/16). The Compose handle is non-nil
	// even on failed Start in the current testcontainers harness.
	t.Cleanup(func() {
		if compose != nil {
			_ = compose.Terminate(context.Background())
		}
	})
	require.Error(t, err, "container should fail to boot when ALLOWED_VECTOR_INDEX_TYPES=hfresh and ALLOWED_COMPRESSION_TYPES is non-empty")
	// The error returned by testcontainers wraps the container exit
	// reason — match on the validator's substring which is included in
	// the captured logs the harness echoes back.
	assert.Contains(t, err.Error(), "hfresh",
		"failure should mention hfresh as the offending value, got: %v", err)
}

// TestFailToBoot_DefaultMismatch covers the multi-entry allow-list
// rule: when ALLOWED_VECTOR_INDEX_TYPES has multiple values, the
// matching default must be set AND in the list. An unset default is a
// boot-time error.
func TestFailToBoot_DefaultMismatch(t *testing.T) {
	ctx, cancel := suiteContext(t)
	defer cancel()

	c := docker.New().WithWeaviate().
		WithWeaviateEnv("ALLOWED_VECTOR_INDEX_TYPES", "hfresh,hnsw")
	// DEFAULT_VECTOR_INDEX deliberately unset — multi-entry allow-list
	// requires an explicit default.

	compose, err := c.Start(ctx)
	t.Cleanup(func() {
		if compose != nil {
			_ = compose.Terminate(context.Background())
		}
	})
	require.Error(t, err, "container should fail to boot when multi-entry ALLOWED_VECTOR_INDEX_TYPES has no DEFAULT_VECTOR_INDEX")
	assert.Contains(t, err.Error(), "DEFAULT_VECTOR_INDEX",
		"failure should mention the missing default env var, got: %v", err)
}
