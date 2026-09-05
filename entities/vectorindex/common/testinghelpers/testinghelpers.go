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

// Package testinghelpers provides shared assertions for the vectorindex
// config parsers (hnsw, flat, dynamic, hfresh), which each vendor their own
// ParseAndValidateConfig but should reject malformed input identically.
package testinghelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssertNullDistanceRejected verifies that a config parser rejects an
// explicit `"distance": null` with a type-mismatch error instead of
// silently leaving the field unset and falling back to the default distance
// metric. See weaviate/weaviate#11732.
func AssertNullDistanceRejected(t *testing.T, parseFn func(input map[string]interface{}) error) {
	t.Helper()
	err := parseFn(map[string]interface{}{"distance": nil})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"distance" must be a string`)
}
