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

package helper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// AssertVectorIndexDropped polls the schema until every named vector on className
// has reached a valid post-drop terminal state, failing the test otherwise.
//
// Dropping a named vector is asynchronous: the handler first replaces the entry
// with the "none" marker (VectorIndexType "none", nil VectorIndexConfig; see
// Handler.dropVectorIndex) and a cleanup finalizer later removes the entry
// outright (see cluster/schema removedDroppedVectorConfigs). Both are valid
// terminal states, so either is accepted:
//
//   - the entry is present carrying the "none" marker and nil config, or
//   - the entry is already absent (the finalizer won the race).
//
// A stuck original index type (e.g. "hnsw" still present) fails the assertion,
// so the check is non-vacuous. Pass more than one vectorName to assert several
// dropped vectors within a single polling window.
//
// Use AssertVectorIndexDroppedAuth for classes that must be read with an auth
// key (e.g. namespaced, qualified class names).
func AssertVectorIndexDropped(t *testing.T, className string, vectorNames ...string) {
	t.Helper()
	assertVectorIndexDropped(t, func() *models.Class { return GetClass(t, className) }, vectorNames...)
}

// AssertVectorIndexDroppedAuth is AssertVectorIndexDropped for a class that must
// be read with an auth key.
func AssertVectorIndexDroppedAuth(t *testing.T, className, key string, vectorNames ...string) {
	t.Helper()
	assertVectorIndexDropped(t, func() *models.Class { return GetClassAuth(t, className, key) }, vectorNames...)
}

func assertVectorIndexDropped(t *testing.T, getClass func() *models.Class, vectorNames ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		cls := getClass()
		for _, name := range vectorNames {
			cfg, ok := cls.VectorConfig[name]
			if !ok {
				continue // finalizer already removed the entry; also a valid terminal state
			}
			assert.Equalf(collect, "none", cfg.VectorIndexType,
				"VectorIndexType should be 'none' for dropped vector %q", name)
			assert.Nilf(collect, cfg.VectorIndexConfig,
				"VectorIndexConfig should be nil for dropped vector %q", name)
		}
	}, 15*time.Second, 200*time.Millisecond,
		"schema should reflect the dropped vector index(es)")
}
