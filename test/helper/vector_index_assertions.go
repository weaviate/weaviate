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

const (
	dropAssertionWaitFor = 15 * time.Second
	dropAssertionTick    = 200 * time.Millisecond
)

// AssertVectorIndexDropped polls until each named vector reaches a terminal
// dropped state: present with VectorIndexType "none" and nil VectorIndexConfig,
// or absent because the async finalizer already removed the entry. Both are
// valid outcomes of the drop-finalizer race, so callers that only need "the
// index is gone" want this.
func AssertVectorIndexDropped(t *testing.T, className string, vectorNames ...string) {
	t.Helper()
	dropAssertion{getClass: func() *models.Class { return GetClass(t, className) }}.
		run(t, vectorNames...)
}

// AssertVectorIndexDroppedAuth is AssertVectorIndexDropped for a class that must
// be read with an auth key.
func AssertVectorIndexDroppedAuth(t *testing.T, className, key string, vectorNames ...string) {
	t.Helper()
	dropAssertion{getClass: func() *models.Class { return GetClassAuth(t, className, key) }}.
		run(t, vectorNames...)
}

// AssertVectorIndexDropMarked additionally requires the "none" marker to still
// be observable, so entry removal is a failure rather than a second valid
// outcome. Only for sites where the marker itself is the subject under test and
// the finalizer provably cannot have run yet; everywhere else the entry may
// legitimately be gone already and AssertVectorIndexDropped is the right call.
func AssertVectorIndexDropMarked(t *testing.T, className string, vectorNames ...string) {
	t.Helper()
	dropAssertion{
		getClass:      func() *models.Class { return GetClass(t, className) },
		requireMarker: true,
	}.run(t, vectorNames...)
}

// dropAssertion carries the knobs of one post-drop schema assertion. waitFor and
// tick default to the package constants when left zero.
type dropAssertion struct {
	getClass      func() *models.Class
	requireMarker bool
	waitFor, tick time.Duration
}

// run takes require.TestingT rather than *testing.T so its guards can be
// exercised against a recording stand-in.
func (a dropAssertion) run(t require.TestingT, vectorNames ...string) {
	if a.waitFor == 0 {
		a.waitFor, a.tick = dropAssertionWaitFor, dropAssertionTick
	}

	// A variadic call with no names polls an empty loop, which no observation
	// can ever falsify.
	require.NotEmpty(t, vectorNames, "no vector names given: the assertion would be vacuous")

	var marked, removed int
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		cls := a.getClass()
		marked, removed = 0, 0
		if !assert.NotNil(collect, cls, "class must be readable to judge the drop") {
			return
		}
		for _, name := range vectorNames {
			cfg, ok := cls.VectorConfig[name]
			if !ok {
				removed++
				continue
			}
			marked++
			assert.Equalf(collect, "none", cfg.VectorIndexType,
				"VectorIndexType should be 'none' for dropped vector %q", name)
			assert.Nilf(collect, cfg.VectorIndexConfig,
				"VectorIndexConfig should be nil for dropped vector %q", name)
		}
		if a.requireMarker {
			assert.Equalf(collect, len(vectorNames), marked,
				"every dropped vector should still carry the 'none' marker")
		}
	}, a.waitFor, a.tick, "schema should reflect the dropped vector index(es)")

	// EventuallyWithT is satisfied by any pass that raises no failure, including
	// one that ran no assertion at all. Re-checking the tally outside the loop
	// turns "observed nothing" into a failure instead of a pass.
	require.Equalf(t, len(vectorNames), marked+removed,
		"expected every named vector to reach a terminal state, saw %d marked and %d removed of %d",
		marked, removed, len(vectorNames))
	if a.requireMarker {
		require.Equalf(t, len(vectorNames), marked,
			"expected every named vector to still carry the 'none' marker, saw %d marked and %d already removed",
			marked, removed)
	}
}
