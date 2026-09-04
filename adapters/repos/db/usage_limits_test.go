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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func TestColdObjectCounts(t *testing.T) {
	t.Run("get on missing key returns zero and false", func(t *testing.T) {
		c := newColdObjectCounts()
		v, ok := c.get("absent")
		assert.False(t, ok)
		assert.Equal(t, int64(0), v)
	})

	t.Run("set then get returns the stored value", func(t *testing.T) {
		c := newColdObjectCounts()
		c.set("T1", 42)
		v, ok := c.get("T1")
		assert.True(t, ok)
		assert.Equal(t, int64(42), v)
	})

	t.Run("set overwrites a prior value", func(t *testing.T) {
		c := newColdObjectCounts()
		c.set("T1", 1)
		c.set("T1", 2)
		v, ok := c.get("T1")
		assert.True(t, ok)
		assert.Equal(t, int64(2), v)
	})

	t.Run("drop removes the entry", func(t *testing.T) {
		c := newColdObjectCounts()
		c.set("T1", 42)
		c.drop("T1")
		v, ok := c.get("T1")
		assert.False(t, ok)
		assert.Equal(t, int64(0), v)
	})

	t.Run("drop on missing key is a safe no-op", func(t *testing.T) {
		c := newColdObjectCounts()
		c.drop("never-set")
		c.drop("never-set")
	})

	t.Run("mutations on one tenant don't affect others", func(t *testing.T) {
		c := newColdObjectCounts()
		c.set("T1", 1)
		c.set("T2", 2)
		c.set("T3", 3)
		c.drop("T2")

		v1, ok1 := c.get("T1")
		assert.True(t, ok1)
		assert.Equal(t, int64(1), v1)

		v2, ok2 := c.get("T2")
		assert.False(t, ok2)
		assert.Equal(t, int64(0), v2)

		v3, ok3 := c.get("T3")
		assert.True(t, ok3)
		assert.Equal(t, int64(3), v3)
	})
}

// TestIndex_SetUsageLimits pins the install-time gating contract:
// the cold-tenant cache is allocated only when the index is
// multi-tenant *and* the object cap is configured at install time.
// Any other combination must leave coldObjects nil and
// coldObjectsTracked false.
func TestIndex_SetUsageLimits(t *testing.T) {
	mgrCapSet := usagelimits.NewManager(usagelimits.Config{
		MaxObjectsCount: runtime.NewDynamicValue(100),
	}, nil)
	mgrCapUnset := usagelimits.NewManager(usagelimits.Config{
		MaxObjectsCount: nil,
	}, nil)

	tests := []struct {
		name         string
		partitioning bool
		manager      *usagelimits.Manager
		wantTracked  bool
	}{
		{
			name:         "single-tenant index never tracks, even with cap set",
			partitioning: false,
			manager:      mgrCapSet,
		},
		{
			name:         "multi-tenant with cap unset does not track",
			partitioning: true,
			manager:      mgrCapUnset,
		},
		{
			name:         "multi-tenant with nil manager does not track",
			partitioning: true,
			manager:      nil,
		},
		{
			name:         "multi-tenant with cap set tracks",
			partitioning: true,
			manager:      mgrCapSet,
			wantTracked:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := &Index{partitioningEnabled: tc.partitioning}
			idx.SetUsageLimits(tc.manager)

			assert.Equal(t, tc.wantTracked, idx.coldObjectsTracked,
				"coldObjectsTracked must reflect the snapshot gate")
			if tc.wantTracked {
				assert.NotNil(t, idx.coldObjects,
					"cache must be allocated when tracking is on")
			} else {
				assert.Nil(t, idx.coldObjects,
					"cache must stay nil when tracking is off")
			}
			assert.Same(t, tc.manager, idx.usageLimits,
				"manager must always be installed regardless of the gate")
		})
	}
}
