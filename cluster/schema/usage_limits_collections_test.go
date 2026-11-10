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

package schema

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func TestPreApplyFilter_CollectionCap(t *testing.T) {
	const nodeID = "testNode"

	newManager := func(t *testing.T, cap int) *SchemaManager {
		t.Helper()
		sm := NewSchemaManager(nodeID, nil, nil, prometheus.NewPedanticRegistry(), logrus.New())
		sm.SetCollectionLimit(func() int { return cap }, nil)
		return sm
	}

	// seed creates n classes named <prefix>C<i>.
	seed := func(t *testing.T, sm *SchemaManager, prefix string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			require.NoError(t, sm.schema.addClass(&models.Class{Class: fmt.Sprintf("%sC%d", prefix, i)},
				&sharding.State{Physical: make(map[string]sharding.Physical)}, 0))
		}
	}

	preApply := func(sm *SchemaManager, typ api.ApplyRequest_Type, class string) error {
		return sm.PreApplyFilter(&api.ApplyRequest{Type: typ, Class: class})
	}
	addClass := func(sm *SchemaManager, class string) error {
		return preApply(sm, api.ApplyRequest_TYPE_ADD_CLASS, class)
	}

	requireRejected := func(t *testing.T, err error, wantCap int64) {
		t.Helper()
		require.Error(t, err)
		le, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok, "expected a typed LimitExceededError, got %v", err)
		assert.Equal(t, usagelimits.LimitCollections, le.Limit)
		assert.Equal(t, wantCap, le.Value)
	}

	t.Run("under the cap is allowed", func(t *testing.T) {
		sm := newManager(t, 2)
		seed(t, sm, "ns1:", 1)
		require.NoError(t, addClass(sm, "ns1:New"))
	})

	t.Run("at the cap is rejected without mutating state", func(t *testing.T) {
		sm := newManager(t, 2)
		seed(t, sm, "ns1:", 2)
		requireRejected(t, addClass(sm, "ns1:New"), 2)
		assert.Equal(t, 2, sm.schema.CollectionsCount("ns1"), "PreApplyFilter must not mutate the schema")
	})

	t.Run("a full namespace does not block another", func(t *testing.T) {
		sm := newManager(t, 2)
		seed(t, sm, "ns2:", 2)
		require.NoError(t, addClass(sm, "ns1:New"))
	})

	t.Run("an unqualified class counts against the global total", func(t *testing.T) {
		sm := newManager(t, 2)
		seed(t, sm, "", 2)
		requireRejected(t, addClass(sm, "New"), 2)
	})

	t.Run("a duplicate at the cap answers already exists", func(t *testing.T) {
		sm := newManager(t, 1)
		seed(t, sm, "ns1:", 1)
		err := addClass(sm, "ns1:C0")
		require.ErrorContains(t, err, "already exists")
		_, isLimit := usagelimits.AsLimitExceeded(err)
		assert.False(t, isLimit)
	})

	t.Run("negative cap means unlimited", func(t *testing.T) {
		sm := newManager(t, -1)
		seed(t, sm, "ns1:", 5)
		require.NoError(t, addClass(sm, "ns1:New"))
	})

	t.Run("no resolver installed is unenforced", func(t *testing.T) {
		sm := newManager(t, 0)
		sm.collectionLimit = nil
		require.NoError(t, addClass(sm, "ns1:New"))
	})

	t.Run("a delete in a full namespace frees a slot", func(t *testing.T) {
		sm := newManager(t, 2)
		seed(t, sm, "ns1:", 2)
		requireRejected(t, addClass(sm, "ns1:New"), 2)
		require.True(t, sm.schema.deleteClass("ns1:C0"))
		require.NoError(t, addClass(sm, "ns1:New"))
	})

	t.Run("restore is not capped", func(t *testing.T) {
		sm := newManager(t, 0)
		require.NoError(t, preApply(sm, api.ApplyRequest_TYPE_RESTORE_CLASS, "ns1:Restored"))
	})

	t.Run("operator error template renders in the pre-commit rejection", func(t *testing.T) {
		sm := newManager(t, 0)
		sm.collectionLimitErrTemplate = func() string { return "Free-tier limit of {value} {limit} reached, upgrade now" }
		le, ok := usagelimits.AsLimitExceeded(addClass(sm, "ns1:New"))
		require.True(t, ok)
		assert.Equal(t, "Free-tier limit of 0 collections reached, upgrade now", le.RenderedMessage)
	})
}
