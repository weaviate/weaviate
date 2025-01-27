//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func Test_schemaCollectionMetrics(t *testing.T) {
	r := prometheus.NewPedanticRegistry()

	s := NewSchema("testNode", nil, r)
	ss := &sharding.State{}

	c1 := &models.Class{
		Class: "collection1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	c2 := &models.Class{
		Class: "collection2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	// Collection metrics
	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount.WithLabelValues("testNode")))
	require.NoError(t, s.addClass(c1, ss, 0)) // adding c1 collection
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	require.NoError(t, s.addClass(c2, ss, 0)) // adding c2 collection
	assert.Equal(t, float64(2), testutil.ToFloat64(s.collectionsCount))

	// delete c2
	s.deleteClass("collection2")
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	// delete c1
	s.deleteClass("collection1")
	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount))
}

func Test_schemaShardMetrics(t *testing.T) {
	r := prometheus.NewPedanticRegistry()

	s := NewSchema("testNode", nil, r)
	ss := &sharding.State{}

	c1 := &models.Class{
		Class: "collection1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	c2 := &models.Class{
		Class: "collection2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	require.NoError(t, s.addClass(c1, ss, 0)) // adding c1 collection
	require.NoError(t, s.addClass(c2, ss, 0)) // adding c2 collection

	// Shard metrics
	// no shards now.
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "")))

	// add shard to c1 collection
	err := s.addTenants(c1.Class, 0, &api.AddTenantsRequest{
		ClusterNodes: []string{"testNode"},
		Tenants: []*api.Tenant{
			{
				Name:   "tenant1",
				Status: "HOT",
			},
			nil, // nil tenant shouldn't be counted in the metrics
		},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "HOT")))

	// add shard to c2 collection
	err = s.addTenants(c2.Class, 0, &api.AddTenantsRequest{
		ClusterNodes: []string{"testNode"},
		Tenants: []*api.Tenant{
			{
				Name:   "tenant2",
				Status: "FROZEN",
			},
			nil, // nil tenant shouldn't be counted in the metrics
		},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "FROZEN")))

	// delete "existing" tenant
	err = s.deleteTenants(c1.Class, 0, &api.DeleteTenantsRequest{
		Tenants: []string{"tenant1"},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "HOT")))
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "FROZEN")))

	// delete "non-existing" tenant
	err = s.deleteTenants(c1.Class, 0, &api.DeleteTenantsRequest{
		Tenants: []string{"tenant1"},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "HOT")))
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("testNode", "FROZEN")))
}
