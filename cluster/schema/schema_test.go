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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func Test_schemaMetrics(t *testing.T) {
	s := NewSchema("testNode", nil)
	ss := &sharding.State{}

	c1 := &models.Class{
		Class: "collection1",
	}
	c2 := &models.Class{
		Class: "collection2",
	}

	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount.WithLabelValues("testNode")))
	require.NoError(t, s.addClass(c1, ss, 0)) // adding c1 collection
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	require.NoError(t, s.addClass(c2, ss, 0))
	assert.Equal(t, float64(2), testutil.ToFloat64(s.collectionsCount)) // adding c2 collection

	// delete c2
	s.deleteClass("collection2")
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	// delete c1
	s.deleteClass("collection2")
	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount))

	// should un-register the metrics. So that creating new schema shouldn't panic with duplicate metrics
	s.Close()
	assert.NotPanics(t, func() {
		NewSchema("testNode", nil) // creating new schema
	})
}
