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

package db

import (
	"math"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardActivity(t *testing.T) {
	logger, _ := test.NewNullLogger()
	db := &DB{
		logger: logger,
		indices: map[string]*Index{
			"Col1": {
				Config: IndexConfig{
					ClassName: "Col1",
				},
				partitioningEnabled: true,
				shards:              shardMap{},
			},
			"NonMT": {
				Config: IndexConfig{
					ClassName: "NonMT",
				},
				partitioningEnabled: false,
				shards:              shardMap{},
			},
		},
	}

	db.indices["Col1"].shards.Store("t1", &Shard{})
	db.indices["Col1"].shards.Store("t2", &Shard{})
	db.indices["Col1"].shards.Store("t3", &Shard{})
	o := newNodeWideMetricsObserver(db)

	o.observeActivity()

	// show activity on two tenants
	time.Sleep(10 * time.Millisecond)
	db.indices["Col1"].shards.Load("t1").(*Shard).activityTracker.Store(math.MaxInt32)
	db.indices["Col1"].shards.Load("t2").(*Shard).activityTracker.Add(1)

	// observe to update timestamps
	o.observeActivity()

	// show activity again on one tenant (should now have the latest timestamp
	time.Sleep(10 * time.Millisecond)
	// previous value was math.MaxInt32, so this counter will overflow now.
	// Assert that everything still works as expected
	db.indices["Col1"].shards.Load("t1").(*Shard).activityTracker.Add(1)
	o.observeActivity()

	usage := o.Usage()
	_, ok := usage["NonMT"]
	assert.False(t, ok, "only MT cols should be contained")

	col, ok := usage["Col1"]
	require.True(t, ok, "MT col should be contained")
	require.Len(t, col, 3, "all 3 tenants should be contained")
	assert.True(t, col["t1"].After(col["t2"]), "t1 should have a newer timestamp than t2")
	assert.True(t, col["t2"].After(col["t3"]), "t2 should have a newer timestamp than t3")
}
