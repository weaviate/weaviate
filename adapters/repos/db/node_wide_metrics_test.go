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
	"github.com/weaviate/weaviate/entities/tenantactivity"
)

func TestShardActivity(t *testing.T) {
	logger, _ := test.NewNullLogger()
	db := &DB{
		logger: logger,
		indices: map[string]*Index{
			"Col1": {
				Config: IndexConfig{
					ClassName:         "Col1",
					ReplicationFactor: 1,
				},
				partitioningEnabled: true,
				shards:              shardMap{},
			},
			"NonMT": {
				Config: IndexConfig{
					ClassName:         "NonMT",
					ReplicationFactor: 1,
				},
				partitioningEnabled: false,
				shards:              shardMap{},
			},
		},
	}

	db.indices["Col1"].shards.Store("t1_overflow", &Shard{})
	db.indices["Col1"].shards.Store("t2_only_reads", &Shard{})
	db.indices["Col1"].shards.Store("t3_no_reads_and_writes", &Shard{})
	db.indices["Col1"].shards.Store("t4_only_writes", &Shard{})
	db.indices["Col1"].shards.Store("t5_reads_and_writes", &Shard{})
	o := newNodeWideMetricsObserver(db)

	o.observeActivity()

	// show activity on two tenants
	time.Sleep(10 * time.Millisecond)
	db.indices["Col1"].shards.Load("t1_overflow").(*Shard).activityTrackerRead.Store(math.MaxInt32)
	db.indices["Col1"].shards.Load("t2_only_reads").(*Shard).activityTrackerRead.Add(1)
	db.indices["Col1"].shards.Load("t4_only_writes").(*Shard).activityTrackerWrite.Add(1)
	db.indices["Col1"].shards.Load("t5_reads_and_writes").(*Shard).activityTrackerRead.Add(1)
	db.indices["Col1"].shards.Load("t5_reads_and_writes").(*Shard).activityTrackerWrite.Add(1)

	// observe to update timestamps
	o.observeActivity()

	// show activity again on one tenant (should now have the latest timestamp
	time.Sleep(10 * time.Millisecond)
	// previous value was math.MaxInt32, so this counter will overflow now.
	// Assert that everything still works as expected
	db.indices["Col1"].shards.Load("t1_overflow").(*Shard).activityTrackerRead.Add(1)
	o.observeActivity()

	t.Run("total usage", func(t *testing.T) {
		usage := o.Usage(tenantactivity.UsageFilterAll)
		_, ok := usage["NonMT"]
		assert.False(t, ok, "only MT cols should be contained")

		col, ok := usage["Col1"]
		require.True(t, ok, "MT col should be contained")
		require.Len(t, col, 5, "all 5 tenants should be contained")
		assert.True(t, col["t1_overflow"].After(col["t2_only_reads"]), "t1 should have a newer timestamp than t2")
		assert.True(t, col["t2_only_reads"].After(col["t3_no_reads_and_writes"]), "t2 should have a newer timestamp than t3")
		assert.True(t, col["t4_only_writes"].After(col["t3_no_reads_and_writes"]), "t4 should have a newer timestamp than t3")
		assert.True(t, col["t5_reads_and_writes"].After(col["t3_no_reads_and_writes"]), "t4 should have a newer timestamp than t3")
	})

	t.Run("display only reads", func(t *testing.T) {
		usage := o.Usage(tenantactivity.UsageFilterOnlyReads)
		_, ok := usage["NonMT"]
		assert.False(t, ok, "only MT cols should be contained")

		col, ok := usage["Col1"]
		require.True(t, ok, "MT col should be contained")
		require.Len(t, col, 3, "all tenants which received reads should be contained")

		// tenants with reads
		_, ok = col["t1_overflow"]
		assert.True(t, ok, "t1 should be contained")
		_, ok = col["t2_only_reads"]
		assert.True(t, ok, "t2 should be contained")
		_, ok = col["t5_reads_and_writes"]
		assert.True(t, ok, "t5 should be contained")

		// tenants without reads
		_, ok = col["t3_no_reads_and_writes"]
		assert.False(t, ok, "t3 should not be contained")
		_, ok = col["t4_only_writes"]
		assert.False(t, ok, "t4 should not be contained")
	})

	t.Run("display only writes", func(t *testing.T) {
		usage := o.Usage(tenantactivity.UsageFilterOnlyWrites)
		_, ok := usage["NonMT"]
		assert.False(t, ok, "only MT cols should be contained")

		col, ok := usage["Col1"]
		require.True(t, ok, "MT col should be contained")
		require.Len(t, col, 2, "all tenants which received reads should be contained")

		// tenants with writes
		_, ok = col["t4_only_writes"]
		assert.True(t, ok, "t4 should be contained")
		// tenants with writes
		_, ok = col["t5_reads_and_writes"]
		assert.True(t, ok, "t5 should be contained")

		// write into t5 again
		db.indices["Col1"].shards.Load("t5_reads_and_writes").(*Shard).activityTrackerWrite.Add(1)
		time.Sleep(10 * time.Millisecond)
		o.observeActivity()

		usage = o.Usage(tenantactivity.UsageFilterOnlyWrites)
		col, ok = usage["Col1"]
		require.True(t, ok, "MT col should be contained")

		assert.True(t, col["t5_reads_and_writes"].After(col["t4_only_writes"]), "t5 should have a newer timestamp than t4")
	})
}
