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
	"context"
	"fmt"
	"maps"
	"math"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"

	"github.com/weaviate/weaviate/entities/tenantactivity"
)

func newActivityTestIndex(className string, partitioningEnabled bool) *Index {
	return &Index{
		Config: IndexConfig{
			ClassName:         schema.ClassName(className),
			ReplicationFactor: 1,
		},
		closingCtx:          context.Background(),
		partitioningEnabled: partitioningEnabled,
		shards:              shardMap{},
		shardCreateLocks:    esync.NewKeyRWLocker(),
	}
}

func TestShardActivity(t *testing.T) {
	logger, _ := test.NewNullLogger()
	db := &DB{
		logger: logger,
		indices: map[string]*Index{
			"Col1":  newActivityTestIndex("Col1", true),
			"NonMT": newActivityTestIndex("NonMT", false),
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

		// t1's counter wrapped past MaxInt32 instead of growing, which does not
		// count as a read, so its read timestamp stays where t2's is even though
		// its total timestamp has moved on
		assert.Equal(t, col["t2_only_reads"], col["t1_overflow"],
			"a wrapped counter should not move the read timestamp")
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

	t.Run("unrecognized filter", func(t *testing.T) {
		usage := o.Usage(tenantactivity.UsageFilter(42))
		require.Len(t, usage["Col1"], 5, "falls back to the total usage")
		require.Equal(t, o.Usage(tenantactivity.UsageFilterAll), usage)
	})
}

// Observation buffers are recycled between cycles, so every cycle has to derive
// its whole answer from the counters as they stand: what disappeared is gone,
// and a timestamp only moves when the counter behind it moved.
func TestShardActivityAcrossCycles(t *testing.T) {
	// how a tenant should appear in one cycle: missing from the map, carrying a
	// timestamp from this cycle, or still carrying the one it already had
	const (
		absent = ""
		fresh  = "fresh"
		kept   = "kept"
	)

	type tenantWant struct{ all, read, write string }

	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	col2 := newActivityTestIndex("Col2", true)
	nonMT := newActivityTestIndex("NonMT", false)
	nonMT.shards.Store("shard1", &Shard{})

	db := &DB{
		logger:  logger,
		indices: map[string]*Index{"Col1": col1, "Col2": col2, "NonMT": nonMT},
	}
	o := newNodeWideMetricsObserver(db)

	require.Empty(t, o.Usage(tenantactivity.UsageFilterAll), "nothing observed yet")

	add := func(index *Index, name string, read, write int32) func() {
		return func() {
			shard := &Shard{}
			shard.activityTrackerRead.Store(read)
			shard.activityTrackerWrite.Store(write)
			index.shards.Store(name, shard)
		}
	}
	bump := func(index *Index, name string, read, write int32) func() {
		return func() {
			shard := index.shards.Load(name).(*Shard)
			shard.activityTrackerRead.Add(read)
			shard.activityTrackerWrite.Add(write)
		}
	}
	on := func(index *Index, name string, fn func(*Shard)) func() {
		return func() { fn(index.shards.Load(name).(*Shard)) }
	}

	cycles := []struct {
		name   string
		mutate func()
		want   map[string]map[string]tenantWant
	}{
		{
			name:   "no tenants",
			mutate: func() {},
			want:   map[string]map[string]tenantWant{"Col1": {}, "Col2": {}},
		},
		{
			name:   "tenant appears at its initial counters",
			mutate: add(col1, "t1", 1, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh}},
				"Col2": {},
			},
		},
		{
			name:   "idle cycle changes nothing",
			mutate: func() {},
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: kept}},
				"Col2": {},
			},
		},
		{
			name:   "tenant is read",
			mutate: bump(col1, "t1", 1, 0),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: fresh}},
				"Col2": {},
			},
		},
		{
			name:   "read timestamp survives an idle cycle",
			mutate: func() {},
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: kept, read: kept}},
				"Col2": {},
			},
		},
		{
			name:   "tenant is written to",
			mutate: bump(col1, "t1", 0, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: kept, write: fresh}},
				"Col2": {},
			},
		},
		{
			name:   "read and write in the same cycle",
			mutate: bump(col1, "t1", 1, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: fresh, write: fresh}},
				"Col2": {},
			},
		},
		{
			name:   "write counter reaches the maximum",
			mutate: on(col1, "t1", func(s *Shard) { s.activityTrackerWrite.Store(math.MaxInt32) }),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: kept, write: fresh}},
				"Col2": {},
			},
		},
		{
			name:   "write counter wraps instead of growing",
			mutate: on(col1, "t1", func(s *Shard) { s.activityTrackerWrite.Add(1) }),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: kept, write: kept}},
				"Col2": {},
			},
		},
		{
			name:   "read counter reaches the maximum",
			mutate: on(col1, "t1", func(s *Shard) { s.activityTrackerRead.Store(math.MaxInt32) }),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: fresh, write: kept}},
				"Col2": {},
			},
		},
		{
			name:   "read counter wraps instead of growing",
			mutate: on(col1, "t1", func(s *Shard) { s.activityTrackerRead.Add(1) }),
			want: map[string]map[string]tenantWant{
				"Col1": {"t1": {all: fresh, read: kept, write: kept}},
				"Col2": {},
			},
		},
		{
			name:   "tenant first seen one read above its initial counter",
			mutate: add(col1, "t2", 2, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: fresh, read: fresh},
				},
				"Col2": {},
			},
		},
		{
			name:   "tenant first seen one write above its initial counter",
			mutate: add(col1, "t3", 1, 2),
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t3": {all: fresh, write: fresh},
				},
				"Col2": {},
			},
		},
		{
			name:   "cold tenant reports no counters at all",
			mutate: func() { col1.shards.Store("t4", newColdShard(col1, "t4")) },
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t3": {all: kept, write: kept},
					"t4": {all: fresh},
				},
				"Col2": {},
			},
		},
		{
			name:   "activity in one collection leaves the other alone",
			mutate: add(col2, "t5", 3, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t3": {all: kept, write: kept},
					"t4": {all: kept},
				},
				"Col2": {"t5": {all: fresh, read: fresh}},
			},
		},
		{
			name:   "tenant removed",
			mutate: func() { col1.shards.LoadAndDelete("t3") },
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t4": {all: kept},
				},
				"Col2": {"t5": {all: kept, read: kept}},
			},
		},
		{
			// the tenant is new to the observer again, so the write timestamp it
			// used to have is gone rather than resurrected
			name:   "tenant returns at its initial counters",
			mutate: add(col1, "t3", 1, 1),
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t3": {all: fresh},
					"t4": {all: kept},
				},
				"Col2": {"t5": {all: kept, read: kept}},
			},
		},
		{
			name:   "collection removed",
			mutate: func() { delete(db.indices, "Col2") },
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: kept, read: kept, write: kept},
					"t2": {all: kept, read: kept},
					"t3": {all: kept},
					"t4": {all: kept},
				},
			},
		},
		{
			name:   "last collection removed",
			mutate: func() { delete(db.indices, "Col1") },
			want:   map[string]map[string]tenantWant{},
		},
		{
			// t1's counters wrapped into the negative earlier, so on re-sighting
			// they no longer read as activity beyond the initial value
			name: "collections return with their tenants",
			mutate: func() {
				db.indices["Col1"], db.indices["Col2"] = col1, col2
			},
			want: map[string]map[string]tenantWant{
				"Col1": {
					"t1": {all: fresh},
					"t2": {all: fresh, read: fresh},
					"t3": {all: fresh},
					"t4": {all: fresh},
				},
				"Col2": {"t5": {all: fresh, read: fresh}},
			},
		},
	}

	filters := []struct {
		name   string
		filter tenantactivity.UsageFilter
		pick   func(tenantWant) string
	}{
		{"all", tenantactivity.UsageFilterAll, func(w tenantWant) string { return w.all }},
		{"reads", tenantactivity.UsageFilterOnlyReads, func(w tenantWant) string { return w.read }},
		{"writes", tenantactivity.UsageFilterOnlyWrites, func(w tenantWant) string { return w.write }},
	}

	// the timestamps each filter reported last cycle, keyed by collection/tenant
	previous := map[string]map[string]time.Time{}

	for _, cycle := range cycles {
		t.Run(cycle.name, func(t *testing.T) {
			cycle.mutate()
			// consecutive cycles have to land on distinguishable timestamps
			time.Sleep(time.Millisecond)
			o.observeActivity()

			for _, f := range filters {
				usage := o.Usage(f.filter)
				require.Len(t, usage, len(cycle.want),
					"%s: only multi-tenant collections are reported", f.name)

				current := map[string]time.Time{}
				for class, tenants := range cycle.want {
					byTenant, ok := usage[class]
					require.True(t, ok, "%s: collection %s should be reported", f.name, class)

					want := map[string]string{}
					for tenant, w := range tenants {
						if expectation := f.pick(w); expectation != absent {
							want[tenant] = expectation
						}
					}
					require.ElementsMatch(t, slices.Collect(maps.Keys(want)),
						slices.Collect(maps.Keys(byTenant)), "%s: tenants of %s", f.name, class)

					for tenant, expectation := range want {
						key := class + "/" + tenant
						if expectation == fresh {
							require.NotEqual(t, previous[f.name][key], byTenant[tenant],
								"%s: %s should carry a timestamp from this cycle", f.name, key)
						} else {
							require.Equal(t, previous[f.name][key], byTenant[tenant],
								"%s: %s should keep its timestamp", f.name, key)
						}
						current[key] = byTenant[tenant]
					}
				}
				previous[f.name] = current
			}
		})
	}
}

// Usage hands out a copy because the observer keeps rewriting the maps it
// derives that copy from.
func TestShardActivityUsageIsIndependent(t *testing.T) {
	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	col1.shards.Store("t1", &Shard{})
	col1.shards.Store("t2", &Shard{})

	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	o := newNodeWideMetricsObserver(db)
	o.observeActivity()

	t.Run("callers cannot change what the observer reports", func(t *testing.T) {
		usage := o.Usage(tenantactivity.UsageFilterAll)
		delete(usage["Col1"], "t1")
		usage["Col1"]["injected"] = time.Now()

		fresh := o.Usage(tenantactivity.UsageFilterAll)
		require.Contains(t, fresh["Col1"], "t1")
		require.NotContains(t, fresh["Col1"], "injected")
	})

	t.Run("a later observation does not change an earlier result", func(t *testing.T) {
		earlier := o.Usage(tenantactivity.UsageFilterAll)
		snapshot := maps.Clone(earlier["Col1"])

		col1.shards.Store("t3", &Shard{})
		col1.shards.Load("t1").(*Shard).activityTrackerRead.Add(1)
		o.observeActivity()

		require.Equal(t, snapshot, earlier["Col1"])
	})

	t.Run("reads while observing", func(t *testing.T) {
		var wg sync.WaitGroup
		stop := make(chan struct{})

		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						for _, byTenant := range o.Usage(tenantactivity.UsageFilterOnlyReads) {
							for range byTenant {
							}
						}
					}
				}
			}()
		}

		for i := 0; i < 20; i++ {
			col1.shards.Load("t1").(*Shard).activityTrackerRead.Add(1)
			o.observeActivity()
		}
		close(stop)
		wg.Wait()
	})
}

// The observe cycle runs on every node every 10 seconds, so its allocations must
// not grow with the number of tenants.
func TestShardActivityObserveAllocations(t *testing.T) {
	const (
		tenants = 2000
		// steady state is ~41 bytes per tenant, and dropping either reuse buffer
		// costs 149 or more, so the budget sits between the two
		maxBytesPerTenant = 60
		runs              = 10
	)

	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	for i := 0; i < tenants; i++ {
		col1.shards.Store(fmt.Sprintf("tenant-%d", i), &Shard{})
	}

	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	o := newNodeWideMetricsObserver(db)

	// the buffers are only fully in place once a cycle has replaced a cycle
	for i := 0; i < 3; i++ {
		o.observeActivity()
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		o.observeActivity()
	}
	runtime.ReadMemStats(&after)

	perCycle := (after.TotalAlloc - before.TotalAlloc) / runs
	require.Less(t, perCycle, uint64(maxBytesPerTenant*tenants),
		"observing %d unchanged tenants allocated %d bytes per cycle", tenants, perCycle)
}

// A read or write filter reports only the tenants that saw that kind of
// activity, so on a large but mostly idle collection its answer has to cost what
// the matches cost, not what the collection costs.
func TestShardActivityUsageAllocations(t *testing.T) {
	const (
		tenants = 2000
		readers = 3
		writers = 2
		runs    = 10
		// a map holding every tenant costs ~98 bytes per tenant when it is sized
		// upfront and ~194 when it grows into it, so the budget sits between the
		// two, and a handful of matches has to stay far below either
		maxBytesFiltered  = 4096
		maxBytesPerTenant = 140
	)

	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	for i := 0; i < tenants; i++ {
		col1.shards.Store(fmt.Sprintf("tenant-%d", i), &Shard{})
	}

	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	o := newNodeWideMetricsObserver(db)
	o.observeActivity()

	for i := 0; i < readers; i++ {
		col1.shards.Load(fmt.Sprintf("tenant-%d", i)).(*Shard).activityTrackerRead.Add(1)
	}
	for i := 0; i < writers; i++ {
		col1.shards.Load(fmt.Sprintf("tenant-%d", i)).(*Shard).activityTrackerWrite.Add(1)
	}
	o.observeActivity()

	cases := []struct {
		name     string
		filter   tenantactivity.UsageFilter
		want     int
		maxBytes uint64
	}{
		{"only reads", tenantactivity.UsageFilterOnlyReads, readers, maxBytesFiltered},
		{"only writes", tenantactivity.UsageFilterOnlyWrites, writers, maxBytesFiltered},
		{"all tenants", tenantactivity.UsageFilterAll, tenants, maxBytesPerTenant * tenants},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// a budget on its own would also pass on an answer that is too small
			require.Len(t, o.Usage(tt.filter)["Col1"], tt.want)

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			for i := 0; i < runs; i++ {
				o.Usage(tt.filter)
			}
			runtime.ReadMemStats(&after)

			perCall := (after.TotalAlloc - before.TotalAlloc) / runs
			require.Less(t, perCall, tt.maxBytes,
				"reporting %d of %d tenants allocated %d bytes per call", tt.want, tenants, perCall)
		})
	}
}

func TestShardActivityUsageWithoutObserver(t *testing.T) {
	var o *nodeWideMetricsObserver
	require.Empty(t, o.Usage(tenantactivity.UsageFilterAll))
}

// newColdShard returns an unloaded LazyLoadShard whose every load attempt fails,
// so a force-load panics through mustLoad instead of passing unnoticed.
func newColdShard(index *Index, name string) *LazyLoadShard {
	return &LazyLoadShard{
		shardOpts:  &deferredShardOpts{name: name, index: index},
		memMonitor: failingAllocChecker{},
	}
}

// Tenant activity is polled for every shard on the node, so a cold tenant has to
// be observable without being pulled into memory.
func TestShardActivityColdShard(t *testing.T) {
	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	cold := newColdShard(col1, "t_cold")
	col1.shards.Store("t_cold", cold)

	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	o := newNodeWideMetricsObserver(db)

	t.Run("observing does not load it", func(t *testing.T) {
		require.NotPanics(t, func() {
			for i := 0; i < 3; i++ {
				o.observeActivity()
			}
		})
		require.False(t, cold.isLoaded(), "a cold tenant must stay cold")
	})

	t.Run("counted as a tenant but never as a read or a write", func(t *testing.T) {
		require.Contains(t, o.Usage(tenantactivity.UsageFilterAll)["Col1"], "t_cold")
		require.NotContains(t, o.Usage(tenantactivity.UsageFilterOnlyReads)["Col1"], "t_cold")
		require.NotContains(t, o.Usage(tenantactivity.UsageFilterOnlyWrites)["Col1"], "t_cold")
	})

	t.Run("timestamp does not drift while it stays cold", func(t *testing.T) {
		first := o.Usage(tenantactivity.UsageFilterAll)["Col1"]["t_cold"]
		time.Sleep(10 * time.Millisecond)
		o.observeActivity()
		require.Equal(t, first, o.Usage(tenantactivity.UsageFilterAll)["Col1"]["t_cold"])
	})
}

// A tenant that loads starts reporting the initial counters of 1 after the zeros
// it reported while cold. The observer reads that step as a read and a write,
// where a tenant that was already loaded on first sight logs an activation.
func TestShardActivityColdShardLoads(t *testing.T) {
	logger, _ := test.NewNullLogger()
	col1 := newActivityTestIndex("Col1", true)
	cold := newColdShard(col1, "t_cold")
	col1.shards.Store("t_cold", cold)

	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	o := newNodeWideMetricsObserver(db)
	o.observeActivity()
	require.NotContains(t, o.Usage(tenantactivity.UsageFilterOnlyReads)["Col1"], "t_cold")

	loaded := &Shard{}
	loaded.activityTrackerRead.Store(1)
	loaded.activityTrackerWrite.Store(1)
	cold.mutex.Lock()
	cold.shard, cold.loaded = loaded, true
	cold.mutex.Unlock()

	o.observeActivity()

	require.Contains(t, o.Usage(tenantactivity.UsageFilterOnlyReads)["Col1"], "t_cold")
	require.Contains(t, o.Usage(tenantactivity.UsageFilterOnlyWrites)["Col1"], "t_cold")
}

// Tenant activity logs are a configurable signal operators rely on, so every
// branch has to keep emitting the entry it is responsible for.
func TestShardActivityLogging(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	col1 := newActivityTestIndex("Col1", true)
	db := &DB{logger: logger, indices: map[string]*Index{"Col1": col1}}
	db.config.TenantActivityReadLogLevel = configRuntime.NewDynamicValue("info")
	db.config.TenantActivityWriteLogLevel = configRuntime.NewDynamicValue("info")
	o := newNodeWideMetricsObserver(db)

	addTenant := func(name string, read, write int32) func() {
		return func() {
			shard := &Shard{}
			shard.activityTrackerRead.Store(read)
			shard.activityTrackerWrite.Store(write)
			col1.shards.Store(name, shard)
		}
	}
	bump := func(name string, read, write int32) func() {
		return func() {
			shard := col1.shards.Load(name).(*Shard)
			shard.activityTrackerRead.Add(read)
			shard.activityTrackerWrite.Add(write)
		}
	}

	cycles := []struct {
		name   string
		mutate func()
		want   map[string]string
	}{
		{
			name:   "new tenant at its initial counters",
			mutate: addTenant("t_activation", 1, 1),
			want:   map[string]string{"t_activation": "activation"},
		},
		{
			name:   "new tenant that has already been read",
			mutate: addTenant("t_new_read", 5, 1),
			want:   map[string]string{"t_new_read": "read"},
		},
		{
			name:   "new tenant that has already been written to",
			mutate: addTenant("t_new_write", 1, 5),
			want:   map[string]string{"t_new_write": "write"},
		},
		{
			name:   "known tenant is read",
			mutate: bump("t_activation", 1, 0),
			want:   map[string]string{"t_activation": "read"},
		},
		{
			name:   "known tenant is written to",
			mutate: bump("t_activation", 0, 1),
			want:   map[string]string{"t_activation": "write"},
		},
		{
			// a cold tenant reports zeros, which is below the initial counters and
			// so is not an activation either
			name:   "cold tenant that reports no counters",
			mutate: func() { col1.shards.Store("t_cold", newColdShard(col1, "t_cold")) },
			want:   map[string]string{},
		},
		{
			name:   "nothing happened",
			mutate: func() {},
			want:   map[string]string{},
		},
	}

	for _, cycle := range cycles {
		t.Run(cycle.name, func(t *testing.T) {
			hook.Reset()
			cycle.mutate()
			o.observeActivity()

			logged := map[string]string{}
			for _, entry := range hook.AllEntries() {
				if entry.Data["action"] != "tenant_activity_change" {
					continue
				}
				logged[entry.Data["tenant"].(string)] = entry.Data["activity_type"].(string)
			}
			require.Equal(t, cycle.want, logged)
		})
	}
}
