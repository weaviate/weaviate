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
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// gaugeUntouched is a value the sum can never produce, so a test that pre-sets
// the gauge to it can tell "published this total" from "published nothing".
const gaugeUntouched = -1

// newObjectCountTestIndex builds an index observeObjectCount can walk. It
// creates each shard's tenant directory, because indexObjectCount skips a shard
// whose directory is missing.
func newObjectCountTestIndex(t *testing.T, className string, shards map[string]ShardLike) *Index {
	t.Helper()

	logger, _ := test.NewNullLogger()
	index := newActivityTestIndex(className, true)
	index.logger = logger
	index.Config.RootPath = t.TempDir()
	index.closeRequestedCtx, index.signalCloseRequested = context.WithCancelCause(context.Background())
	index.allShardsReady.Store(true)

	for name, shard := range shards {
		require.NoError(t, os.MkdirAll(shardPath(index.path(), name), 0o755))
		index.shards.Store(name, shard)
	}
	return index
}

// newObjectCountObserver returns an observer over the given indices, the DB it
// reads them from, and the gauge observeObjectCount publishes into, pre-set to
// gaugeUntouched.
func newObjectCountObserver(indices ...*Index) (*nodeWideMetricsObserver, *DB, prometheus.Gauge) {
	logger, _ := test.NewNullLogger()
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "object_count"},
		[]string{"class_name", "shard_name"})
	gauge := gaugeVec.WithLabelValues("n/a", "n/a")
	gauge.Set(gaugeUntouched)

	byName := make(map[string]*Index, len(indices))
	for _, index := range indices {
		byName[index.ID()] = index
	}
	db := &DB{
		logger:      logger,
		indices:     byName,
		promMetrics: &monitoring.PrometheusMetrics{ObjectCount: gaugeVec},
	}
	return newNodeWideMetricsObserver(db), db, gauge
}

// countingShard reports count and never fails. Its expectation is Maybe(),
// because a walk that stops early leaves the shards behind it uncounted.
func countingShard(t *testing.T, count int64) *MockShardLike {
	t.Helper()

	shard := NewMockShardLike(t)
	shard.EXPECT().ObjectCountAsync(mock.Anything).Return(count, nil).Maybe()
	return shard
}

// untouchedShard fails the test if anything asks it for a count. An expectation
// cannot do that, because testify caps a call count from above, never at zero.
func untouchedShard(t *testing.T) *MockShardLike {
	t.Helper()

	return NewMockShardLike(t)
}

func TestObserveObjectCount(t *testing.T) {
	tests := []struct {
		name    string
		indices func(t *testing.T) []*Index
		want    float64
	}{
		{
			name: "sums every shard of every index",
			indices: func(t *testing.T) []*Index {
				return []*Index{
					newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
						"tenant-0": countingShard(t, 3),
						"tenant-1": countingShard(t, 4),
					}),
					newObjectCountTestIndex(t, "Col2", map[string]ShardLike{
						"tenant-0": countingShard(t, 5),
					}),
				}
			},
			want: 12,
		},
		{
			name: "no index at all publishes zero",
			indices: func(t *testing.T) []*Index {
				return nil
			},
			want: 0,
		},
		{
			name: "an index with no shards publishes zero",
			indices: func(t *testing.T) []*Index {
				return []*Index{newObjectCountTestIndex(t, "Col1", nil)}
			},
			want: 0,
		},
		{
			name: "one index short of ready leaves every shard unread",
			indices: func(t *testing.T) []*Index {
				pending := newObjectCountTestIndex(t, "Col2", map[string]ShardLike{
					"tenant-0": untouchedShard(t),
				})
				pending.allShardsReady.Store(false)
				return []*Index{
					newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
						"tenant-0": untouchedShard(t),
					}),
					pending,
				}
			},
			want: gaugeUntouched,
		},
		{
			name: "a collection closed with no recorded cause skips the tick",
			indices: func(t *testing.T) []*Index {
				closed := newObjectCountTestIndex(t, "Col2", map[string]ShardLike{
					"tenant-0": untouchedShard(t),
				})
				closed.closed = true
				return []*Index{
					newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
						"tenant-0": countingShard(t, 3),
					}),
					closed,
				}
			},
			want: gaugeUntouched,
		},
		{
			name: "a collection closed by a delete counts zero, the rest still count",
			indices: func(t *testing.T) []*Index {
				deleted := newObjectCountTestIndex(t, "Col2", map[string]ShardLike{
					"tenant-0": untouchedShard(t),
				})
				deleted.signalCloseRequested(errIndexDropped)
				deleted.closed = true
				return []*Index{
					newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
						"tenant-0": countingShard(t, 3),
					}),
					deleted,
				}
			},
			want: 3,
		},
		{
			name: "a shard whose directory is gone is skipped",
			indices: func(t *testing.T) []*Index {
				index := newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
					"tenant-0": countingShard(t, 3),
					"tenant-1": untouchedShard(t),
				})
				require.NoError(t, os.RemoveAll(shardPath(index.path(), "tenant-1")))
				return []*Index{index}
			},
			want: 3,
		},
		{
			name: "a shard that fails to report its count contributes zero",
			indices: func(t *testing.T) []*Index {
				failing := NewMockShardLike(t)
				failing.EXPECT().ObjectCountAsync(mock.Anything).
					Return(0, errors.New("segment metadata unreadable")).Maybe()
				return []*Index{
					newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
						"tenant-0": countingShard(t, 3),
						"tenant-1": failing,
					}),
				}
			},
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, _, gauge := newObjectCountObserver(tt.indices(t)...)

			o.observeObjectCount()

			require.Equal(t, tt.want, testutil.ToFloat64(gauge))
		})
	}
}

func TestObserveObjectCountReleasesIndexLock(t *testing.T) {
	counting := make(chan struct{})
	release := make(chan struct{})
	// releaseOnce is deferred as well as called, so a failed assertion below
	// still lets the walk finish instead of leaving it parked on release.
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	shard := NewMockShardLike(t)
	shard.EXPECT().ObjectCountAsync(mock.Anything).RunAndReturn(
		func(context.Context) (int64, error) {
			close(counting)
			<-release
			return 3, nil
		})

	index := newObjectCountTestIndex(t, "Col1", map[string]ShardLike{"tenant-0": shard})
	o, db, gauge := newObjectCountObserver(index)

	observed := make(chan struct{})
	go func() {
		defer close(observed)
		o.observeObjectCount()
	}()

	<-counting
	// The walk takes longer the more tenants the node has, because a cold shard
	// reads its count off disk. Every index lookup takes indexLock for read and
	// DeleteIndex takes it for write.
	require.True(t, db.indexLock.TryLock(), "indexLock must be free while shards are counted")
	db.indexLock.Unlock()

	releaseOnce()
	<-observed
	require.Equal(t, float64(3), testutil.ToFloat64(gauge))
}

func TestObserveObjectCountWalkStoppedByCloseRequest(t *testing.T) {
	tests := []struct {
		name   string
		cause  error
		shards []string
		// countAwaitsCancel holds the shard's count open until closeRequestedCtx
		// reaches walkCtx, as a shard honouring its context would. Both real
		// ObjectCountAsync implementations ignore it and return at once, which is
		// the shape the one-shard rows below use.
		countAwaitsCancel bool
		want              float64
	}{
		{
			name:              "a collection being deleted counts zero, the rest still publish",
			cause:             errIndexDropped,
			shards:            []string{"tenant-0", "tenant-1"},
			countAwaitsCancel: true,
			want:              100,
		},
		{
			name:              "a collection shutting down skips the whole tick",
			cause:             errIndexShutdown,
			shards:            []string{"tenant-0", "tenant-1"},
			countAwaitsCancel: true,
			want:              gaugeUntouched,
		},
		// A close during the last shard leaves no callback to observe it, so the
		// walk checks the close request again after ForEachShard returns.
		{
			name:   "a delete during the only shard still counts that collection zero",
			cause:  errIndexDropped,
			shards: []string{"tenant-0"},
			want:   100,
		},
		{
			name:   "a shutdown during the only shard still skips the whole tick",
			cause:  errIndexShutdown,
			shards: []string{"tenant-0"},
			want:   gaugeUntouched,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stopping *Index
			var once sync.Once
			var counted atomic.Int32

			// Whichever shard the walk reaches first requests the close, so no shard
			// behind it is counted, whatever order the shards come in.
			requestClose := func(ctx context.Context) (int64, error) {
				counted.Add(1)
				once.Do(func() { stopping.signalCloseRequested(tt.cause) })
				if !tt.countAwaitsCancel {
					return 5, nil
				}
				select {
				case <-ctx.Done():
					return 5, nil
				case <-time.After(2 * time.Second):
					return 0, errors.New("walk context outlived the close request")
				}
			}

			shards := map[string]ShardLike{}
			for _, name := range tt.shards {
				shard := NewMockShardLike(t)
				shard.EXPECT().ObjectCountAsync(mock.Anything).RunAndReturn(requestClose).Maybe()
				shards[name] = shard
			}
			stopping = newObjectCountTestIndex(t, "Col2", shards)

			healthy := newObjectCountTestIndex(t, "Col1", map[string]ShardLike{
				"tenant-0": countingShard(t, 100),
			})
			o, _, gauge := newObjectCountObserver(healthy, stopping)

			o.observeObjectCount()

			require.Equal(t, int32(1), counted.Load(),
				"the walk must count exactly the shards ahead of the close request")
			require.Equal(t, tt.want, testutil.ToFloat64(gauge))
		})
	}
}
