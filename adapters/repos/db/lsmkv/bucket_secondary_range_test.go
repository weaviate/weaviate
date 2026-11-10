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

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TestBucketGetBySecondaryRange: the ranged read finds
// the value where GetBySecondary finds it, in the memtable, in a segment,
// under a newer version or a deletion, and returns the picked bytes of it.
func TestBucketGetBySecondaryRange(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	key, key2, secKey, secKey2 := []byte("k1"), []byte("k2"), []byte("s1"), []byte("s2")
	value := make([]byte, 1000)
	for i := range value {
		value[i] = byte(i)
	}
	value2 := make([]byte, 700)
	for i := range value2 {
		value2[i] = byte(255 - i%256)
	}
	withSec := WithSecondaryKey(0, secKey)

	// a picker that names a fixed range
	fixed := func(offset, length uint64) ValueRangePicker {
		return func(head []byte) (uint64, uint64, uint64, error) {
			return offset, length, 0, nil
		}
	}
	// a picker that needs the value's first 600 bytes, then names the byte
	// after them, checking what it was given on the way
	growing := func(t *testing.T) ValueRangePicker {
		return func(head []byte) (uint64, uint64, uint64, error) {
			require.Equal(t, value[:len(head)], head, "a head must be the value's first bytes")
			if len(head) < 600 {
				return 0, 0, 600, nil
			}
			return 599, 1, 0, nil
		}
	}

	tests := []struct {
		name    string
		setup   func(t *testing.T, b *Bucket)
		head    uint64
		pick    func(t *testing.T) ValueRangePicker
		want    []byte // nil: absent or deleted
		wantErr string
	}{
		{
			name:  "in the memtable",
			setup: func(t *testing.T, b *Bucket) { require.NoError(t, b.Put(key, value, withSec)) },
			head:  16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: value[100:150],
		},
		{
			name: "in a segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: value[100:150],
		},
		{
			name: "the whole value from a segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 0, pick: func(*testing.T) ValueRangePicker { return fixed(0, uint64(len(value))) },
			want: value,
		},
		{
			name: "an empty range",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(10, 0) },
			want: []byte{},
		},
		{
			name: "updated in a newer segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
				require.NoError(t, b.Put(key, value2, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: value2[100:150],
		},
		{
			name: "updated in the memtable over a segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
				require.NoError(t, b.Put(key, value2, withSec))
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: value2[100:150],
		},
		{
			// the primary-key recheck: the old node is still indexed under
			// the old secondary key, a newer segment holds the key
			name: "secondary key moved in a newer segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
				require.NoError(t, b.Put(key, value2, WithSecondaryKey(0, secKey2)))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: nil,
		},
		{
			name: "deleted in the memtable over a segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
				require.NoError(t, b.Delete(key, withSec))
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: nil,
		},
		{
			name: "deleted in a newer segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
				require.NoError(t, b.Delete(key, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: nil,
		},
		{
			name:  "absent",
			setup: func(t *testing.T, b *Bucket) { require.NoError(t, b.Put(key2, value, WithSecondaryKey(0, secKey2))) },
			head:  16, pick: func(*testing.T) ValueRangePicker { return fixed(100, 50) },
			want: nil,
		},
		{
			name: "head grows on the picker's request, in the memtable",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
			},
			head: 16, pick: growing,
			want: value[599:600],
		},
		{
			name: "head grows on the picker's request, in a segment",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: growing,
			want: value[599:600],
		},
		{
			name: "range past the value",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker { return fixed(990, 20) },
			wantErr: "outside the value",
		},
		{
			name: "head longer than the value",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker {
				return func(head []byte) (uint64, uint64, uint64, error) { return 0, 0, 5000, nil }
			},
			wantErr: "the value has 1000",
		},
		{
			name: "a picker that never has enough",
			setup: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(key, value, withSec))
				require.NoError(t, b.FlushMemtable())
			},
			head: 16, pick: func(*testing.T) ValueRangePicker {
				return func(head []byte) (uint64, uint64, uint64, error) { return 0, 0, uint64(len(head)) + 1, nil }
			},
			wantErr: fmt.Sprintf("value head still short after %d reads (%d bytes asked)", maxHeadRounds, 16+maxHeadRounds),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyReplace), WithSecondaryIndices(1))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })
			tt.setup(t, b)

			got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, tt.head, tt.pick(t), nil)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)

			// the plain read agrees on presence
			whole, err := b.GetBySecondary(ctx, 0, secKey)
			require.NoError(t, err)
			require.Equal(t, tt.want == nil, whole == nil)
		})
	}

	// a stored object whose sections before the asked multi-vector are each
	// longer than the head read before them, read with the multi-vector
	// locator as the shard does, from the memtable and from a segment
	t.Run("a multi-vector object with several long sections", func(t *testing.T) {
		long := make([][]float32, 50)
		for i := range long {
			long[i] = []float32{float32(i), 1, 2}
		}
		multi := map[string][][]float32{"a": long, "b": {{1, 2, 3}, {4, 5, 6}}}
		obj := storobj.FromObject(&models.Object{
			Class:      "D" + strings.Repeat("c", 250),
			ID:         strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties: map[string]interface{}{"text": strings.Repeat("x", 1000)},
		}, make([]float32, 300), map[string][]float32{"t": make([]float32, 64)}, multi)
		bin, err := obj.MarshalBinaryDisk(false)
		require.NoError(t, err)

		for _, flush := range []bool{false, true} {
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyReplace), WithSecondaryIndices(1))
			require.NoError(t, err)
			// shut down on a failure too; the second Shutdown below is then
			// skipped
			shut := false
			t.Cleanup(func() {
				if !shut {
					_ = b.Shutdown(context.Background())
				}
			})
			require.NoError(t, b.Put(key, bin, withSec))
			if flush {
				require.NoError(t, b.FlushMemtable())
			}
			for name, tokens := range multi {
				for pos, want := range tokens {
					pick := func(head []byte) (uint64, uint64, uint64, error) {
						return storobj.MultiVectorTokenRange(head, name, pos, len(want))
					}
					got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, storobj.MultiVectorLocatorHead, pick, nil)
					require.NoError(t, err, "token %d of %q, flushed %v", pos, name, flush)
					token, err := storobj.TokenFromBytes(got, len(want), nil)
					require.NoError(t, err)
					require.Equal(t, want, token, "token %d of %q, flushed %v", pos, name, flush)
				}
			}
			shut = true
			require.NoError(t, b.Shutdown(ctx))
		}
	})

	t.Run("buffer reuse and a missing index position", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace), WithSecondaryIndices(1))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })
		require.NoError(t, b.Put(key, value, withSec))
		require.NoError(t, b.FlushMemtable())

		buffer := make([]byte, 64)
		got, returned, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, fixed(100, 50), buffer)
		require.NoError(t, err)
		require.Equal(t, value[100:150], got)
		require.Equal(t, &buffer[0], &returned[0], "a buffer with the capacity is reused")
		require.Equal(t, &buffer[0], &got[0])

		_, _, err = b.GetBySecondaryRange(ctx, 1, secKey, 16, fixed(100, 50), nil)
		require.Error(t, err)
	})
}

// TestBucketGetBySecondaryRangeCorruptLength patches the length field of a
// stored node: the read returns an error, never a panic or a value, whether
// the segment is read from memory or with positioned reads, and whatever range
// the picker names.
func TestBucketGetBySecondaryRangeCorruptLength(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	key, secKey := []byte("k1"), []byte("s1")
	value := make([]byte, 1000)
	for i := range value {
		value[i] = byte(i)
	}

	lengths := []struct {
		name      string
		length    uint64
		tombstone bool
	}{
		{name: "one too large", length: uint64(len(value)) + 1},
		{name: "one too small", length: uint64(len(value)) - 1},
		{name: "wrapping a sum", length: math.MaxUint64 - 12},
		{name: "zero", length: 0},
		{name: "a tombstone declaring a value past its end", length: 1 << 40, tombstone: true},
	}
	pickers := []struct {
		name string
		pick ValueRangePicker
	}{
		{"a range inside the value", func([]byte) (uint64, uint64, uint64, error) { return 100, 50, 0, nil }},
		{"an empty range", func([]byte) (uint64, uint64, uint64, error) { return 0, 0, 0, nil }},
	}

	for _, mode := range []struct {
		name  string
		pread bool
	}{{"mmap", false}, {"pread", true}} {
		for _, tt := range lengths {
			for _, picker := range pickers {
				t.Run(mode.name+"/"+tt.name+"/"+picker.name, func(t *testing.T) {
					dir := t.TempDir()
					open := func() *Bucket {
						b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
							cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
							WithStrategy(StrategyReplace), WithSecondaryIndices(1), WithPread(mode.pread))
						require.NoError(t, err)
						return b
					}
					b := open()
					require.NoError(t, b.Put(key, value, WithSecondaryKey(0, secKey)))
					require.NoError(t, b.FlushMemtable())
					require.NoError(t, b.Shutdown(ctx))

					segments, err := filepath.Glob(filepath.Join(dir, "segment-*.db"))
					require.NoError(t, err)
					require.Len(t, segments, 1)
					f, err := os.OpenFile(segments[0], os.O_RDWR, 0o644)
					require.NoError(t, err)
					length := make([]byte, 8)
					binary.LittleEndian.PutUint64(length, tt.length)
					// the node starts after the header: a tombstone byte, then the length
					_, err = f.WriteAt(length, segmentindex.HeaderSize+1)
					require.NoError(t, err)
					if tt.tombstone {
						_, err = f.WriteAt([]byte{1}, segmentindex.HeaderSize)
						require.NoError(t, err)
					}
					require.NoError(t, f.Close())

					b = open()
					t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })
					got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, picker.pick, nil)
					require.Error(t, err)
					require.Nil(t, got)
				})
			}
		}
	}
}

// TestBucketGetBySecondaryRangeMetrics: a read that reaches the segments counts
// under its own operation of the bucket read metric.
func TestBucketGetBySecondaryRangeMetrics(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(monitoring.GetMetrics(), "TestClass", "shard")
	require.NoError(t, err)
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })
	require.NoError(t, b.Put([]byte("k1"), make([]byte, 100), WithSecondaryKey(0, []byte("s1"))))
	require.NoError(t, b.FlushMemtable())

	count := func() float64 {
		return testutil.ToFloat64(metrics.bucketReadOpCountByComponent.WithLabelValues("getbysecondaryrange", "segment_group"))
	}
	before := count()
	pick := func([]byte) (uint64, uint64, uint64, error) { return 0, 10, 0, nil }
	got, _, err := b.GetBySecondaryRange(ctx, 0, []byte("s1"), 16, pick, nil)
	require.NoError(t, err)
	require.Len(t, got, 10)
	require.Equal(t, before+1, count())
}

// TestBucketGetBySecondaryRangeBranches covers where the value can be: the
// flushing memtable, a segment read from memory, a lazily loaded segment, a
// long tombstone node, and a read while a compaction runs.
func TestBucketGetBySecondaryRangeBranches(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	key, secKey := []byte("k1"), []byte("s1")
	withSec := WithSecondaryKey(0, secKey)
	value := make([]byte, 1000)
	for i := range value {
		value[i] = byte(i)
	}
	pick := func([]byte) (uint64, uint64, uint64, error) { return 100, 50, 0, nil }
	newBucket := func(t *testing.T, dir string, opts ...BucketOption) *Bucket {
		t.Helper()
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			append([]BucketOption{WithStrategy(StrategyReplace), WithSecondaryIndices(1)}, opts...)...)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })
		return b
	}

	t.Run("in the flushing memtable", func(t *testing.T) {
		b := newBucket(t, t.TempDir())
		require.NoError(t, b.Put(key, value, withSec))
		switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
		require.NoError(t, err)
		require.True(t, switched)
		defer func() { require.NoError(t, b.FlushAndSwitch()) }()
		got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, pick, nil)
		require.NoError(t, err)
		require.Equal(t, value[100:150], got)
	})

	t.Run("in the flushing memtable, superseded in the active one", func(t *testing.T) {
		b := newBucket(t, t.TempDir())
		require.NoError(t, b.Put(key, value, withSec))
		switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
		require.NoError(t, err)
		require.True(t, switched)
		defer func() { require.NoError(t, b.FlushAndSwitch()) }()
		require.NoError(t, b.Put(key, value[:500], WithSecondaryKey(0, []byte("s2"))))
		got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, pick, nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("in a segment read from memory", func(t *testing.T) {
		b := newBucket(t, t.TempDir(), WithPread(false))
		require.NoError(t, b.Put(key, value, withSec))
		require.NoError(t, b.FlushMemtable())
		got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, pick, nil)
		require.NoError(t, err)
		require.Equal(t, value[100:150], got)
	})

	t.Run("in a lazily loaded segment", func(t *testing.T) {
		dir := t.TempDir()
		first, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace), WithSecondaryIndices(1))
		require.NoError(t, err)
		require.NoError(t, first.Put(key, value, withSec))
		require.NoError(t, first.FlushMemtable())
		require.NoError(t, first.Shutdown(ctx))

		b := newBucket(t, dir, WithLazySegmentLoading(true))
		got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, pick, nil)
		require.NoError(t, err)
		require.Equal(t, value[100:150], got)
	})

	t.Run("a long tombstone node", func(t *testing.T) {
		for _, pread := range []bool{false, true} {
			b := newBucket(t, t.TempDir(), WithPread(pread), WithKeepTombstones(true))
			require.NoError(t, b.Put(key, value, withSec))
			require.NoError(t, b.FlushMemtable())
			// a deletion time makes the tombstone carry a value
			require.NoError(t, b.DeleteWith(key, time.Now(), withSec))
			require.NoError(t, b.FlushMemtable())
			got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 2, pick, nil)
			require.NoError(t, err)
			require.Nil(t, got)
		}
	})

	t.Run("a read during a compaction", func(t *testing.T) {
		b := newBucket(t, t.TempDir())
		for i := range 6 {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("other%d", i)), value, WithSecondaryKey(0, []byte(fmt.Sprintf("o%d", i)))))
			if i == 2 {
				require.NoError(t, b.Put(key, value, withSec))
			}
			require.NoError(t, b.FlushMemtable())
		}

		done := make(chan struct{})
		var compactions atomic.Int64
		enterrors.GoWrapper(func() {
			defer close(done)
			for {
				compacted, err := b.disk.compactOnce(ctx)
				assert.NoError(t, err)
				if !compacted || err != nil {
					return
				}
				compactions.Add(1)
			}
		}, logger)
		// the reader's failures are collected, so that the compactor is
		// never left running while the bucket is shut down
		var readErr error
		for reading := true; reading; {
			select {
			case <-done:
				reading = false
			default:
			}
			got, _, err := b.GetBySecondaryRange(ctx, 0, secKey, 16, pick, nil)
			if err == nil && !bytes.Equal(value[100:150], got) {
				err = fmt.Errorf("read %v during the compaction", got)
			}
			if err != nil && readErr == nil {
				readErr = err
			}
		}
		<-done
		require.NoError(t, readErr)
		require.Positive(t, compactions.Load(), "no compaction ran during the reads")
	})
}

// TestDeadKeyAndReAddedEntriesAcrossCompaction is what the posting store relies
// on from a Set bucket: an entry added, removed and added again across three
// segments is there after a compaction, one removed last is not, and a key the
// skip function names is gone.
func TestDeadKeyAndReAddedEntriesAcrossCompaction(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	live, dead := []byte("live"), []byte("dead")
	again, removed := []byte("again"), []byte("removed")
	var skip atomic.Bool

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection), WithForceCompaction(true),
		WithShouldSkipKeyFunction(func(key []byte, ctx context.Context) (bool, error) {
			return skip.Load() && bytes.Equal(key, dead), nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.SetAdd(live, [][]byte{again, removed}))
	require.NoError(t, b.SetAdd(dead, [][]byte{again}))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.SetDeleteSingle(live, again))
	require.NoError(t, b.FlushMemtable())
	require.NoError(t, b.SetAdd(live, [][]byte{again}))
	require.NoError(t, b.SetDeleteSingle(live, removed))
	require.NoError(t, b.FlushMemtable())

	check := func(t *testing.T) {
		t.Helper()
		got, err := b.SetList(live)
		require.NoError(t, err)
		require.Equal(t, [][]byte{again}, got)
	}
	check(t)

	got, err := b.SetList(dead)
	require.NoError(t, err)
	require.Len(t, got, 1, "before the compaction the key is still read")

	skip.Store(true)
	for {
		compacted, err := b.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
		check(t)
	}
	check(t)
	got, err = b.SetList(dead)
	require.NoError(t, err)
	require.Empty(t, got)
}
