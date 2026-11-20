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

//go:build integrationTest

package lsmkv

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/geopoints"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// geoFixture tracks the points a test wrote so queries can be checked against
// a brute-force answer.
type geoFixture struct {
	r      *rand.Rand
	live   map[uint64]geopoints.Point
	nextID uint64
}

func newGeoFixture(seed int64) *geoFixture {
	return &geoFixture{r: rand.New(rand.NewSource(seed)), live: map[uint64]geopoints.Point{}}
}

func (f *geoFixture) add(t *testing.T, b *Bucket, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		p := geopoints.Point{DocID: f.nextID}
		f.nextID++
		switch f.r.Intn(4) {
		case 0:
			p.Lat, p.Lon = 52.5, 13.4 // exact duplicates
		case 1:
			p.Lat = float32(f.r.NormFloat64()*0.5) + 48.8
			p.Lon = float32(f.r.NormFloat64()*0.5) + 2.3
		default:
			p.Lat = float32(f.r.Float64()*180 - 90)
			p.Lon = float32(f.r.Float64()*360 - 180)
		}
		require.NoError(t, b.GeoPointsAdd(p.DocID, p.Lat, p.Lon))
		f.live[p.DocID] = p
	}
}

func (f *geoFixture) remove(t *testing.T, b *Bucket, n int) {
	t.Helper()
	ids := make([]uint64, 0, len(f.live))
	for id := range f.live {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	f.r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	for _, id := range ids[:min(n, len(ids))] {
		require.NoError(t, b.GeoPointsRemove(id))
		delete(f.live, id)
	}
}

func (f *geoFixture) check(t *testing.T, b *Bucket, queries int) {
	t.Helper()
	for q := 0; q < queries; q++ {
		lat := float32(f.r.Float64()*180 - 90)
		lon := float32(f.r.Float64()*360 - 180)
		switch q % 3 {
		case 0:
			lat, lon = 52.5, 13.4 // the duplicate cluster
		case 1:
			lat, lon = 48.8, 2.3 // the gaussian cluster
		}
		dist := float32(math.Exp(f.r.Float64() * 16))

		c, err := geopoints.NewCircle(lat, lon, dist)
		require.NoError(t, err)
		want := []uint64{}
		for id, p := range f.live {
			if c.Contains(p.Lat, p.Lon) {
				want = append(want, id)
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

		got, err := b.GeoPointsWithinRange(context.Background(), lat, lon, dist)
		require.NoError(t, err)
		require.Equal(t, want, got.ToArray(), "circle (%v, %v, %v)", lat, lon, dist)
	}
}

func TestGeoPointsBucket(t *testing.T) {
	for _, checksums := range []bool{false, true} {
		for _, pread := range []bool{false, true} {
			t.Run(testName(checksums, pread), func(t *testing.T) {
				testGeoPointsBucket(t, checksums, pread)
			})
		}
	}
}

func testName(checksums, pread bool) string {
	name := "mmap"
	if pread {
		name = "pread"
	}
	if checksums {
		return name + "-checksums"
	}
	return name
}

func testGeoPointsBucket(t *testing.T, checksums, pread bool) {
	ctx := testCtx()
	dir := t.TempDir()
	opts := []BucketOption{
		WithStrategy(StrategyGeoPoints),
		WithSegmentsChecksumValidationEnabled(checksums),
		WithPread(pread),
		WithWriteSegmentInfoIntoFileName(true),
	}
	open := func(dir string) *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		return b
	}

	b := open(dir)
	f := newGeoFixture(42)

	t.Run("memtable only", func(t *testing.T) {
		f.add(t, b, 3000)
		f.check(t, b, 60)
	})

	t.Run("one segment", func(t *testing.T) {
		require.NoError(t, b.FlushAndSwitch())
		f.check(t, b, 60)
	})

	t.Run("removals across segment and memtable", func(t *testing.T) {
		f.add(t, b, 2000)
		f.remove(t, b, 1500)
		f.check(t, b, 60)
		require.NoError(t, b.FlushAndSwitch())
		f.add(t, b, 1000)
		f.remove(t, b, 800)
		require.NoError(t, b.FlushAndSwitch())
		f.check(t, b, 60)
	})

	t.Run("compaction", func(t *testing.T) {
		segments := len(b.disk.segments)
		require.GreaterOrEqual(t, segments, 3)
		var compacted bool
		var err error
		for compacted, err = b.disk.compactOnce(ctx); err == nil && compacted; compacted, err = b.disk.compactOnce(ctx) {
		}
		require.NoError(t, err)
		require.Less(t, len(b.disk.segments), segments)
		f.check(t, b, 60)

		// the oldest segment carries no deletions once everything is merged
		if len(b.disk.segments) == 1 {
			reader, err := b.disk.segments[0].newGeoPointsReader()
			require.NoError(t, err)
			del, err := reader.Deletions()
			require.NoError(t, err)
			assert.True(t, del.IsEmpty())
			assert.Equal(t, uint64(len(f.live)), reader.PointCount())
		}
	})

	t.Run("write-ahead log survives a crash", func(t *testing.T) {
		f.add(t, b, 500)
		f.remove(t, b, 200)
		require.NoError(t, b.WriteWAL())

		// a copy of the directory stands in for the process dying before a flush
		crashed := t.TempDir()
		copyDir(t, dir, crashed)
		b2 := open(crashed)
		defer b2.Shutdown(ctx)
		f.check(t, b2, 40)
	})

	t.Run("reopen after shutdown", func(t *testing.T) {
		require.NoError(t, b.Shutdown(ctx))
		b = open(dir)
		f.check(t, b, 60)
		f.add(t, b, 100)
		f.check(t, b, 20)
		require.NoError(t, b.Shutdown(ctx))
	})
}

func TestGeoPointsBucketRejectsBadWrites(t *testing.T) {
	ctx := testCtx()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyGeoPoints))
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	require.Error(t, b.GeoPointsAdd(1, float32(math.NaN()), 0))
	require.Error(t, b.GeoPointsAdd(1, 0, float32(math.Inf(1))))
	_, err = b.GeoPointsWithinRange(ctx, 0, 0, -1)
	require.Error(t, err)

	other, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))
	require.NoError(t, err)
	defer other.Shutdown(ctx)
	require.Error(t, other.GeoPointsAdd(1, 0, 0))
	require.Error(t, other.GeoPointsRemove(1))
	_, err = other.GeoPointsWithinRange(ctx, 0, 0, 1)
	require.Error(t, err)
}

// TestGeoPointsBucketCanonicalCoordinates pins that a coordinate outside the
// usual ranges is the point on the sphere it names, before and after a flush:
// the memtable scans every point, the segment prunes by canonical ranges.
func TestGeoPointsBucketCanonicalCoordinates(t *testing.T) {
	ctx := testCtx()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyGeoPoints))
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	require.NoError(t, b.GeoPointsAdd(1, 0, 360))   // (0, 0)
	require.NoError(t, b.GeoPointsAdd(2, 100, 0))   // (80, 180)
	require.NoError(t, b.GeoPointsAdd(3, 10, -190)) // (10, 170)

	check := func(t *testing.T) {
		t.Helper()
		within := func(lat, lon, dist float32) []uint64 {
			bm, err := b.GeoPointsWithinRange(ctx, lat, lon, dist)
			require.NoError(t, err)
			return bm.ToArray()
		}
		assert.Equal(t, []uint64{1}, within(0, 0, 1_000))
		assert.Equal(t, []uint64{1}, within(0, 360, 1_000), "a query center is canonical too")
		assert.Equal(t, []uint64{2}, within(80, 180, 1_000))
		assert.Equal(t, []uint64{2}, within(80, -180, 1_000))
		assert.Equal(t, []uint64{3}, within(10, 170, 1_000))
		assert.Empty(t, within(0, 180, 1_000))
	}
	check(t)
	require.NoError(t, b.FlushAndSwitch())
	check(t)
}

// TestGeoPointsBucketRemoveBeforeAdd pins that a removal stands over an add of
// the same doc ID that arrives after it, in the memtable and across a flush.
// The shard writes an object's geo points under the object's lock, so this
// order cannot arise there; the bucket's semantics are the backstop. They end
// where compaction of the oldest segments drops its unspent deletions, on the
// premise the lock guarantees: a point can only be in a layer at least as old
// as its removal.
func TestGeoPointsBucketRemoveBeforeAdd(t *testing.T) {
	ctx := testCtx()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyGeoPoints), WithWriteSegmentInfoIntoFileName(true))
	require.NoError(t, err)
	defer b.Shutdown(ctx)

	within := func(t *testing.T) []uint64 {
		t.Helper()
		bm, err := b.GeoPointsWithinRange(ctx, 52.5, 13.4, 1_000)
		require.NoError(t, err)
		return bm.ToArray()
	}

	// the live point every check expects
	require.NoError(t, b.GeoPointsAdd(1, 52.5, 13.4))

	// same memtable: the late add is dropped at the flush along with the removal
	require.NoError(t, b.GeoPointsRemove(7))
	require.NoError(t, b.GeoPointsAdd(7, 52.5, 13.4))
	assert.Equal(t, []uint64{1}, within(t))
	require.NoError(t, b.FlushAndSwitch())
	assert.Equal(t, []uint64{1}, within(t))
	reader, err := b.disk.segments[0].newGeoPointsReader()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), reader.PointCount())
	del, err := reader.Deletions()
	require.NoError(t, err)
	assert.True(t, del.IsEmpty(), "the removal found its point in the memtable and is spent")

	// removal in a segment, add in the memtable, then in a later segment
	require.NoError(t, b.GeoPointsRemove(8))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.GeoPointsAdd(8, 52.5, 13.4))
	assert.Equal(t, []uint64{1}, within(t))
	require.NoError(t, b.FlushAndSwitch())
	assert.Equal(t, []uint64{1}, within(t))

	// merging the two newest segments spends the removal on the late add
	require.Len(t, b.disk.segments, 3)
	segments := b.disk.segments
	merged, err := geopoints.Merge(ctx, mustGeoReader(t, segments[1]), mustGeoReader(t, segments[2]), false)
	require.NoError(t, err)
	assert.Empty(t, merged.Points)
	assert.Nil(t, merged.Deletions)
}

func mustGeoReader(t *testing.T, seg Segment) *geopoints.SegmentReader {
	t.Helper()
	reader, err := seg.newGeoPointsReader()
	require.NoError(t, err)
	return reader
}
