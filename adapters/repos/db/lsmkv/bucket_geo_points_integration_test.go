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
