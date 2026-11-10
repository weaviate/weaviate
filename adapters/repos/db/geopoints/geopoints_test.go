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

package geopoints

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestDistance(t *testing.T) {
	tests := []struct {
		name                   string
		latA, lonA, latB, lonB float32
		want                   float32
		tolerance              float32
	}{
		{name: "same point", want: 0},
		{name: "berlin to paris", latA: 52.52, lonA: 13.405, latB: 48.8566, lonB: 2.3522, want: 877_460, tolerance: 1_000},
		{name: "across the antimeridian", latA: 0, lonA: 179.5, latB: 0, lonB: -179.5, want: 111_195, tolerance: 100},
		{name: "pole to pole", latA: 90, lonA: 0, latB: -90, lonB: 0, want: float32(math.Pi * earthRadiusMeters), tolerance: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Distance(test.latA, test.lonA, test.latB, test.lonB)
			assert.InDelta(t, test.want, got, float64(test.tolerance))
			// symmetric
			assert.Equal(t, got, Distance(test.latB, test.lonB, test.latA, test.lonA))
		})
	}
}

func TestValidateCoordinates(t *testing.T) {
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))
	tests := []struct {
		name     string
		lat, lon float32
		wantErr  bool
	}{
		{name: "regular", lat: 1, lon: 2},
		{name: "out of range is accepted", lat: 100, lon: 400},
		{name: "nan lat", lat: nan, lon: 0, wantErr: true},
		{name: "nan lon", lat: 0, lon: nan, wantErr: true},
		{name: "inf lat", lat: inf, lon: 0, wantErr: true},
		{name: "inf lon", lat: 0, lon: -inf, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCoordinates(test.lat, test.lon)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewCircleRejectsBadInput(t *testing.T) {
	_, err := NewCircle(float32(math.NaN()), 0, 1)
	require.Error(t, err)
	_, err = NewCircle(0, 0, -1)
	require.Error(t, err)
	_, err = NewCircle(0, 0, float32(math.NaN()))
	require.Error(t, err)
	c, err := NewCircle(0, 0, 0)
	require.NoError(t, err)
	assert.True(t, c.Contains(0, 0))
	assert.False(t, c.Contains(0, 0.001))
}

func TestCircleBoundingBoxes(t *testing.T) {
	tests := []struct {
		name         string
		lat, lon     float32
		distance     float32
		wantBoxes    int
		wantEveryLon bool
		// maxLatWidth and maxLonWidth bound the first box, so the slack added
		// for rounding stays negligible against the circle
		maxLatWidth, maxLonWidth float32
	}{
		{name: "small circle in europe", lat: 52, lon: 13, distance: 10_000, wantBoxes: 1, maxLatWidth: 0.181, maxLonWidth: 0.293},
		// the slack for float32 rounding is a few meters, so it shows here
		{name: "a meter at the equator", lat: 0, lon: 0, distance: 1, wantBoxes: 1, maxLatWidth: 0.00007, maxLonWidth: 0.00011},
		{name: "crossing the antimeridian east", lat: 0, lon: 179.9, distance: 50_000, wantBoxes: 2},
		{name: "crossing the antimeridian west", lat: 0, lon: -179.9, distance: 50_000, wantBoxes: 2},
		{name: "covering the north pole", lat: 89, lon: 0, distance: 200_000, wantBoxes: 1, wantEveryLon: true},
		{name: "covering the south pole", lat: -89.5, lon: 100, distance: 100_000, wantBoxes: 1, wantEveryLon: true},
		{name: "half the planet", lat: 10, lon: 10, distance: float32(math.Pi * earthRadiusMeters / 2), wantBoxes: 1, wantEveryLon: true},
		{name: "whole planet", lat: 10, lon: 10, distance: float32(math.Pi * earthRadiusMeters), wantBoxes: 1, wantEveryLon: true},
		{name: "zero radius", lat: 10, lon: 10, distance: 0, wantBoxes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := NewCircle(test.lat, test.lon, test.distance)
			require.NoError(t, err)
			require.Len(t, c.boxes, test.wantBoxes)
			if test.wantEveryLon {
				assert.LessOrEqual(t, c.boxes[0].MinLon, float32(-180))
				assert.GreaterOrEqual(t, c.boxes[0].MaxLon, float32(180))
			}
			if test.maxLatWidth > 0 {
				assert.Less(t, c.boxes[0].MaxLat-c.boxes[0].MinLat, test.maxLatWidth)
				assert.Less(t, c.boxes[0].MaxLon-c.boxes[0].MinLon, test.maxLonWidth)
			}
			// the center is always in a box
			assert.True(t, c.IntersectsBox(Box{test.lat, test.lat, test.lon, test.lon}))
		})
	}
}

// TestCircleBoxesCoverCircle samples points around many circles and checks that
// every point inside the circle is inside a bounding box, including circles
// that wrap or reach a pole.
func TestCircleBoxesCoverCircle(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		lat := float32(r.Float64()*180 - 90)
		lon := float32(r.Float64()*360 - 180)
		dist := float32(math.Exp(r.Float64() * 17)) // 1 m .. ~24,000 km
		c, err := NewCircle(lat, lon, dist)
		require.NoError(t, err)

		for j := 0; j < 50; j++ {
			plat := float32(r.Float64()*180 - 90)
			plon := float32(r.Float64()*360 - 180)
			// bias half the samples towards the boundary
			if j%2 == 0 {
				plat = lat + float32(r.NormFloat64())*float32(dist/earthRadiusMeters*180/math.Pi)
				plon = lon + float32(r.NormFloat64())*float32(dist/earthRadiusMeters*180/math.Pi)*2
				plat = float32(math.Max(-90, math.Min(90, float64(plat))))
				for plon > 180 {
					plon -= 360
				}
				for plon < -180 {
					plon += 360
				}
			}
			if !c.Contains(plat, plon) {
				continue
			}
			assert.True(t, c.IntersectsBox(Box{plat, plat, plon, plon}),
				"circle (%v,%v,%v) contains (%v,%v) but its boxes %v miss it", lat, lon, dist, plat, plon, c.boxes)
		}
	}
}

// TestContainsBoxIsSound checks that a box reported inside the circle holds no
// point outside it.
func TestContainsBoxIsSound(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	contained := 0
	for i := 0; i < 5000; i++ {
		lat := float32(r.Float64()*180 - 90)
		lon := float32(r.Float64()*360 - 180)
		dist := float32(math.Exp(r.Float64() * 16))
		c, err := NewCircle(lat, lon, dist)
		require.NoError(t, err)

		rDeg := float32(dist / earthRadiusMeters * 180 / math.Pi)
		box := Box{
			MinLat: lat + float32(r.NormFloat64())*rDeg,
			MinLon: lon + float32(r.NormFloat64())*rDeg,
		}
		box.MaxLat = box.MinLat + float32(r.Float64())*rDeg
		box.MaxLon = box.MinLon + float32(r.Float64())*rDeg
		if !c.ContainsBox(box) {
			continue
		}
		contained++
		for j := 0; j < 20; j++ {
			plat := box.MinLat + float32(r.Float64())*(box.MaxLat-box.MinLat)
			plon := box.MinLon + float32(r.Float64())*(box.MaxLon-box.MinLon)
			require.True(t, c.Contains(plat, plon),
				"box %v is reported inside circle (%v,%v,%v) but (%v,%v) is outside", box, lat, lon, dist, plat, plon)
		}
	}
	require.Greater(t, contained, 100, "the sampling never exercised ContainsBox")
}

func genPoints(r *rand.Rand, n int) []Point {
	points := make([]Point, n)
	for i := range points {
		var lat, lon float32
		switch r.Intn(10) {
		case 0:
			// exact duplicates on a handful of coordinates
			lat, lon = 52.5, 13.4
		case 1:
			// on the antimeridian and the poles
			lat = float32([]float64{-90, 90, 0}[r.Intn(3)])
			lon = float32([]float64{-180, 180, 179.999, -179.999}[r.Intn(4)])
		case 2, 3, 4, 5:
			// clustered
			c := r.Intn(20)
			cr := rand.New(rand.NewSource(int64(c)))
			lat = float32(cr.Float64()*170-85) + float32(r.NormFloat64()*0.2)
			lon = float32(cr.Float64()*360-180) + float32(r.NormFloat64()*0.2)
		default:
			lat = float32(r.Float64()*180 - 90)
			lon = float32(r.Float64()*360 - 180)
		}
		points[i] = Point{DocID: uint64(i), Lat: lat, Lon: lon}
	}
	return points
}

func bruteForce(points []Point, c *Circle) []uint64 {
	out := []uint64{}
	for _, p := range points {
		if c.Contains(p.Lat, p.Lon) {
			out = append(out, p.DocID)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func randomCircle(r *rand.Rand, points []Point) *Circle {
	var lat, lon float32
	if len(points) > 0 && r.Intn(2) == 0 {
		p := points[r.Intn(len(points))]
		lat, lon = p.Lat, p.Lon
	} else {
		lat = float32(r.Float64()*180 - 90)
		lon = float32(r.Float64()*360 - 180)
	}
	dist := float32(math.Exp(r.Float64() * 17))
	if r.Intn(10) == 0 {
		dist = 0
	}
	c, _ := NewCircle(lat, lon, dist)
	return c
}

func TestBuildAndQueryMatchesBruteForce(t *testing.T) {
	sizes := []int{0, 1, 2, MaxPointsPerLeaf - 1, MaxPointsPerLeaf, MaxPointsPerLeaf + 1, 5_000, 60_000}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("%d points", n), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(n)))
			points := genPoints(r, n)
			original := append([]Point(nil), points...)

			body := Build(points, nil)
			require.Len(t, body, BodySize(n, 0))

			reader, err := NewSegmentReader(body)
			require.NoError(t, err)
			assert.Equal(t, uint64(n), reader.PointCount())

			stored, err := reader.Points()
			require.NoError(t, err)
			assertSamePoints(t, original, stored, "every point survives the build")

			pread, err := NewSegmentReaderPread(bytes.NewReader(body), 0, int64(len(body)))
			require.NoError(t, err)

			for q := 0; q < 200; q++ {
				c := randomCircle(r, original)
				want := bruteForce(original, c)

				out, del := sroar.NewBitmap(), sroar.NewBitmap()
				require.NoError(t, reader.WithinRange(context.Background(), c, out, del))
				assert.Equal(t, want, out.ToArray(), "circle (%v,%v,%v)", c.Lat, c.Lon, c.Distance)
				assert.True(t, del.IsEmpty())

				out2, del2 := sroar.NewBitmap(), sroar.NewBitmap()
				require.NoError(t, pread.WithinRange(context.Background(), c, out2, del2))
				assert.Equal(t, want, out2.ToArray(), "pread circle (%v,%v,%v)", c.Lat, c.Lon, c.Distance)
			}
		})
	}
}

func TestBuildCarriesDeletions(t *testing.T) {
	del := sroar.NewBitmap()
	del.SetMany([]uint64{3, 5, 700})
	body := Build([]Point{{DocID: 1, Lat: 1, Lon: 1}}, del.ToBuffer())

	reader, err := NewSegmentReader(body)
	require.NoError(t, err)
	got, err := reader.Deletions()
	require.NoError(t, err)
	assert.Equal(t, []uint64{3, 5, 700}, got.ToArray())

	c, _ := NewCircle(1, 1, 10)
	out, deleted := sroar.NewBitmap(), sroar.NewBitmap()
	require.NoError(t, reader.WithinRange(context.Background(), c, out, deleted))
	assert.Equal(t, []uint64{1}, out.ToArray())
	assert.Equal(t, []uint64{3, 5, 700}, deleted.ToArray())
}

func TestSegmentReaderRejectsCorruptBodies(t *testing.T) {
	good := Build(genPoints(rand.New(rand.NewSource(3)), 1000), nil)

	tests := []struct {
		name string
		body []byte
	}{
		{name: "empty", body: nil},
		{name: "truncated header", body: good[:10]},
		{name: "truncated points", body: good[:len(good)-1]},
		{name: "trailing bytes", body: append(append([]byte(nil), good...), 0)},
		{name: "wrong version", body: func() []byte {
			b := append([]byte(nil), good...)
			b[0] = 9
			return b
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewSegmentReader(test.body)
			require.Error(t, err)
		})
	}
}

func TestWithinRangeHonoursContext(t *testing.T) {
	points := genPoints(rand.New(rand.NewSource(4)), 100_000)
	reader, err := NewSegmentReader(Build(points, nil))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, _ := NewCircle(0, 0, float32(math.Pi*earthRadiusMeters))
	err = reader.WithinRange(ctx, c, sroar.NewBitmap(), sroar.NewBitmap())
	require.ErrorIs(t, err, context.Canceled)

	m := NewMemtable()
	for _, p := range points {
		m.Add(p.DocID, p.Lat, p.Lon)
	}
	err = m.WithinRange(ctx, c, sroar.NewBitmap(), sroar.NewBitmap())
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemtable(t *testing.T) {
	m := NewMemtable()
	assert.Equal(t, 0, m.Size())

	m.Add(1, 10, 10)
	m.Add(2, 10.001, 10)
	m.Add(3, 50, 50)
	assert.Equal(t, 48, m.Size())

	m.Remove(2)
	m.Remove(99) // a point in an older segment
	assert.Equal(t, 64, m.Size())

	c, _ := NewCircle(10, 10, 1_000)
	out, deleted := sroar.NewBitmap(), sroar.NewBitmap()
	require.NoError(t, m.WithinRange(context.Background(), c, out, deleted))
	// the memtable reports both; the bucket subtracts
	assert.Equal(t, []uint64{1, 2}, out.ToArray())
	assert.Equal(t, []uint64{2, 99}, deleted.ToArray())

	reader, err := NewSegmentReader(m.Body().Bytes())
	require.NoError(t, err)
	points, err := reader.Points()
	require.NoError(t, err)
	assert.ElementsMatch(t, []Point{{1, 10, 10}, {3, 50, 50}}, points, "the removed point is dropped")
	del, err := reader.Deletions()
	require.NoError(t, err)
	assert.Equal(t, []uint64{99}, del.ToArray(), "the spent deletion is dropped, the other kept")

	// an add after a remove is a write that lost a race with the delete of its
	// object: the removal stands, in the memtable and in the flushed segment
	m.Add(99, 1, 1)
	assert.True(t, m.deleted.Contains(99))
	assert.Equal(t, 4*16+2*8, m.Size())

	c99, _ := NewCircle(1, 1, 1_000)
	out, deleted = sroar.NewBitmap(), sroar.NewBitmap()
	require.NoError(t, m.WithinRange(context.Background(), c99, out, deleted))
	out.AndNot(deleted)
	assert.True(t, out.IsEmpty(), "the bucket's subtraction hides the late add")

	reader, err = NewSegmentReader(m.Body().Bytes())
	require.NoError(t, err)
	points, err = reader.Points()
	require.NoError(t, err)
	assert.ElementsMatch(t, []Point{{1, 10, 10}, {3, 50, 50}}, points, "the late add is dropped at flush")
	del, err = reader.Deletions()
	require.NoError(t, err)
	assert.True(t, del.IsEmpty(), "both deletions found their point in this memtable")
}

func TestMemtableSerializeEmpty(t *testing.T) {
	reader, err := NewSegmentReader(NewMemtable().Body().Bytes())
	require.NoError(t, err)
	assert.Zero(t, reader.PointCount())
	c, _ := NewCircle(0, 0, 1e9)
	out := sroar.NewBitmap()
	require.NoError(t, reader.WithinRange(context.Background(), c, out, sroar.NewBitmap()))
	assert.True(t, out.IsEmpty())
}

func TestMerge(t *testing.T) {
	leftDel := sroar.NewBitmap()
	// 1000 refers to a segment older than left; 4 is a point of right that was
	// added after its removal, a write that lost a race with its delete
	leftDel.SetMany([]uint64{1000, 4})
	left := mustReader(t, Build([]Point{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}, leftDel.ToBuffer()))

	rightDel := sroar.NewBitmap()
	rightDel.SetMany([]uint64{2, 2000}) // 2 is in left, 2000 in an older segment
	right := mustReader(t, Build([]Point{{4, 4, 4}, {5, 5, 5}}, rightDel.ToBuffer()))

	tests := []struct {
		name          string
		cleanup       bool
		wantDeletions []uint64
	}{
		{name: "deletions kept for older segments", cleanup: false, wantDeletions: []uint64{1000, 2000}},
		{name: "oldest segment drops every deletion", cleanup: true, wantDeletions: []uint64{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := Merge(context.Background(), left, right, test.cleanup)
			require.NoError(t, err)
			require.Equal(t, body.Size(), len(body.Bytes()))
			merged := mustReader(t, body.Bytes())

			points, err := merged.Points()
			require.NoError(t, err)
			assert.ElementsMatch(t, []Point{{1, 1, 1}, {3, 3, 3}, {5, 5, 5}}, points,
				"a point deleted by either segment is dropped")

			del, err := merged.Deletions()
			require.NoError(t, err)
			assert.Equal(t, test.wantDeletions, del.ToArray())
		})
	}
}

func TestMergeMatchesBruteForce(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	all := genPoints(r, 20_000)
	left := append([]Point(nil), all[:12_000]...)
	right := append([]Point(nil), all[12_000:]...)

	// right deletes a third of left's points and some unknown ones
	rightDel := sroar.NewBitmap()
	live := map[uint64]bool{}
	for _, p := range all {
		live[p.DocID] = true
	}
	for _, p := range left {
		if r.Intn(3) == 0 {
			rightDel.Set(p.DocID)
			live[p.DocID] = false
		}
	}
	rightDel.Set(1_000_000)

	body, err := Merge(context.Background(),
		mustReader(t, Build(left, nil)),
		mustReader(t, Build(right, rightDel.ToBuffer())), false)
	require.NoError(t, err)
	merged := mustReader(t, body.Bytes())

	var want []Point
	for _, p := range all {
		if live[p.DocID] {
			want = append(want, p)
		}
	}
	points, err := merged.Points()
	require.NoError(t, err)
	assertSamePoints(t, want, points, "merged points")

	del, err := merged.Deletions()
	require.NoError(t, err)
	assert.Equal(t, []uint64{1_000_000}, del.ToArray())

	for q := 0; q < 100; q++ {
		c := randomCircle(r, want)
		out := sroar.NewBitmap()
		require.NoError(t, merged.WithinRange(context.Background(), c, out, sroar.NewBitmap()))
		assert.Equal(t, bruteForce(want, c), out.ToArray())
	}
}

func TestCompactorWritesLoadableSegment(t *testing.T) {
	left := mustReader(t, Build([]Point{{1, 1, 1}}, nil))
	right := mustReader(t, Build([]Point{{2, 2, 2}}, nil))

	for _, checksum := range []bool{false, true} {
		buf := &seekableBuffer{}
		c := NewCompactor(buf, left, right, 1, true, checksum, 1<<20)
		require.NoError(t, c.Do(context.Background()))

		// header, body, optional checksum
		body := Build([]Point{{1, 1, 1}, {2, 2, 2}}, nil)
		wantLen := 16 + len(body)
		if checksum {
			wantLen += 4
		}
		require.Len(t, buf.Bytes(), wantLen)
		reader, err := NewSegmentReader(buf.Bytes()[16 : 16+len(body)])
		require.NoError(t, err)
		points, err := reader.Points()
		require.NoError(t, err)
		assert.ElementsMatch(t, []Point{{1, 1, 1}, {2, 2, 2}}, points)
	}
}

// assertSamePoints compares two point sets by doc ID order. ElementsMatch is
// quadratic and does not finish on tens of thousands of points.
func assertSamePoints(t *testing.T, want, got []Point, msg string) {
	t.Helper()
	byID := func(ps []Point) []Point {
		out := append([]Point(nil), ps...)
		sort.Slice(out, func(i, j int) bool { return out[i].DocID < out[j].DocID })
		return out
	}
	assert.Equal(t, byID(want), byID(got), msg)
}

func mustReader(t *testing.T, body []byte) *SegmentReader {
	t.Helper()
	r, err := NewSegmentReader(body)
	require.NoError(t, err)
	return r
}

// seekableBuffer is an in-memory io.WriteSeeker for the compactor.
type seekableBuffer struct {
	buf []byte
	pos int
}

func (b *seekableBuffer) Write(p []byte) (int, error) {
	end := b.pos + len(p)
	if end > len(b.buf) {
		b.buf = append(b.buf, make([]byte, end-len(b.buf))...)
	}
	copy(b.buf[b.pos:], p)
	b.pos = end
	return len(p), nil
}

func (b *seekableBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		b.pos = int(offset)
	case 1:
		b.pos += int(offset)
	case 2:
		b.pos = len(b.buf) + int(offset)
	}
	return int64(b.pos), nil
}

func (b *seekableBuffer) Bytes() []byte { return b.buf }

// TestContainsMatchesDistance pins that the precomputed center terms give the
// same answer as Distance at the boundary, which decides which objects a
// filter returns.
func TestContainsMatchesDistance(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	for i := 0; i < 200_000; i++ {
		lat := float32(r.Float64()*180 - 90)
		lon := float32(r.Float64()*360 - 180)
		plat := lat + float32(r.NormFloat64())*0.01
		plon := lon + float32(r.NormFloat64())*0.01
		// a radius right at the distance, so the comparison is on the boundary
		dist := Distance(lat, lon, plat, plon)
		for _, d := range []float32{dist, math.Nextafter32(dist, 0), math.Nextafter32(dist, math.MaxFloat32)} {
			c, err := NewCircle(lat, lon, d)
			require.NoError(t, err)
			require.Equal(t, Distance(lat, lon, plat, plon) <= d, c.Contains(plat, plon),
				"center (%v,%v) point (%v,%v) radius %v", lat, lon, plat, plon, d)
		}
	}
}

func TestSelectNth(t *testing.T) {
	r := rand.New(rand.NewSource(10))
	for _, n := range []int{1, 2, 3, 15, 16, 17, 100, 5000} {
		for trial := 0; trial < 8; trial++ {
			points := make([]Point, n)
			for i := range points {
				// few distinct values force duplicate coordinates; doc IDs are
				// unique, as in a shard
				points[i] = Point{DocID: uint64(i), Lat: float32(r.Intn(4)), Lon: float32(r.Intn(4))}
			}
			r.Shuffle(n, func(i, j int) { points[i], points[j] = points[j], points[i] })
			for _, byLat := range []bool{true, false} {
				k := r.Intn(n)
				got := append([]Point(nil), points...)
				selectNth(got, k, byLat)

				want := append([]Point(nil), points...)
				sort.Slice(want, func(i, j int) bool { return lessPoint(&want[i], &want[j], byLat) })
				for i := 0; i < k; i++ {
					require.False(t, lessPoint(&got[k], &got[i], byLat), "n=%d k=%d: element before k orders after it", n, k)
				}
				for i := k + 1; i < n; i++ {
					require.False(t, lessPoint(&got[i], &got[k], byLat), "n=%d k=%d: element after k orders before it", n, k)
				}
				require.Equal(t, want[k].DocID, got[k].DocID)
				assertSamePoints(t, points, got, "selectNth must only reorder")
			}
		}
	}
}

func TestCanonical(t *testing.T) {
	tests := []struct {
		name             string
		lat, lon         float32
		wantLat, wantLon float32
	}{
		{name: "in range", lat: 52.5, lon: 13.4, wantLat: 52.5, wantLon: 13.4},
		{name: "the edges stay", lat: 90, lon: 180, wantLat: 90, wantLon: 180},
		{name: "minus 180 stays", lat: -90, lon: -180, wantLat: -90, wantLon: -180},
		{name: "a full turn east", lat: 0, lon: 360, wantLat: 0, wantLon: 0},
		{name: "past the antimeridian", lat: 10, lon: 190, wantLat: 10, wantLon: -170},
		{name: "two turns west", lat: 10, lon: -730, wantLat: 10, wantLon: -10},
		{name: "over the north pole", lat: 100, lon: 0, wantLat: 80, wantLon: -180},
		{name: "over the south pole", lat: -100, lon: 10, wantLat: -80, wantLon: -170},
		{name: "the antipode", lat: 180, lon: 20, wantLat: 0, wantLon: -160},
		{name: "a turn and a bit in latitude", lat: 370, lon: 0, wantLat: 10, wantLon: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lat, lon := Canonical(test.lat, test.lon)
			assert.InDelta(t, test.wantLat, lat, 1e-4)
			assert.InDelta(t, test.wantLon, lon, 1e-4)
			assert.LessOrEqual(t, float32(-90), lat)
			assert.GreaterOrEqual(t, float32(90), lat)
			assert.LessOrEqual(t, float32(-180), lon)
			assert.GreaterOrEqual(t, float32(180), lon)
			// the same point on the sphere
			x1, y1, z1 := unitVector(test.lat, test.lon)
			x2, y2, z2 := unitVector(lat, lon)
			assert.InDelta(t, x1, x2, 1e-5)
			assert.InDelta(t, y1, y2, 1e-5)
			assert.InDelta(t, z1, z2, 1e-5)
		})
	}
}

func unitVector(lat, lon float32) (float64, float64, float64) {
	phi, lambda := float64(lat)*math.Pi/180, float64(lon)*math.Pi/180
	return math.Cos(phi) * math.Cos(lambda), math.Cos(phi) * math.Sin(lambda), math.Sin(phi)
}

// TestCircleAcceptsNoncanonicalCenter pins that a query centered on (0, 360)
// is the query centered on (0, 0), as it was for the geo HNSW.
func TestCircleAcceptsNoncanonicalCenter(t *testing.T) {
	c, err := NewCircle(0, 360, 1_000)
	require.NoError(t, err)
	assert.Equal(t, float32(0), c.Lon)
	assert.True(t, c.Contains(0, 0))
	assert.True(t, c.IntersectsBox(Box{0, 0, 0, 0}))
	assert.False(t, c.IntersectsBox(Box{0, 0, 10, 10}))
}

// destination is the point at angular distance d (radians) from (lat, lon)
// along the bearing, on the sphere, in degrees.
func destination(lat, lon, bearing, d float64) (float64, float64) {
	phi := lat * math.Pi / 180
	lambda := lon * math.Pi / 180
	phi2 := math.Asin(math.Sin(phi)*math.Cos(d) + math.Cos(phi)*math.Sin(d)*math.Cos(bearing))
	lambda2 := lambda + math.Atan2(math.Sin(bearing)*math.Sin(d)*math.Cos(phi),
		math.Cos(d)-math.Sin(phi)*math.Sin(phi2))
	lon2 := lambda2 * 180 / math.Pi
	for lon2 > 180 {
		lon2 -= 360
	}
	for lon2 < -180 {
		lon2 += 360
	}
	return phi2 * 180 / math.Pi, lon2
}

func stepFloat32(f float32, n int) float32 {
	for ; n > 0; n-- {
		f = math.Nextafter32(f, math.MaxFloat32)
	}
	for ; n < 0; n++ {
		f = math.Nextafter32(f, -math.MaxFloat32)
	}
	return f
}

// TestCircleBoxesCoverBoundary puts points right on the circle, and their
// float32 neighbours, and checks that none Contains accepts is outside the
// boxes. The boxes are derived from the ideal cap while Contains rounds to
// float32, so this is where the two can disagree: a point the memtable's scan
// finds would then vanish once the segment's tree prunes it.
func TestCircleBoxesCoverBoundary(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	checked := 0
	for i := 0; i < 2000; i++ {
		lat := float32(r.Float64()*180 - 90)
		lon := float32(r.Float64()*360 - 180)
		switch i % 4 {
		case 1: // hugging the antimeridian
			lon = float32(math.Copysign(180-math.Exp(r.Float64()*6-6), r.Float64()-0.5))
		case 2: // hugging a pole
			lat = float32(math.Copysign(90-math.Exp(r.Float64()*8-6), r.Float64()-0.5))
		}
		dist := float32(math.Exp(r.Float64() * 17)) // 1 m .. ~24,000 km
		c, err := NewCircle(lat, lon, dist)
		require.NoError(t, err)

		for j := 0; j < 64; j++ {
			bearing := r.Float64() * 2 * math.Pi
			d := float64(dist) / earthRadiusMeters * (1 + (r.Float64()-0.5)*1e-6)
			plat, plon := destination(float64(c.Lat), float64(c.Lon), bearing, d)
			base := Point{Lat: float32(plat), Lon: float32(plon)}
			for dlat := -2; dlat <= 2; dlat++ {
				for dlon := -2; dlon <= 2; dlon++ {
					p := Point{Lat: stepFloat32(base.Lat, dlat), Lon: stepFloat32(base.Lon, dlon)}
					p.Lat, p.Lon = Canonical(p.Lat, p.Lon)
					if !c.Contains(p.Lat, p.Lon) {
						continue
					}
					checked++
					require.True(t, c.IntersectsBox(Box{p.Lat, p.Lat, p.Lon, p.Lon}),
						"circle (%v,%v,%v) contains (%v,%v) but its boxes %v miss it", c.Lat, c.Lon, dist, p.Lat, p.Lon, c.boxes)
				}
			}
		}
	}
	require.Greater(t, checked, 1_000_000, "the sampling found too few boundary points")

	t.Run("a point across the antimeridian the formula accepts", func(t *testing.T) {
		c, err := NewCircle(10.43258285522461, 179.2627410888672, 178805.828125)
		require.NoError(t, err)
		lat, lon := float32(10.441040992736816), float32(-179.1021728515625)
		require.True(t, c.Contains(lat, lon))
		assert.True(t, c.IntersectsBox(Box{lat, lat, lon, lon}), "boxes %v", c.boxes)
	})
}

// TestSegmentReaderRejectsLoopingTree pins that a tree whose inner node points
// back at itself ends in an error rather than a stack that grows until the
// node runs out of memory.
func TestSegmentReaderRejectsLoopingTree(t *testing.T) {
	body := Build(genPoints(rand.New(rand.NewSource(12)), 4*MaxPointsPerLeaf), nil)
	reader, err := NewSegmentReader(body)
	require.NoError(t, err)
	require.Greater(t, reader.hdr.nodeCount, uint32(1))

	// the root's first child becomes the root
	binary.LittleEndian.PutUint32(body[bodyHeaderSize+16:], 0)
	c, _ := NewCircle(0, 0, float32(math.Pi*earthRadiusMeters))
	err = reader.WithinRange(context.Background(), c, sroar.NewBitmap(), sroar.NewBitmap())
	require.ErrorContains(t, err, "does not terminate")
}

// TestBodyWriteToMatchesBuild pins that the streamed layout is the one Build
// produces, byte for byte, and reports its size.
func TestBodyWriteToMatchesBuild(t *testing.T) {
	for _, n := range []int{0, 1, 3 * MaxPointsPerLeaf, 70_000} {
		points := genPoints(rand.New(rand.NewSource(int64(n))), n)
		del := sroar.NewBitmap()
		del.SetMany([]uint64{1, 2, 3})
		body := &Body{Points: append([]Point(nil), points...), Deletions: del.ToBuffer()}

		var buf bytes.Buffer
		written, err := body.WriteTo(&buf)
		require.NoError(t, err)
		assert.Equal(t, int64(body.Size()), written)
		assert.Equal(t, Build(points, del.ToBuffer()), buf.Bytes())
	}
}
