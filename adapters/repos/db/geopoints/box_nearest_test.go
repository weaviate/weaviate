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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// genCanonicalPoints is genPoints as a bucket stores them.
func genCanonicalPoints(r *rand.Rand, n int) []Point {
	points := genPoints(r, n)
	for i := range points {
		points[i].Lat, points[i].Lon = Canonical(points[i].Lat, points[i].Lon)
	}
	return points
}

func TestNewBoxQuery(t *testing.T) {
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))
	tests := []struct {
		name                     string
		south, north, west, east float32
		wantErr                  string
		// a point is (lat, lon)
		inside, outside [][2]float32
	}{
		{
			name: "plain rectangle, edges included", south: 10, north: 20, west: 30, east: 40,
			inside:  [][2]float32{{10, 30}, {20, 40}, {15, 35}, {10, 40}},
			outside: [][2]float32{{9.99, 35}, {20.01, 35}, {15, 29.99}, {15, 40.01}, {15, -35}},
		},
		{
			name: "west past east spans the antimeridian", south: -10, north: 10, west: 170, east: -170,
			inside:  [][2]float32{{0, 170}, {0, 180}, {0, -180}, {0, -170}, {0, 175}, {0, -175}},
			outside: [][2]float32{{0, 169.99}, {0, -169.99}, {0, 0}, {11, 180}},
		},
		{
			name: "longitudes past 180 are wrapped", south: -10, north: 10, west: 170, east: 190,
			inside:  [][2]float32{{0, 175}, {0, -175}, {0, 180}, {0, -180}},
			outside: [][2]float32{{0, 0}, {0, 169}, {0, -169}},
		},
		{
			name: "ending on the antimeridian matches both of its names", south: -10, north: 10, west: 170, east: 180,
			inside:  [][2]float32{{0, 180}, {0, -180}, {0, 170}},
			outside: [][2]float32{{0, -179.99}, {0, 169.99}},
		},
		{
			name: "starting on the antimeridian matches both of its names", south: -10, north: 10, west: -180, east: -170,
			inside:  [][2]float32{{0, 180}, {0, -180}, {0, -170}},
			outside: [][2]float32{{0, 179.99}, {0, -169.99}},
		},
		{
			name: "a full turn is every longitude", south: -90, north: 90, west: -180, east: 180,
			inside: [][2]float32{{0, 0}, {90, 180}, {-90, -180}, {12, -77}},
		},
		{
			name: "more than a full turn is every longitude", south: 0, north: 1, west: 0, east: 360,
			inside:  [][2]float32{{0.5, 0}, {0.5, 180}, {0.5, -180}, {0.5, -90}},
			outside: [][2]float32{{2, 0}},
		},
		{
			name: "a point", south: 5, north: 5, west: 6, east: 6,
			inside:  [][2]float32{{5, 6}},
			outside: [][2]float32{{5, 6.001}, {5.001, 6}},
		},
		{name: "south past north", south: 20, north: 10, west: 0, east: 1, wantErr: "past north"},
		{name: "latitude below the pole", south: -91, north: 10, west: 0, east: 1, wantErr: "within [-90, 90]"},
		{name: "latitude above the pole", south: 0, north: 90.5, west: 0, east: 1, wantErr: "within [-90, 90]"},
		{name: "NaN latitude", south: nan, north: 10, west: 0, east: 1, wantErr: "not a finite number"},
		{name: "NaN longitude", south: 0, north: 10, west: 0, east: nan, wantErr: "not a finite number"},
		{name: "infinite longitude", south: 0, north: 10, west: inf, east: 1, wantErr: "not a finite number"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := NewBoxQuery(tt.south, tt.north, tt.west, tt.east)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, p := range tt.inside {
				assert.True(t, q.Contains(p[0], p[1]), "(%v, %v) is inside", p[0], p[1])
			}
			for _, p := range tt.outside {
				assert.False(t, q.Contains(p[0], p[1]), "(%v, %v) is outside", p[0], p[1])
			}
		})
	}
}

// boxContainsNaive decides a box query from its definition, independent of
// the rectangles NewBoxQuery cuts it into: going east from west reaches the
// point before it reaches east. The ±180 meridian is compared under one name.
func boxContainsNaive(south, north, west, east, lat, lon float32) bool {
	if lat < south || lat > north {
		return false
	}
	if float64(east)-float64(west) >= 360 {
		return true
	}
	turn := func(from, to float32) float64 {
		d := math.Mod(float64(to)-float64(from), 360)
		if d < 0 {
			d += 360
		}
		return d
	}
	return turn(west, lon) <= turn(west, east)
}

func randomBox(r *rand.Rand, points []Point) (south, north, west, east float32) {
	lat, lon := float32(r.Float64()*180-90), float32(r.Float64()*360-180)
	if len(points) > 0 && r.Intn(2) == 0 {
		p := points[r.Intn(len(points))]
		lat, lon = p.Lat, p.Lon
	}
	height := float32(math.Exp(r.Float64()*9) / 100) // 0.01 to 81 degrees
	width := float32(math.Exp(r.Float64()*10) / 100) // 0.01 to 220 degrees
	south, north = max(lat-height/2, -90), min(lat+height/2, 90)
	west, east = wrapLongitude(lon-width/2), wrapLongitude(lon+width/2)
	switch r.Intn(12) {
	case 0:
		west, east = -180, 180
	case 1:
		east = 180
	case 2:
		west = -180
	}
	return south, north, west, east
}

func TestBoxQueryMatchesBruteForce(t *testing.T) {
	sizes := []int{0, 1, MaxPointsPerLeaf, MaxPointsPerLeaf + 1, 5_000, 60_000}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("%d points", n), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(n) + 7))
			points := genCanonicalPoints(r, n)
			original := append([]Point(nil), points...)

			body := Build(points, nil)
			reader := mustReader(t, body)
			pread, err := NewSegmentReaderPread(bytes.NewReader(body), 0, int64(len(body)))
			require.NoError(t, err)
			memtable := NewMemtable()
			for _, p := range original {
				memtable.Add(p.DocID, p.Lat, p.Lon)
			}

			var matched int
			for i := 0; i < 200; i++ {
				south, north, west, east := randomBox(r, original)
				q, err := NewBoxQuery(south, north, west, east)
				require.NoError(t, err)

				want := []uint64{}
				for _, p := range original {
					// stored longitudes of -180 and 180 name one meridian
					in := boxContainsNaive(south, north, west, east, p.Lat, p.Lon)
					if !in && (p.Lon == 180 || p.Lon == -180) {
						in = boxContainsNaive(south, north, west, east, p.Lat, -p.Lon)
					}
					if in {
						want = append(want, p.DocID)
					}
				}
				sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
				matched += len(want)

				for name, layer := range map[string]InnerReader{"segment": reader, "pread": pread, "memtable": memtable} {
					out, del := sroar.NewBitmap(), sroar.NewBitmap()
					require.NoError(t, layer.WithinBox(context.Background(), q, out, del))
					assert.Equal(t, want, out.ToArray(), "%s: box lat [%v, %v] lon %v east to %v",
						name, south, north, west, east)
					assert.True(t, del.IsEmpty())
				}
			}
			if n >= 5_000 {
				require.Greater(t, matched, 1000, "the boxes matched next to nothing")
			}
		})
	}
}

func TestBoxQueryContainsBoxIsSound(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	var contained int
	for i := 0; i < 20_000; i++ {
		south, north, west, east := randomBox(r, nil)
		q, err := NewBoxQuery(south, north, west, east)
		require.NoError(t, err)

		lat0, lat1 := float32(r.Float64()*180-90), float32(r.Float64()*180-90)
		lon0, lon1 := float32(r.Float64()*360-180), float32(r.Float64()*360-180)
		if r.Intn(2) == 0 {
			// a small box near the query's own corner, likelier to be inside
			lat0, lon0 = south+float32(r.Float64()), west+float32(r.Float64())
			lat1, lon1 = lat0+float32(r.Float64()), lon0+float32(r.Float64())
		}
		b := Box{MinLat: min(lat0, lat1), MaxLat: max(lat0, lat1), MinLon: min(lon0, lon1), MaxLon: max(lon0, lon1)}
		if b.MinLat < -90 || b.MaxLat > 90 || b.MinLon < -180 || b.MaxLon > 180 {
			continue
		}

		containsAll := q.ContainsBox(b)
		intersects := q.IntersectsBox(b)
		if containsAll {
			contained++
			require.True(t, intersects)
		}
		for j := 0; j < 20; j++ {
			lat := b.MinLat + float32(r.Float64())*(b.MaxLat-b.MinLat)
			lon := b.MinLon + float32(r.Float64())*(b.MaxLon-b.MinLon)
			lat, lon = min(max(lat, b.MinLat), b.MaxLat), min(max(lon, b.MinLon), b.MaxLon)
			in := q.Contains(lat, lon)
			if containsAll {
				require.True(t, in, "ContainsBox holds but (%v, %v) is outside", lat, lon)
			}
			if in {
				require.True(t, intersects, "(%v, %v) is inside a box IntersectsBox rejects", lat, lon)
			}
		}
	}
	require.Greater(t, contained, 100, "the sampling never exercised ContainsBox")
}

// nearestBruteForce is the definition of a nearest search: every kept point by
// distance, ties by doc ID, cut at limit.
func nearestBruteForce(points []Point, lat, lon float32, limit int, keep func(uint64) bool) []Neighbor {
	lat, lon = Canonical(lat, lon)
	all := make([]Neighbor, 0, len(points))
	for _, p := range points {
		if keep != nil && !keep(p.DocID) {
			continue
		}
		all = append(all, Neighbor{DocID: p.DocID, Distance: Distance(lat, lon, p.Lat, p.Lon)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].closer(all[j]) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

func TestNearestMatchesBruteForce(t *testing.T) {
	sizes := []int{0, 1, 2, MaxPointsPerLeaf, MaxPointsPerLeaf + 1, 5_000, 60_000}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("%d points", n), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(n) + 3))
			points := genCanonicalPoints(r, n)
			original := append([]Point(nil), points...)

			// three layers, as a bucket has them: two segments and a memtable
			cutA, cutB := n/2, n*4/5
			segA := mustReader(t, Build(append([]Point(nil), original[:cutA]...), nil))
			bodyB := Build(append([]Point(nil), original[cutA:cutB]...), nil)
			segB, err := NewSegmentReaderPread(bytes.NewReader(bodyB), 0, int64(len(bodyB)))
			require.NoError(t, err)
			memtable := NewMemtable()
			for _, p := range original[cutB:] {
				memtable.Add(p.DocID, p.Lat, p.Lon)
			}
			layers := []NearestReader{memtable.Snapshot(), segA, segB}

			for q := 0; q < 150; q++ {
				lat, lon := float32(r.Float64()*180-90), float32(r.Float64()*360-180)
				switch {
				case n > 0 && r.Intn(3) == 0:
					p := original[r.Intn(n)]
					lat, lon = p.Lat, p.Lon
				case r.Intn(8) == 0:
					// the poles and the antimeridian, where the bound's boxes wrap
					lat = float32([]float64{90, -90, 0, 89.9999}[r.Intn(4)])
					lon = float32([]float64{180, -180, 179.9999, 0}[r.Intn(4)])
				}
				limit := []int{0, 1, 3, 10, 100, 700, n + 5}[r.Intn(7)]

				var keep func(uint64) bool
				switch r.Intn(4) {
				case 0:
					keep = func(id uint64) bool { return id%3 != 0 }
				case 1:
					keep = func(id uint64) bool { return id%97 == 5 } // a selective filter
				}

				nearest, err := NewNearest(lat, lon, limit, keep)
				require.NoError(t, err)
				for _, layer := range layers {
					require.NoError(t, layer.Nearest(context.Background(), nearest))
				}

				want := nearestBruteForce(original, lat, lon, limit, keep)
				got := nearest.Results()
				if len(want) == 0 {
					assert.Empty(t, got)
					continue
				}
				assert.Equal(t, want, got, "origin (%v, %v) limit %d", lat, lon, limit)
			}
		})
	}
}

// TestNearestPrunes pins that a nearest search reads a small part of a large
// tree: without the bound it would be a scan.
func TestNearestPrunes(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	points := make([]Point, 200_000)
	for i := range points {
		points[i] = Point{DocID: uint64(i), Lat: float32(r.Float64()*160 - 80), Lon: float32(r.Float64()*360 - 180)}
	}
	reader := mustReader(t, Build(points, nil))

	var asked int
	nearest, err := NewNearest(48.1, 11.6, 10, func(uint64) bool { asked++; return true })
	require.NoError(t, err)
	require.NoError(t, reader.Nearest(context.Background(), nearest))
	require.Len(t, nearest.Results(), 10)
	// keep is asked only for points that would enter the result
	assert.Less(t, asked, 5_000, "the search considered %d of %d points", asked, len(points))
}

func TestNearestHonoursContext(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	reader := mustReader(t, Build(genCanonicalPoints(r, 100_000), nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// every doc ID is refused, so nothing bounds the search and it must visit
	// the whole tree, or stop at the context
	nearest, err := NewNearest(0, 0, 10, func(uint64) bool { return false })
	require.NoError(t, err)
	require.ErrorIs(t, reader.Nearest(ctx, nearest), context.Canceled)

	box, err := NewBoxQuery(-90, 90, -180, 180)
	require.NoError(t, err)
	err = reader.WithinBox(ctx, box, sroar.NewBitmap(), sroar.NewBitmap())
	require.ErrorIs(t, err, context.Canceled)
}

func TestNewNearestRejectsBadInput(t *testing.T) {
	_, err := NewNearest(float32(math.NaN()), 0, 10, nil)
	require.Error(t, err)
	_, err = NewNearest(0, float32(math.Inf(-1)), 10, nil)
	require.Error(t, err)
	_, err = NewNearest(0, 0, -1, nil)
	require.Error(t, err)
}

func TestMemtableBoxAndNearest(t *testing.T) {
	m := NewMemtable()
	m.Add(1, 10, 10)
	m.Add(2, 10.001, 10)
	m.Add(3, 50, 50)
	m.Remove(2)
	m.Remove(99) // a point in an older segment

	box, err := NewBoxQuery(9, 11, 9, 11)
	require.NoError(t, err)
	out, deleted := sroar.NewBitmap(), sroar.NewBitmap()
	require.NoError(t, m.WithinBox(context.Background(), box, out, deleted))
	// the memtable reports both; the bucket subtracts
	assert.Equal(t, []uint64{1, 2}, out.ToArray())
	assert.Equal(t, []uint64{2, 99}, deleted.ToArray())

	search := func(snap *MemtableSnapshot) []uint64 {
		collected := sroar.NewBitmap()
		require.NoError(t, snap.CollectDeletions(collected))
		nearest, err := NewNearest(10, 10, 5, func(id uint64) bool { return !collected.Contains(id) })
		require.NoError(t, err)
		require.NoError(t, snap.Nearest(context.Background(), nearest))
		var ids []uint64
		for _, n := range nearest.Results() {
			ids = append(ids, n.DocID)
		}
		return ids
	}

	snap := m.Snapshot()
	assert.Equal(t, []uint64{1, 3}, search(snap))

	// an update after the snapshot retires doc 1 for doc 4 at the same place,
	// and enough writes follow to move the points to a new backing array
	m.Remove(1)
	m.Add(4, 10, 10)
	for id := uint64(100); id < 5000; id++ {
		m.Add(id, 10, 10)
		m.Remove(id)
	}
	assert.Equal(t, []uint64{1, 3}, search(snap), "the snapshot still is the memtable before the update")
	assert.Equal(t, []uint64{4, 3}, search(m.Snapshot()), "a new one sees the update whole")
}

// A snapshot of a memtable without removals shares nothing a search could
// change, and one taken empty stays empty.
func TestMemtableSnapshotEdges(t *testing.T) {
	m := NewMemtable()
	empty := m.Snapshot()
	m.Add(1, 1, 1)

	nearest, err := NewNearest(1, 1, 3, nil)
	require.NoError(t, err)
	require.NoError(t, empty.Nearest(context.Background(), nearest))
	assert.Empty(t, nearest.Results())

	deleted := sroar.NewBitmap()
	require.NoError(t, m.Snapshot().CollectDeletions(deleted))
	assert.True(t, deleted.IsEmpty())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for id := uint64(2); id < 10_000; id++ {
		m.Add(id, 1, 1)
	}
	nearest, err = NewNearest(1, 1, 3, nil)
	require.NoError(t, err)
	require.ErrorIs(t, m.Snapshot().Nearest(ctx, nearest), context.Canceled)
}

// BenchmarkNearest searches one segment of a million clustered points.
func BenchmarkNearest(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	centers := make([][2]float32, 200)
	for i := range centers {
		centers[i] = [2]float32{float32(r.Float64()*140 - 70), float32(r.Float64()*360 - 180)}
	}
	points := make([]Point, 1_000_000)
	for i := range points {
		var lat, lon float32
		if r.Intn(5) == 0 {
			lat, lon = float32(r.Float64()*180-90), float32(r.Float64()*360-180)
		} else {
			c := centers[r.Intn(len(centers))]
			lat, lon = c[0]+float32(r.NormFloat64()*0.5), c[1]+float32(r.NormFloat64()*0.5)
		}
		lat, lon = Canonical(lat, lon)
		points[i] = Point{DocID: uint64(i), Lat: lat, Lon: lon}
	}
	reader, err := NewSegmentReader(Build(points, nil))
	require.NoError(b, err)

	for _, limit := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("limit %d", limit), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c := centers[i%len(centers)]
				nearest, _ := NewNearest(c[0], c[1], limit, nil)
				if err := reader.Nearest(context.Background(), nearest); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
