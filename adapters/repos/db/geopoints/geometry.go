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

// Package geopoints is the lsmkv strategy behind a geoCoordinates property's
// filterable index: an immutable BKD tree per segment, built at flush and
// rebuilt at compaction, plus a per-segment bitmap of deleted doc IDs. See
// docs/geo-index.md.
package geopoints

import (
	"fmt"
	"math"
)

// earthRadiusMeters is the radius the geo HNSW used, so distances are unchanged.
const earthRadiusMeters = 6371e3

type Point struct {
	DocID uint64
	Lat   float32
	Lon   float32
}

// ValidateCoordinates rejects NaN and infinite values, which have no place in
// a bounding box.
func ValidateCoordinates(lat, lon float32) error {
	if math.IsNaN(float64(lat)) || math.IsInf(float64(lat), 0) {
		return fmt.Errorf("latitude %v is not a finite number", lat)
	}
	if math.IsNaN(float64(lon)) || math.IsInf(float64(lon), 0) {
		return fmt.Errorf("longitude %v is not a finite number", lon)
	}
	return nil
}

// Canonical maps a coordinate pair to the same point on the sphere with the
// latitude in [-90, 90] and the longitude in [-180, 180]: (0, 360) is (0, 0),
// (100, 0) is (80, 180). Coordinates are not range-checked on write and the
// haversine formula is the great-circle distance for any real pair, but the
// bounding boxes only hold for canonical ones.
func Canonical(lat, lon float32) (float32, float32) {
	if lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 {
		return lat, lon
	}
	la, lo := float64(lat), float64(lon)

	// the latitude into [-180, 180), then folded over the pole it crossed
	la = math.Mod(la+180, 360)
	if la < 0 {
		la += 360
	}
	la -= 180
	switch {
	case la > 90:
		la, lo = 180-la, lo+180
	case la < -90:
		la, lo = -180-la, lo+180
	}

	lo = math.Mod(lo+180, 360)
	if lo < 0 {
		lo += 360
	}
	lo -= 180
	return float32(la), float32(lo)
}

// Distance is the haversine distance in meters, with the exact arithmetic of
// the former geo HNSW distancer so a filter's boundary has not moved.
func Distance(latA, lonA, latB, lonB float32) float32 {
	latARadian := float64(latA * math.Pi / 180)
	latBRadian := float64(latB * math.Pi / 180)
	deltaLatRadian := float64(latB-latA) * math.Pi / 180
	deltaLonRadian := float64(lonB-lonA) * math.Pi / 180

	a := math.Sin(deltaLatRadian/2)*math.Sin(deltaLatRadian/2) +
		math.Cos(latARadian)*math.Cos(latBRadian)*math.Sin(deltaLonRadian/2)*math.Sin(deltaLonRadian/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return float32(earthRadiusMeters * c)
}

// Box is a latitude/longitude rectangle, both ranges inclusive.
type Box struct {
	MinLat, MaxLat, MinLon, MaxLon float32
}

func (b Box) Intersects(o Box) bool {
	return b.MinLat <= o.MaxLat && b.MaxLat >= o.MinLat &&
		b.MinLon <= o.MaxLon && b.MaxLon >= o.MinLon
}

func (b Box) Contains(lat, lon float32) bool {
	return lat >= b.MinLat && lat <= b.MaxLat && lon >= b.MinLon && lon <= b.MaxLon
}

// Circle is a withinGeoRange query. Its bounding boxes prune the tree, split
// in two across the antimeridian or widened to every longitude over a pole;
// Contains decides on the leaf points.
type Circle struct {
	Lat, Lon float32 // canonical
	Distance float32

	boxes         []Box
	innerDistance float32 // see ContainsBox
	cosLat        float64 // precomputed as Distance computes it, so Contains matches it bit for bit
	// the haversine term of the radius, with a band around it inside which
	// only the full formula decides; see Contains
	aBelow, aAbove float64

	// rows is built once a query has checked enough points to pay for it; see
	// rowTable. A Circle serves one query on one goroutine.
	rows       *rowTable
	rowsFailed bool
	checks     int
}

func NewCircle(lat, lon, distance float32) (*Circle, error) {
	if err := ValidateCoordinates(lat, lon); err != nil {
		return nil, err
	}
	if math.IsNaN(float64(distance)) || distance < 0 {
		return nil, fmt.Errorf("distance %v must be a non-negative number", distance)
	}
	lat, lon = Canonical(lat, lon)
	c := &Circle{Lat: lat, Lon: lon, Distance: distance}
	c.cosLat = math.Cos(float64(lat * math.Pi / 180))
	c.boxes, c.innerDistance = c.bounds()
	c.aBelow, c.aAbove = c.haversineBand()
	return c, nil
}

// haversineBand brackets the haversine term sin²(r/2) of the radius: a point
// whose term is below the band is inside, above it outside, and only in
// between does the rounding of the full formula decide. The band is a relative
// 2^-16, far wider than the float32 roundings of Distance.
func (c *Circle) haversineBand() (float64, float64) {
	r := float64(c.Distance) / earthRadiusMeters
	if math.IsInf(r, 1) || r >= math.Pi {
		return 2, 2 // everything is inside
	}
	a := math.Pow(math.Sin(r/2), 2)
	const band = 1.0 / (1 << 16)
	return a * (1 - band), a * (1 + band)
}

// Contains rounds its inputs to float32, so the boxes must be wider than the
// ideal cap by the roundings' worst case: a coordinate difference is one
// float32 subtraction of values up to 360 apart (latPad, lonPad are twice
// 2^-24 of that), the distance is rounded once (relSlack on the radius), and a
// latitude in radians is computed in float32, which puts the cosines off by
// latRadianErr * tan(lat) relative (cosineSlack widens the radius for that).
const (
	relSlack      = 1.0 / (1 << 20)
	latPad        = 180.0 / (1 << 23)
	lonPad        = 360.0 / (1 << 23)
	latRadianErr  = 1.0 / (1 << 20)
	paddingMeters = 8.0 // latPad and lonPad on the ground, with room to spare
)

func cosineSlack(maxAbsLat float64) float64 {
	return 2 * latRadianErr * math.Tan(maxAbsLat*math.Pi/180)
}

// bounds computes the boxes and the inner radius for ContainsBox. The latitude
// extent of a cap is its angular radius; the longitude extent of one that
// reaches neither pole is asin(sin(r)/cos(lat)).
func (c *Circle) bounds() ([]Box, float32) {
	if math.IsInf(float64(c.Distance), 1) {
		return []Box{{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}}, 0
	}

	r := float64(c.Distance) / earthRadiusMeters * (1 + relSlack)
	rDeg := r*180/math.Pi + latPad
	cLat, cLon := float64(c.Lat), float64(c.Lon)
	minLat, maxLat := cLat-rDeg, cLat+rDeg

	if minLat <= -90 || maxLat >= 90 || r >= math.Pi/2 {
		return []Box{{
			MinLat: max(nextDown32(minLat), -90), MaxLat: min(nextUp32(maxLat), 90),
			MinLon: -180, MaxLon: 180,
		}}, 0
	}
	latBand := Box{MinLat: nextDown32(minLat), MaxLat: nextUp32(maxLat), MinLon: -180, MaxLon: 180}

	// the cosines' slack folded into the radius: a point the formula accepts is
	// within the cap whose haversine term is the radius' divided by (1 - slack)
	slack := cosineSlack(math.Max(math.Abs(minLat), math.Abs(maxLat)))
	if slack >= 1 {
		return []Box{latBand}, 0
	}
	a := math.Pow(math.Sin(r/2), 2) / (1 - slack)
	if a >= 1 {
		return []Box{latBand}, 0
	}
	rEff := 2 * math.Asin(math.Sqrt(a))
	// the widened cap may reach a pole where the original did not; past that
	// point sin(rEff) shrinks again and the formula below would narrow the box
	if math.Abs(cLat)*math.Pi/180+rEff >= math.Pi/2 {
		return []Box{latBand}, 0
	}
	ratio := math.Sin(rEff) / math.Cos(cLat*math.Pi/180)
	if ratio >= 1 {
		return []Box{latBand}, 0
	}
	dLon := math.Asin(ratio)*180/math.Pi*(1+relSlack) + lonPad
	minLon, maxLon := cLon-dLon, cLon+dLon

	// a point the formula places within the inner radius is within the radius
	// however the roundings fall, twice over, which ContainsBox needs
	inner := float64(c.Distance)*(1-2*slack-2*relSlack) - 2*paddingMeters
	if inner < 0 {
		inner = 0
	}
	innerDistance := float32(inner)

	if maxLon-minLon >= 360 {
		return []Box{latBand}, innerDistance
	}
	if minLon >= -180 && maxLon <= 180 {
		return []Box{{
			MinLat: latBand.MinLat, MaxLat: latBand.MaxLat,
			MinLon: nextDown32(minLon), MaxLon: nextUp32(maxLon),
		}}, innerDistance
	}

	// wraps around the antimeridian; the turn is applied before the float32 rounding
	if minLon < -180 {
		return []Box{
			{MinLat: latBand.MinLat, MaxLat: latBand.MaxLat, MinLon: -180, MaxLon: nextUp32(maxLon)},
			{MinLat: latBand.MinLat, MaxLat: latBand.MaxLat, MinLon: nextDown32(minLon + 360), MaxLon: 180},
		}, innerDistance
	}
	return []Box{
		{MinLat: latBand.MinLat, MaxLat: latBand.MaxLat, MinLon: nextDown32(minLon), MaxLon: 180},
		{MinLat: latBand.MinLat, MaxLat: latBand.MaxLat, MinLon: -180, MaxLon: nextUp32(maxLon - 360)},
	}, innerDistance
}

func nextUp32(f float64) float32 {
	return math.Nextafter32(float32(f), math.MaxFloat32)
}

func nextDown32(f float64) float32 {
	return math.Nextafter32(float32(f), -math.MaxFloat32)
}

// IntersectsBox is a necessary condition for any point of the box to be inside.
func (c *Circle) IntersectsBox(b Box) bool {
	for i := range c.boxes {
		if c.boxes[i].Intersects(b) {
			return true
		}
	}
	return false
}

// ContainsBox reports whether every point of the box is inside the circle, so
// a leaf can be collected without per-point distances. The farthest point of a
// rectangle from the center is a corner as long as every longitude offset
// stays under a half turn, which holds for a rectangle inside the circle's own
// bounding box when that box reaches neither a pole nor the antimeridian. The
// corners are held to the inner radius so the float32 roundings of Contains
// cannot reject a point of a box whose corners pass.
func (c *Circle) ContainsBox(b Box) bool {
	if len(c.boxes) != 1 || c.innerDistance <= 0 {
		return false
	}
	bb := c.boxes[0]
	if bb.MinLon <= -180 || bb.MaxLon >= 180 || bb.MinLat <= -90 || bb.MaxLat >= 90 {
		return false
	}
	if b.MinLat < bb.MinLat || b.MaxLat > bb.MaxLat || b.MinLon < bb.MinLon || b.MaxLon > bb.MaxLon {
		return false
	}
	return c.distance(b.MinLat, b.MinLon) <= c.innerDistance &&
		c.distance(b.MinLat, b.MaxLon) <= c.innerDistance &&
		c.distance(b.MaxLat, b.MinLon) <= c.innerDistance &&
		c.distance(b.MaxLat, b.MaxLon) <= c.innerDistance
}

// rowTable answers most point checks of a broad query without trigonometry.
// The circle's latitude band is cut into rows; for each, inner is a longitude
// offset within which every point of the row is inside, outer one beyond which
// every point is outside, both in degrees and measured the short way around.
// The offsets come from the haversine formula solved for longitude at the
// row's edges and at the latitude where the circle is widest, with the slack
// Contains' band uses, so a point the table decides is decided as Contains
// would. Only points between the two offsets run the formula. An inner of -1
// decides nothing inside, an outer of 180 nothing outside.
type rowTable struct {
	minLat, invRowHeight float64
	inner, outer         [rowCount]float64
}

const (
	rowCount = 64
	// rowBuildAfter is how many point checks a query makes before the table is
	// worth its few hundred trigonometric calls
	rowBuildAfter = 1024
)

// containsFast is Contains with the row table in front of it.
func (c *Circle) containsFast(lat, lon float32) bool {
	t := c.rows
	if t == nil {
		if c.rowsFailed {
			return c.Contains(lat, lon)
		}
		c.checks++
		if c.checks < rowBuildAfter {
			return c.Contains(lat, lon)
		}
		if t = c.buildRows(); t == nil {
			return c.Contains(lat, lon)
		}
	}
	row := (float64(lat) - t.minLat) * t.invRowHeight
	if row < 0 || row >= rowCount {
		return c.Contains(lat, lon)
	}
	i := int(row)
	// the subtraction Contains makes; the formula is periodic in it, so an
	// offset past a half turn is the same as the one the other way around
	d := math.Abs(float64(lon - c.Lon))
	if d > 180 {
		d = 360 - d
	}
	if d <= t.inner[i] {
		return true
	}
	if d > t.outer[i] {
		return false
	}
	return c.Contains(lat, lon)
}

// rejectsBox reports whether the row table places every point of the box
// outside the circle: on each row the box spans, its nearest longitude is
// beyond the row's outer offset. It prunes the leaves in the corners of the
// bounding box that the box test alone lets through. False without a table.
func (c *Circle) rejectsBox(b Box) bool {
	t := c.rows
	if t == nil {
		return false
	}
	// the box's nearest longitude offset to the center, the short way around:
	// an arc that does not hold the center is nearest at one of its ends
	cLon := float64(c.Lon)
	if float64(b.MinLon) <= cLon && cLon <= float64(b.MaxLon) {
		return false
	}
	d := math.Min(wrapOffset(float64(b.MinLon)-cLon), wrapOffset(float64(b.MaxLon)-cLon))
	// a point's offset is a float32 subtraction, within lonPad of d
	d -= lonPad

	// a box past the band in latitude falls to its nearest row, which is as
	// sound as any row since the box test already placed it inside the band
	lo := clampRow((float64(b.MinLat) - t.minLat) * t.invRowHeight)
	hi := clampRow((float64(b.MaxLat) - t.minLat) * t.invRowHeight)
	for i := lo; i <= hi; i++ {
		if d <= t.outer[i] {
			return false
		}
	}
	return true
}

func clampRow(row float64) int {
	return min(max(int(row), 0), rowCount-1)
}

// wrapOffset is the size of a longitude difference the short way around.
func wrapOffset(d float64) float64 {
	d = math.Abs(d)
	if d > 180 {
		d = 360 - d
	}
	return d
}

// buildRows fills the table. The cosines of Contains are off by up to
// cosineSlack of the latitudes involved, so each row folds its own slack into
// the thresholds; rows near a pole, where that slack is large, decide nothing.
func (c *Circle) buildRows() *rowTable {
	if c.aBelow >= 1 {
		c.rowsFailed = true
		return nil
	}
	bb := c.boxes[0]
	t := &rowTable{minLat: float64(bb.MinLat)}
	height := (float64(bb.MaxLat) - t.minLat) / rowCount
	if height <= 0 {
		c.rowsFailed = true
		return nil
	}
	t.invRowHeight = 1 / height
	tanCenter := math.Tan(math.Abs(float64(c.Lat)) * math.Pi / 180)
	for i := range rowCount {
		lo := t.minLat + float64(i)*height
		hi := lo + height
		maxAbsLat := math.Max(math.Abs(lo), math.Abs(hi))
		slack := latRadianErr*(tanCenter+math.Tan(maxAbsLat*math.Pi/180)) + relSlack
		if slack >= 1.0/(1<<8) {
			t.inner[i], t.outer[i] = -1, 180
			continue
		}
		aIn := c.aBelow * (1 - slack)
		aOut := c.aAbove * (1 + slack)
		// the offset is unimodal in latitude: a cap under a hemisphere peaks at
		// its axis latitude, a larger one is narrowest at the axis of the cap
		// it leaves out. So one extreme over a row is at an edge, the other at
		// the clamped axis.
		t.inner[i] = math.Min(c.lonOffset(lo, aIn), c.lonOffset(hi, aIn))
		if axis, trough := c.axisLat(aIn); trough && axis >= lo && axis <= hi {
			t.inner[i] = math.Min(t.inner[i], c.lonOffset(axis, aIn))
		}
		t.outer[i] = math.Max(c.lonOffset(lo, aOut), c.lonOffset(hi, aOut))
		if axis, trough := c.axisLat(aOut); !trough && axis >= lo && axis <= hi {
			t.outer[i] = math.Max(t.outer[i], c.lonOffset(axis, aOut))
		}
	}
	c.rows = t
	return t
}

// lonOffset is the longitude offset in degrees within which a point at
// latitude lat has a haversine term of at most a: the circle's half-width on
// that parallel. -1 when no offset does, 180 when every offset does.
func (c *Circle) lonOffset(lat, a float64) float64 {
	cLat := float64(c.Lat)
	dLat := (lat - cLat) * math.Pi / 180
	s1 := math.Sin(dLat / 2)
	s1 *= s1
	denom := math.Cos(cLat*math.Pi/180) * math.Cos(lat*math.Pi/180)
	if denom <= 0 {
		// at a pole every longitude is the same point
		if s1 <= a {
			return 180
		}
		return -1
	}
	x := (a - s1) / denom
	if x < 0 {
		return -1
	}
	if x >= 1 {
		return 180
	}
	return 2 * math.Asin(math.Sqrt(x)) * 180 / math.Pi
}

// axisLat is the latitude at which the cap with haversine term a is widest in
// longitude, asin(sin(lat) / cos(r)). For a cap over a hemisphere it is the
// latitude where it is narrowest instead, the axis of the cap it leaves out,
// which trough reports.
func (c *Circle) axisLat(a float64) (lat float64, trough bool) {
	r := 2 * math.Asin(math.Sqrt(a))
	if r > math.Pi/2 {
		r = math.Pi - r
		return -math.Asin(math.Sin(float64(c.Lat)*math.Pi/180)/math.Cos(r)) * 180 / math.Pi, true
	}
	if r == math.Pi/2 {
		return float64(c.Lat), false
	}
	return math.Asin(math.Sin(float64(c.Lat)*math.Pi/180)/math.Cos(r)) * 180 / math.Pi, false
}

// Contains decides as Distance would, bit for bit, but compares the haversine
// term against the radius' first and finishes the formula only for points
// within the band around the boundary.
func (c *Circle) Contains(lat, lon float32) bool {
	a := c.haversine(lat, lon)
	if a < c.aBelow {
		return true
	}
	if a > c.aAbove {
		return false
	}
	return c.finish(a) <= c.Distance
}

func (c *Circle) distance(lat, lon float32) float32 {
	return c.finish(c.haversine(lat, lon))
}

// haversine and finish are Distance split in two.
func (c *Circle) haversine(lat, lon float32) float64 {
	latBRadian := float64(lat * math.Pi / 180)
	deltaLatRadian := float64(lat-c.Lat) * math.Pi / 180
	deltaLonRadian := float64(lon-c.Lon) * math.Pi / 180

	return math.Sin(deltaLatRadian/2)*math.Sin(deltaLatRadian/2) +
		c.cosLat*math.Cos(latBRadian)*math.Sin(deltaLonRadian/2)*math.Sin(deltaLonRadian/2)
}

func (c *Circle) finish(a float64) float32 {
	return float32(earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a)))
}
