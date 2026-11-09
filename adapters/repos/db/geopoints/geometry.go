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
// filterable index. Every segment holds an immutable BKD tree over its points,
// built at flush and rebuilt at compaction, so a withinGeoRange filter is an
// exact bounding-box descent followed by a haversine check on the leaves it
// reaches. Deletions are a per-segment bitmap of doc IDs that newer segments
// use to mask points of older ones.
package geopoints

import (
	"fmt"
	"math"
)

// earthRadiusMeters is the sphere the haversine distance is computed on. It is
// the radius the geo HNSW used, so distances are unchanged.
const earthRadiusMeters = 6371e3

// Point is one indexed coordinate pair.
type Point struct {
	DocID uint64
	Lat   float32
	Lon   float32
}

// ValidateCoordinates rejects values the tree cannot order. NaN breaks the
// bounding boxes and sorts, and an infinite coordinate has no distance.
func ValidateCoordinates(lat, lon float32) error {
	if math.IsNaN(float64(lat)) || math.IsInf(float64(lat), 0) {
		return fmt.Errorf("latitude %v is not a finite number", lat)
	}
	if math.IsNaN(float64(lon)) || math.IsInf(float64(lon), 0) {
		return fmt.Errorf("longitude %v is not a finite number", lon)
	}
	return nil
}

// Distance is the haversine distance in meters between two points. It is the
// exact arithmetic of the former geo HNSW distancer so that a filter keeps
// returning the same objects at its boundary.
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

// Intersects reports whether the two rectangles share at least one point.
func (b Box) Intersects(o Box) bool {
	return b.MinLat <= o.MaxLat && b.MaxLat >= o.MinLat &&
		b.MinLon <= o.MaxLon && b.MaxLon >= o.MinLon
}

// Contains reports whether the rectangle holds the point.
func (b Box) Contains(lat, lon float32) bool {
	return lat >= b.MinLat && lat <= b.MaxLat && lon >= b.MinLon && lon <= b.MaxLon
}

// Circle is a withinGeoRange query: a center and a radius in meters. Its
// bounding boxes prune the tree; Contains decides on the leaf points.
type Circle struct {
	Lat, Lon float32
	Distance float32
	// boxes is the circle's bounding rectangle, split in two where it crosses
	// the antimeridian, or widened to every longitude where it covers a pole.
	boxes []Box
	// the center's terms of the haversine formula, computed once; the values
	// are exactly those Distance derives, so Contains matches it bit for bit
	latRadian float64
	cosLat    float64
}

// NewCircle validates the center and precomputes the bounding boxes.
func NewCircle(lat, lon, distance float32) (*Circle, error) {
	if err := ValidateCoordinates(lat, lon); err != nil {
		return nil, err
	}
	if math.IsNaN(float64(distance)) || distance < 0 {
		return nil, fmt.Errorf("distance %v must be a non-negative number", distance)
	}
	c := &Circle{Lat: lat, Lon: lon, Distance: distance}
	c.boxes = c.boundingBoxes()
	c.latRadian = float64(lat * math.Pi / 180)
	c.cosLat = math.Cos(c.latRadian)
	return c, nil
}

// boundingBoxes computes the rectangles covering a spherical cap. The
// latitude extent is the angular radius; the longitude extent of a cap that
// reaches neither pole is asin(sin(r)/cos(lat)). A cap reaching a pole covers
// every longitude. One crossing the antimeridian is split in two.
func (c *Circle) boundingBoxes() []Box {
	const (
		everyLat = float32(math.MaxFloat32)
		everyLon = float32(math.MaxFloat32)
	)
	if math.IsInf(float64(c.Distance), 1) {
		return []Box{{MinLat: -everyLat, MaxLat: everyLat, MinLon: -everyLon, MaxLon: everyLon}}
	}

	// angular radius in degrees, rounded up so float32 coordinates right on
	// the boundary are not pruned before the exact check
	r := float64(c.Distance) / earthRadiusMeters
	rDeg := nextUp32(float32(r * 180 / math.Pi))
	minLat := c.Lat - rDeg
	maxLat := c.Lat + rDeg

	if minLat <= -90 || maxLat >= 90 || r >= math.Pi/2 {
		// the cap covers a pole: every longitude is inside the latitude band;
		// the band is left open towards the pole for coordinates stored past it
		if minLat <= -90 {
			minLat = -everyLat
		}
		if maxLat >= 90 {
			maxLat = everyLat
		}
		return []Box{{MinLat: minLat, MaxLat: maxLat, MinLon: -everyLon, MaxLon: everyLon}}
	}

	latRad := float64(c.Lat) * math.Pi / 180
	ratio := math.Sin(r) / math.Cos(latRad)
	if ratio >= 1 {
		return []Box{{MinLat: minLat, MaxLat: maxLat, MinLon: -everyLon, MaxLon: everyLon}}
	}
	dLon := nextUp32(float32(math.Asin(ratio) * 180 / math.Pi))
	minLon := c.Lon - dLon
	maxLon := c.Lon + dLon

	if minLon >= -180 && maxLon <= 180 {
		return []Box{{MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: maxLon}}
	}
	if maxLon-minLon >= 360 {
		return []Box{{MinLat: minLat, MaxLat: maxLat, MinLon: -everyLon, MaxLon: everyLon}}
	}

	// wraps around the antimeridian: [minLon, 180] and [-180, maxLon] with the
	// overflow moved by one turn. The box reaching the edge is left open past
	// it, so a coordinate stored as 181 is still reached.
	if minLon < -180 {
		return []Box{
			{MinLat: minLat, MaxLat: maxLat, MinLon: -everyLon, MaxLon: maxLon},
			{MinLat: minLat, MaxLat: maxLat, MinLon: minLon + 360, MaxLon: everyLon},
		}
	}
	return []Box{
		{MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: everyLon},
		{MinLat: minLat, MaxLat: maxLat, MinLon: -everyLon, MaxLon: maxLon - 360},
	}
}

func nextUp32(f float32) float32 {
	return math.Nextafter32(f, math.MaxFloat32)
}

// IntersectsBox reports whether the circle's bounding rectangle reaches the
// box. It is a necessary condition for any point of the box to be inside.
func (c *Circle) IntersectsBox(b Box) bool {
	for i := range c.boxes {
		if c.boxes[i].Intersects(b) {
			return true
		}
	}
	return false
}

// ContainsBox reports whether every point of the box is inside the circle,
// in which case a leaf can be collected without per-point distances. Along a
// meridian the distance to the center has a single minimum, so over a
// latitude interval it peaks at an end; along a parallel it grows with the
// longitude offset while that stays under a half turn. The farthest point of
// a rectangle is therefore one of its corners, provided the rectangle lies in
// the circle's own bounding box, which keeps every offset under a half turn.
// A circle reaching a pole or the antimeridian has no such box and is left to
// the per-point check.
func (c *Circle) ContainsBox(b Box) bool {
	if len(c.boxes) != 1 {
		return false
	}
	bb := c.boxes[0]
	if bb.MinLon <= -180 || bb.MaxLon >= 180 || bb.MinLat <= -90 || bb.MaxLat >= 90 {
		return false
	}
	if b.MinLat < bb.MinLat || b.MaxLat > bb.MaxLat || b.MinLon < bb.MinLon || b.MaxLon > bb.MaxLon {
		return false
	}
	return c.Contains(b.MinLat, b.MinLon) && c.Contains(b.MinLat, b.MaxLon) &&
		c.Contains(b.MaxLat, b.MinLon) && c.Contains(b.MaxLat, b.MaxLon)
}

// Contains is the exact test: haversine distance within the radius. It is
// Distance with the center's terms precomputed.
func (c *Circle) Contains(lat, lon float32) bool {
	latBRadian := float64(lat * math.Pi / 180)
	deltaLatRadian := float64(lat-c.Lat) * math.Pi / 180
	deltaLonRadian := float64(lon-c.Lon) * math.Pi / 180

	a := math.Sin(deltaLatRadian/2)*math.Sin(deltaLatRadian/2) +
		c.cosLat*math.Cos(latBRadian)*math.Sin(deltaLonRadian/2)*math.Sin(deltaLonRadian/2)

	d := float32(earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a)))
	return d <= c.Distance
}
