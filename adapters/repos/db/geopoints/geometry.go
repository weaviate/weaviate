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

// Point is one indexed coordinate pair, canonical (see Canonical).
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

// Canonical maps a coordinate pair to the representation of the same point on
// the sphere with the latitude in [-90, 90] and the longitude in [-180, 180].
// Coordinates are not range-checked on write, and the haversine formula is the
// great-circle distance for any real pair, so (0, 360) is the point (0, 0) and
// (100, 0) the point (80, 180). The bounding boxes only hold for canonical
// pairs, so every stored point and every query center goes through here.
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
	// Lat and Lon are the canonical center.
	Lat, Lon float32
	Distance float32
	// boxes is the circle's bounding rectangle, split in two where it crosses
	// the antimeridian, or widened to every longitude where it covers a pole.
	boxes []Box
	// innerDistance is the radius a leaf's corners must be within for the
	// whole leaf to be collected without per-point checks; see ContainsBox.
	innerDistance float32
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
	lat, lon = Canonical(lat, lon)
	c := &Circle{Lat: lat, Lon: lon, Distance: distance}
	c.latRadian = float64(lat * math.Pi / 180)
	c.cosLat = math.Cos(c.latRadian)
	c.boxes, c.innerDistance = c.bounds()
	return c, nil
}

// The bounding boxes must hold every point Contains accepts, and Contains is
// not the ideal spherical cap: it rounds its inputs to float32. The slack
// below covers those roundings for canonical coordinates.
//
//   - Each coordinate difference is one float32 subtraction of values within
//     360 degrees of each other, so it is off by at most 2^-24 of a value up
//     to 360 (longitude) or 180 (latitude): latPad and lonPad are twice that.
//   - The distance it compares to the radius is rounded to float32 once, a
//     relative 2^-24, covered many times over by relSlack on the radius.
//   - A latitude in radians is computed in float32, in three roundings, so a
//     cosine of the formula is off by up to latRadianErr * tan(lat) relative.
//     That scales the longitude term of the formula; cosineSlack converts the
//     worst case over the circle's latitude band into a wider radius.
const (
	relSlack      = 1.0 / (1 << 20)
	latPad        = 180.0 / (1 << 23)
	lonPad        = 360.0 / (1 << 23)
	latRadianErr  = 1.0 / (1 << 20)
	paddingMeters = 8.0 // latPad and lonPad on the ground, with room to spare
)

// cosineSlack is the relative amount the haversine term the cosines scale can
// be off by for latitudes up to maxAbsLat degrees, both the point's and the
// center's.
func cosineSlack(maxAbsLat float64) float64 {
	return 2 * latRadianErr * math.Tan(maxAbsLat*math.Pi/180)
}

// bounds computes the rectangles covering the circle and the inner radius for
// ContainsBox. The latitude extent of a cap is its angular radius; the
// longitude extent of one that reaches neither pole is asin(sin(r)/cos(lat)).
// A cap reaching a pole covers every longitude. One crossing the antimeridian
// is split in two. Every edge is widened by the slack described above.
func (c *Circle) bounds() ([]Box, float32) {
	if math.IsInf(float64(c.Distance), 1) {
		return []Box{{MinLat: -90, MaxLat: 90, MinLon: -180, MaxLon: 180}}, 0
	}

	// the angular radius in radians and degrees, with the relative slack
	r := float64(c.Distance) / earthRadiusMeters * (1 + relSlack)
	rDeg := r*180/math.Pi + latPad
	cLat, cLon := float64(c.Lat), float64(c.Lon)
	minLat, maxLat := cLat-rDeg, cLat+rDeg

	if minLat <= -90 || maxLat >= 90 || r >= math.Pi/2 {
		// the cap covers a pole: every longitude is inside the latitude band
		return []Box{{
			MinLat: max(nextDown32(minLat), -90), MaxLat: min(nextUp32(maxLat), 90),
			MinLon: -180, MaxLon: 180,
		}}, 0
	}
	latBand := Box{MinLat: nextDown32(minLat), MaxLat: nextUp32(maxLat), MinLon: -180, MaxLon: 180}

	// the longitude extent, with the cosines' slack folded into the radius:
	// a point the formula accepts is within the cap whose haversine term is
	// the radius' divided by what the cosines may be off by
	slack := cosineSlack(math.Max(math.Abs(minLat), math.Abs(maxLat)))
	if slack >= 1 {
		return []Box{latBand}, 0
	}
	a := math.Pow(math.Sin(r/2), 2) / (1 - slack)
	if a >= 1 {
		return []Box{latBand}, 0
	}
	rEff := 2 * math.Asin(math.Sqrt(a))
	ratio := math.Sin(rEff) / math.Cos(cLat*math.Pi/180)
	if ratio >= 1 {
		return []Box{latBand}, 0
	}
	dLon := math.Asin(ratio)*180/math.Pi*(1+relSlack) + lonPad
	minLon, maxLon := cLon-dLon, cLon+dLon

	// the inner radius: a point the formula places within it is within the
	// radius however the roundings fall, twice over, which ContainsBox needs
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

	// wraps around the antimeridian: [minLon, 180] and [-180, maxLon] with the
	// overflow moved by one turn, in float64 so the turn adds no rounding
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

// nextUp32 rounds f to a float32 no smaller than it; nextDown32 to one no
// larger.
func nextUp32(f float64) float32 {
	return math.Nextafter32(float32(f), math.MaxFloat32)
}

func nextDown32(f float64) float32 {
	return math.Nextafter32(float32(f), -math.MaxFloat32)
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
// the per-point check. The corners are held to the inner radius so that the
// float32 roundings of Contains cannot make it reject a point of a box whose
// corners pass.
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

// Contains is the exact test: haversine distance within the radius. It is
// Distance with the center's terms precomputed.
func (c *Circle) Contains(lat, lon float32) bool {
	return c.distance(lat, lon) <= c.Distance
}

func (c *Circle) distance(lat, lon float32) float32 {
	latBRadian := float64(lat * math.Pi / 180)
	deltaLatRadian := float64(lat-c.Lat) * math.Pi / 180
	deltaLonRadian := float64(lon-c.Lon) * math.Pi / 180

	a := math.Sin(deltaLatRadian/2)*math.Sin(deltaLatRadian/2) +
		c.cosLat*math.Cos(latBRadian)*math.Sin(deltaLonRadian/2)*math.Sin(deltaLonRadian/2)

	return float32(earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a)))
}
