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
	return c, nil
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

// Contains is Distance with the center's terms precomputed.
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
