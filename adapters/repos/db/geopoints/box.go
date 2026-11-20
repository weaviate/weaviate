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
	"fmt"
	"math"
)

// BoxQuery is a withinGeoBox query: the points between two latitudes whose
// longitude lies between west and east going east, so a west past the east
// spans the antimeridian. The edges are inclusive. The comparisons are exact
// on the stored float32 coordinates, so the tree needs no slack.
type BoxQuery struct {
	// one rectangle, two across the antimeridian, and a sliver for the other
	// name of the ±180 meridian when a rectangle ends on it
	boxes []Box
}

func NewBoxQuery(south, north, west, east float32) (*BoxQuery, error) {
	if err := ValidateCoordinates(south, west); err != nil {
		return nil, err
	}
	if err := ValidateCoordinates(north, east); err != nil {
		return nil, err
	}
	if south < -90 || north > 90 {
		return nil, fmt.Errorf("latitudes %v and %v must be within [-90, 90]", south, north)
	}
	if south > north {
		return nil, fmt.Errorf("south latitude %v is past north latitude %v", south, north)
	}

	if float64(east)-float64(west) >= 360 {
		return &BoxQuery{boxes: []Box{{MinLat: south, MaxLat: north, MinLon: -180, MaxLon: 180}}}, nil
	}
	west, east = wrapLongitude(west), wrapLongitude(east)

	q := &BoxQuery{}
	if west <= east {
		q.boxes = append(q.boxes, Box{MinLat: south, MaxLat: north, MinLon: west, MaxLon: east})
	} else {
		q.boxes = append(q.boxes,
			Box{MinLat: south, MaxLat: north, MinLon: west, MaxLon: 180},
			Box{MinLat: south, MaxLat: north, MinLon: -180, MaxLon: east})
	}

	// a stored longitude may be -180 or 180 for the same meridian
	var reachesEast, reachesWest bool
	for _, b := range q.boxes {
		reachesEast = reachesEast || b.MaxLon == 180
		reachesWest = reachesWest || b.MinLon == -180
	}
	switch {
	case reachesEast && !reachesWest:
		q.boxes = append(q.boxes, Box{MinLat: south, MaxLat: north, MinLon: -180, MaxLon: -180})
	case reachesWest && !reachesEast:
		q.boxes = append(q.boxes, Box{MinLat: south, MaxLat: north, MinLon: 180, MaxLon: 180})
	}
	return q, nil
}

// wrapLongitude brings a longitude into [-180, 180], leaving one already there
// as it is.
func wrapLongitude(lon float32) float32 {
	if lon >= -180 && lon <= 180 {
		return lon
	}
	lo := math.Mod(float64(lon)+180, 360)
	if lo < 0 {
		lo += 360
	}
	return float32(lo - 180)
}

// Contains reports whether a canonical point is inside.
func (q *BoxQuery) Contains(lat, lon float32) bool {
	for i := range q.boxes {
		if q.boxes[i].Contains(lat, lon) {
			return true
		}
	}
	return false
}

// IntersectsBox is a necessary condition for any point of b to be inside.
func (q *BoxQuery) IntersectsBox(b Box) bool {
	for i := range q.boxes {
		if q.boxes[i].Intersects(b) {
			return true
		}
	}
	return false
}

// ContainsBox reports whether every point of b is inside, so a leaf can be
// collected without looking at its coordinates.
func (q *BoxQuery) ContainsBox(b Box) bool {
	for i := range q.boxes {
		o := q.boxes[i]
		if b.MinLat >= o.MinLat && b.MaxLat <= o.MaxLat && b.MinLon >= o.MinLon && b.MaxLon <= o.MaxLon {
			return true
		}
	}
	return false
}
