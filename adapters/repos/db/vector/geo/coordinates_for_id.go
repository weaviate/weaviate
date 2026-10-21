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

package geo

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

// CoordinatesForID must provide the geo coordinates for the specified index
// id
type CoordinatesForID func(ctx context.Context, id uint64) (*models.GeoCoordinates, error)

// VectorForID transforms the geo coordinates into a "vector" of fixed length
// two, where element 0 represents the latitude and element 1 represents the
// longitude. This way it is usable by a generic vector index such as HNSW
func (cfid CoordinatesForID) VectorForID(ctx context.Context, id uint64) ([]float32, error) {
	coordinates, err := cfid(ctx, id)
	if err != nil {
		return nil, err
	}

	return geoCoordiantesToVector(coordinates)
}

// CoordinatesFromObject must provide the geo coordinates held by one stored
// object, or nil if the object holds none.
type CoordinatesFromObject func(objectBytes []byte) (*models.GeoCoordinates, error)

// VectorFromObject transforms the coordinates of a stored object into the same
// two-element vector VectorForID produces. An object without coordinates yields
// a nil vector, which the cache prefill skips.
func (cfo CoordinatesFromObject) VectorFromObject(objectBytes []byte) ([]float32, error) {
	coordinates, err := cfo(objectBytes)
	if err != nil || coordinates == nil {
		return nil, err
	}

	return geoCoordiantesToVector(coordinates)
}

// GeoCoordinatesToVector converts geo coordinates to a vector of [lat, lon].
func GeoCoordinatesToVector(in *models.GeoCoordinates) ([]float32, error) {
	return geoCoordiantesToVector(in)
}

func geoCoordiantesToVector(in *models.GeoCoordinates) ([]float32, error) {
	if in.Latitude == nil {
		return nil, fmt.Errorf("latitude must be set")
	}

	if in.Longitude == nil {
		return nil, fmt.Errorf("longitude must be set")
	}

	return []float32{*in.Latitude, *in.Longitude}, nil
}
