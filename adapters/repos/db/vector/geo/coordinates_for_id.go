//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

func geoCoordiantesToVector(in *models.GeoCoordinates) ([]float32, error) {
	if in.Latitude == nil {
		return nil, fmt.Errorf("latitude must be set")
	}

	if in.Longitude == nil {
		return nil, fmt.Errorf("longitude must be set")
	}

	return []float32{*in.Latitude, *in.Longitude}, nil
}
