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

package propertyspecific

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/schema"
)

// Index - for now - only supports a Geo index as a property-specific index.
// This could be extended in the future, for example to allow vectorization of
// single properties, as opposed to only allowing vectorization of the entire
// object.
type Index struct {
	Name     string
	Type     schema.DataType
	GeoIndex *geo.Index
}

// Indices is a collection of property-specific Indices by propname
type Indices map[string]Index

// ByProp retrieves a property-specific index by prop name. Second argument is
// false, if the index doesn't exist.
func (i Indices) ByProp(propName string) (Index, bool) {
	index, ok := i[propName]
	return index, ok
}

func (i Indices) DropAll(ctx context.Context) error {
	for propName, index := range i {
		if index.Type != schema.DataTypeGeoCoordinates {
			return errors.Errorf("no implementation to delete property %s index of type %v",
				propName, index.Type)
		}

		if err := index.GeoIndex.Drop(ctx); err != nil {
			return errors.Wrapf(err, "drop property %s", propName)
		}

		index.GeoIndex = nil
		delete(i, propName)

	}
	return nil
}
