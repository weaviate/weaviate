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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// createsGeoIndex decides which props leave the per-property fan-out, so it has
// to agree with the branch createPropertyValueIndex actually takes.
func TestCreatesGeoIndex(t *testing.T) {
	geoDataType := []string{string(schema.DataTypeGeoCoordinates)}

	tests := []struct {
		name string
		prop *models.Property
		want bool
	}{
		{
			name: "geo prop with filterable defaulted",
			prop: &models.Property{Name: "location", DataType: geoDataType},
			want: true,
		},
		{
			name: "geo prop with filterable set",
			prop: &models.Property{Name: "location", DataType: geoDataType, IndexFilterable: boolPtr(true)},
			want: true,
		},
		{
			// createPropertyValueIndex never reaches initGeoProp for these, so
			// batching them would claim work that is not happening
			name: "geo prop with filterable off",
			prop: &models.Property{Name: "location", DataType: geoDataType, IndexFilterable: boolPtr(false)},
			want: false,
		},
		{
			name: "text prop",
			prop: &models.Property{Name: "name", DataType: schema.DataTypeText.PropString()},
			want: false,
		},
		{
			name: "int prop",
			prop: &models.Property{Name: "count", DataType: schema.DataTypeInt.PropString()},
			want: false,
		},
		{
			name: "reference prop",
			prop: &models.Property{Name: "parkedAt", DataType: []string{"MultiRefParkingGarage"}},
			want: false,
		},
		{
			name: "prop naming two datatypes",
			prop: &models.Property{Name: "parkedAt", DataType: []string{"MultiRefParkingGarage", "MultiRefParkingLot"}},
			want: false,
		},
		{
			name: "prop with no datatype",
			prop: &models.Property{Name: "broken"},
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, createsGeoIndex(test.prop))
		})
	}
}
