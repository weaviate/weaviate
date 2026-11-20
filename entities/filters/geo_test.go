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

package filters

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// A filter travels to the shards of other nodes as JSON, where a geo value is
// either a range or a box.
func TestGeoValuesSurviveSerialization(t *testing.T) {
	tests := map[string]*Clause{
		"range": {
			Operator: OperatorWithinGeoRange,
			On:       &Path{Class: "Car", Property: "parkedAt"},
			Value: &Value{Type: schema.DataTypeGeoCoordinates, Value: GeoRange{
				GeoCoordinates: &models.GeoCoordinates{Latitude: ptFloat32(51.51), Longitude: ptFloat32(-0.09)},
				Distance:       2000,
			}},
		},
		"range at the origin": {
			Operator: OperatorWithinGeoRange,
			On:       &Path{Class: "Car", Property: "parkedAt"},
			Value: &Value{Type: schema.DataTypeGeoCoordinates, Value: GeoRange{
				GeoCoordinates: &models.GeoCoordinates{Latitude: ptFloat32(0), Longitude: ptFloat32(0)},
			}},
		},
		"box": {
			Operator: OperatorWithinGeoBox,
			On:       &Path{Class: "Car", Property: "parkedAt"},
			Value: &Value{Type: schema.DataTypeGeoCoordinates, Value: GeoBox{
				SouthWest: &models.GeoCoordinates{Latitude: ptFloat32(-20), Longitude: ptFloat32(175)},
				NorthEast: &models.GeoCoordinates{Latitude: ptFloat32(-10), Longitude: ptFloat32(-170)},
			}},
		},
		"box at the origin": {
			Operator: OperatorWithinGeoBox,
			On:       &Path{Class: "Car", Property: "parkedAt"},
			Value: &Value{Type: schema.DataTypeGeoCoordinates, Value: GeoBox{
				SouthWest: &models.GeoCoordinates{Latitude: ptFloat32(0), Longitude: ptFloat32(0)},
				NorthEast: &models.GeoCoordinates{Latitude: ptFloat32(0), Longitude: ptFloat32(0)},
			}},
		},
	}
	for name, before := range tests {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(&LocalFilter{Root: before})
			require.NoError(t, err)

			var after LocalFilter
			require.NoError(t, json.Unmarshal(raw, &after))
			assert.Equal(t, before.Operator, after.Root.Operator)
			assert.Equal(t, before.Value.Value, after.Root.Value.Value)
			assert.Equal(t, before.Value.Type, after.Root.Value.Type)
		})
	}

	t.Run("a sort by distance", func(t *testing.T) {
		before := []Sort{
			{Path: []string{"parkedAt"}, Order: "asc", DistanceFrom: &SortGeoOrigin{Latitude: 52.5, Longitude: 13.4}},
			{Path: []string{"modelName"}, Order: "desc"},
		}
		raw, err := json.Marshal(before)
		require.NoError(t, err)
		assert.NotContains(t, string(raw[len(raw)/2:]), "distanceFrom", "a sort by value carries no origin")

		var after []Sort
		require.NoError(t, json.Unmarshal(raw, &after))
		assert.Equal(t, before, after)
	})
}

func TestOperatorWithinGeoBox(t *testing.T) {
	assert.Equal(t, "WithinGeoBox", OperatorWithinGeoBox.Name())
	assert.True(t, OperatorWithinGeoBox.OnValue())
	assert.False(t, OperatorWithinGeoBox.IsContains())
	// operators are sent to other nodes as their number: the existing ones
	// must keep theirs
	assert.Equal(t, Operator(15), OperatorNot)
	assert.Equal(t, Operator(16), OperatorWithinGeoBox)
}

func geoTestClass(string) (*models.Class, error) {
	return &models.Class{
		Class: "Car",
		Properties: []*models.Property{
			{Name: "modelName", DataType: schema.DataTypeText.PropString()},
			{Name: "horsepower", DataType: schema.DataTypeInt.PropString()},
			{Name: "parkedAt", DataType: schema.DataTypeGeoCoordinates.PropString()},
		},
	}, nil
}

func TestValidateGeoFilters(t *testing.T) {
	geoRange := GeoRange{
		GeoCoordinates: &models.GeoCoordinates{Latitude: ptFloat32(51.51), Longitude: ptFloat32(-0.09)},
		Distance:       2000,
	}
	geoBox := GeoBox{
		SouthWest: &models.GeoCoordinates{Latitude: ptFloat32(47), Longitude: ptFloat32(5)},
		NorthEast: &models.GeoCoordinates{Latitude: ptFloat32(55), Longitude: ptFloat32(15)},
	}
	tests := []struct {
		name     string
		prop     string
		propType schema.DataType
		operator Operator
		value    interface{}
		wantErr  string
	}{
		{name: "range", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates, operator: OperatorWithinGeoRange, value: geoRange},
		{name: "box", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates, operator: OperatorWithinGeoBox, value: geoBox},
		{
			name: "range value with the box operator", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates,
			operator: OperatorWithinGeoBox, value: geoRange, wantErr: "needs operator WithinGeoRange",
		},
		{
			name: "box value with the range operator", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates,
			operator: OperatorWithinGeoRange, value: geoBox, wantErr: "needs operator WithinGeoBox",
		},
		{
			name: "range value with equal", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates,
			operator: OperatorEqual, value: geoRange, wantErr: "needs operator WithinGeoRange",
		},
		{
			name: "a string on a geo property", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates,
			operator: OperatorEqual, value: "berlin", wantErr: "can only be filtered with a geo range or a geo box",
		},
		{
			name: "no value on a geo property", prop: "parkedAt", propType: schema.DataTypeGeoCoordinates,
			operator: OperatorWithinGeoRange, value: nil, wantErr: "can only be filtered with a geo range or a geo box",
		},
		{
			name: "the box operator on a text property", prop: "modelName", propType: schema.DataTypeText,
			operator: OperatorWithinGeoBox, value: geoBox, wantErr: "needs a geoCoordinates property",
		},
		{
			name: "the range operator on an int property", prop: "horsepower", propType: schema.DataTypeInt,
			operator: OperatorWithinGeoRange, value: geoRange, wantErr: "needs a geoCoordinates property",
		},
		{name: "other properties keep their operators", prop: "horsepower", propType: schema.DataTypeInt, operator: OperatorEqual, value: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := &LocalFilter{Root: &Clause{
				Operator: tt.operator,
				On:       &Path{Class: "Car", Property: schema.PropertyName(tt.prop)},
				Value:    &Value{Value: tt.value, Type: tt.propType},
			}}
			err := ValidateFilters(geoTestClass, filter)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("nested in an and", func(t *testing.T) {
		filter := &LocalFilter{Root: &Clause{
			Operator: OperatorAnd,
			Operands: []Clause{
				{
					Operator: OperatorEqual, On: &Path{Class: "Car", Property: "horsepower"},
					Value: &Value{Value: 3, Type: schema.DataTypeInt},
				},
				{
					Operator: OperatorWithinGeoBox, On: &Path{Class: "Car", Property: "parkedAt"},
					Value: &Value{Value: geoRange, Type: schema.DataTypeGeoCoordinates},
				},
			},
		}}
		require.ErrorContains(t, ValidateFilters(geoTestClass, filter), "needs operator WithinGeoRange")
	})
}

func TestValidateSortByDistance(t *testing.T) {
	getClass := func(name string) *models.Class {
		class, _ := geoTestClass(name)
		return class
	}
	origin := &SortGeoOrigin{Latitude: 52.5, Longitude: 13.4}
	nan, inf := float32(math.NaN()), float32(math.Inf(1))
	tests := []struct {
		name    string
		sort    Sort
		wantErr string
	}{
		{name: "on a geo property", sort: Sort{Path: []string{"parkedAt"}, Order: "asc", DistanceFrom: origin}},
		{name: "descending", sort: Sort{Path: []string{"parkedAt"}, Order: "desc", DistanceFrom: origin}},
		{name: "a geo property without an origin", sort: Sort{Path: []string{"parkedAt"}, Order: "asc"}},
		{
			name: "on a text property", sort: Sort{Path: []string{"modelName"}, Order: "asc", DistanceFrom: origin},
			wantErr: "needs a geoCoordinates property",
		},
		{
			name: "on an int property", sort: Sort{Path: []string{"horsepower"}, Order: "asc", DistanceFrom: origin},
			wantErr: "needs a geoCoordinates property",
		},
		{
			name: "on an internal property", sort: Sort{Path: []string{"_creationTimeUnix"}, Order: "asc", DistanceFrom: origin},
			wantErr: "needs a geoCoordinates property",
		},
		{
			name: "NaN latitude", sort: Sort{Path: []string{"parkedAt"}, DistanceFrom: &SortGeoOrigin{Latitude: nan, Longitude: 1}},
			wantErr: "must be finite numbers",
		},
		{
			name: "infinite longitude", sort: Sort{Path: []string{"parkedAt"}, DistanceFrom: &SortGeoOrigin{Latitude: 1, Longitude: inf}},
			wantErr: "must be finite numbers",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSort(getClass, "Car", []Sort{tt.sort})
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestValidateGeoDecay(t *testing.T) {
	valid := func() *Decay {
		return &Decay{
			Path: &Path{Property: "parkedAt"}, IsGeo: true,
			OriginGeo: GeoOrigin{Latitude: 52.5, Longitude: 13.4}, ScaleNumeric: 1000,
		}
	}
	tests := []struct {
		name    string
		mutate  func(d *Decay)
		wantErr string
	}{
		{name: "valid", mutate: func(d *Decay) {}},
		{name: "with an offset and a curve", mutate: func(d *Decay) { d.OffsetNumeric = 50; d.Curve = DecayCurveGauss; d.DecayValue = 0.3 }},
		{name: "no path", mutate: func(d *Decay) { d.Path = nil }, wantErr: "path is required"},
		{name: "zero scale", mutate: func(d *Decay) { d.ScaleNumeric = 0 }, wantErr: "scale must be a distance in meters > 0"},
		{name: "negative scale", mutate: func(d *Decay) { d.ScaleNumeric = -1 }, wantErr: "scale must be a distance"},
		{name: "NaN scale", mutate: func(d *Decay) { d.ScaleNumeric = math.NaN() }, wantErr: "scale must be a distance"},
		{name: "infinite scale", mutate: func(d *Decay) { d.ScaleNumeric = math.Inf(1) }, wantErr: "scale must be a distance"},
		{name: "negative offset", mutate: func(d *Decay) { d.OffsetNumeric = -1 }, wantErr: "offset must be a distance in meters >= 0"},
		{name: "NaN offset", mutate: func(d *Decay) { d.OffsetNumeric = math.NaN() }, wantErr: "offset must be a distance"},
		{name: "NaN origin", mutate: func(d *Decay) { d.OriginGeo.Latitude = float32(math.NaN()) }, wantErr: "origin coordinates must be finite"},
		{name: "infinite origin", mutate: func(d *Decay) { d.OriginGeo.Longitude = float32(math.Inf(-1)) }, wantErr: "origin coordinates must be finite"},
		{name: "geo and numeric", mutate: func(d *Decay) { d.IsNumeric = true }, wantErr: "cannot be both numeric and geo"},
		{name: "unknown curve", mutate: func(d *Decay) { d.Curve = "cubic" }, wantErr: "curve must be one of"},
		{name: "decay value above 1", mutate: func(d *Decay) { d.DecayValue = 1.5 }, wantErr: "decay_value must be between 0 and 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decay := valid()
			tt.mutate(decay)
			err := ValidateBoost(&Boost{Weight: 0.5, Conditions: []BoostCondition{{Decay: decay, Weight: 1}}})
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
