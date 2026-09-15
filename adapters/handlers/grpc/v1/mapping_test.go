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

package v1

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type innerTest struct {
	datatype    schema.DataType
	out         *pb.Value
	shouldError bool
}

func makeTestList(succeedingInnerTests map[schema.DataType]*pb.Value) []innerTest {
	dtypes := append(schema.PrimitiveDataTypes, schema.DeprecatedPrimitiveDataTypes...)
	list := make([]innerTest, len(dtypes))
	for idx, dtype := range dtypes {
		out, ok := succeedingInnerTests[dtype]
		if ok {
			list[idx] = innerTest{
				datatype:    dtype,
				out:         out,
				shouldError: false,
			}
		} else {
			list[idx] = innerTest{
				datatype:    dtype,
				out:         nil,
				shouldError: true,
			}
		}
	}
	return list
}

func TestNewPrimitiveValue(t *testing.T) {
	float_val := float32(1.1)

	tests := []struct {
		name  string
		in    any
		tests []innerTest
	}{
		{
			name: "bools",
			in:   []bool{true, false},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeBooleanArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_BoolValues{BoolValues: &pb.BoolValues{Values: []bool{true, false}}},
				}}},
			}),
		},
		{
			name: "strings",
			in:   []string{"a string", "another string"},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeDateArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_DateValues{DateValues: &pb.DateValues{Values: []string{"a string", "another string"}}},
				}}},
				schema.DataTypeStringArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_TextValues{TextValues: &pb.TextValues{Values: []string{"a string", "another string"}}},
				}}},
				schema.DataTypeTextArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_TextValues{TextValues: &pb.TextValues{Values: []string{"a string", "another string"}}},
				}}},
				schema.DataTypeUUIDArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_UuidValues{UuidValues: &pb.UuidValues{Values: []string{"a string", "another string"}}},
				}}},
			}),
		},
		{
			name: "float64s",
			in:   []float64{1.1, 2.2, 3.3},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeNumberArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_NumberValues{NumberValues: &pb.NumberValues{Values: byteops.Fp64SliceToBytes([]float64{1.1, 2.2, 3.3})}},
				}}},
				schema.DataTypeIntArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_IntValues{IntValues: &pb.IntValues{Values: byteops.IntsToByteVector([]float64{1, 2, 3})}},
				}}},
			}),
		},
		{
			name: "empty array",
			in:   []interface{}{},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeBooleanArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_BoolValues{BoolValues: &pb.BoolValues{Values: []bool{}}},
				}}},
				schema.DataTypeDateArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_DateValues{DateValues: &pb.DateValues{Values: []string{}}},
				}}},
				schema.DataTypeNumberArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_NumberValues{NumberValues: &pb.NumberValues{Values: byteops.Fp64SliceToBytes([]float64{})}},
				}}},
				schema.DataTypeIntArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_IntValues{IntValues: &pb.IntValues{Values: byteops.IntsToByteVector([]float64{})}},
				}}},
				schema.DataTypeStringArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_TextValues{TextValues: &pb.TextValues{Values: []string{}}},
				}}},
				schema.DataTypeTextArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_TextValues{TextValues: &pb.TextValues{Values: []string{}}},
				}}},
				schema.DataTypeUUIDArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
					Kind: &pb.ListValue_UuidValues{UuidValues: &pb.UuidValues{Values: []string{}}},
				}}},
			}),
		},
		{
			name: "bool",
			in:   true,
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeBoolean: {Kind: &pb.Value_BoolValue{BoolValue: true}},
			}),
		},
		{
			name: "string",
			in:   "a string",
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeDate:     {Kind: &pb.Value_DateValue{DateValue: "a string"}},
				schema.DataTypeString:   {Kind: &pb.Value_TextValue{TextValue: "a string"}},
				schema.DataTypeText:     {Kind: &pb.Value_TextValue{TextValue: "a string"}},
				schema.DataTypeUUID:     {Kind: &pb.Value_UuidValue{UuidValue: "a string"}},
				schema.DataTypeBlob:     {Kind: &pb.Value_BlobValue{BlobValue: "a string"}},
				schema.DataTypeBlobHash: {Kind: &pb.Value_BlobValue{BlobValue: "a string"}},
			}),
		},
		{
			name: "float64",
			in:   1.1,
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeNumber: {Kind: &pb.Value_NumberValue{NumberValue: 1.1}},
				schema.DataTypeInt:    {Kind: &pb.Value_IntValue{IntValue: 1}},
			}),
		},
		{
			name: "geo",
			in:   &models.GeoCoordinates{Longitude: &float_val, Latitude: &float_val},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypeGeoCoordinates: {Kind: &pb.Value_GeoValue{GeoValue: &pb.GeoCoordinate{Latitude: float_val, Longitude: float_val}}},
			}),
		},
		{
			name: "phone number",
			in:   &models.PhoneNumber{Input: "1234567890"},
			tests: makeTestList(map[schema.DataType]*pb.Value{
				schema.DataTypePhoneNumber: {Kind: &pb.Value_PhoneValue{PhoneValue: &pb.PhoneNumber{Input: "1234567890"}}},
			}),
		},
	}

	for _, tt := range tests {
		for _, test := range tt.tests {
			m := NewMapping()
			out, err := m.NewPrimitiveValue(tt.in, test.datatype)
			if test.shouldError {
				if err == nil {
					t.Logf("expected an error for %v and %s", tt.in, test.datatype)
				}
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.out, out)
			}
		}
	}
}

func TestNewNestedValueGeoAndPhoneShapedObjects(t *testing.T) {
	float32p := func(f float32) *float32 { return &f }

	geoParent := &Property{Property: &models.Property{
		Name: "location",
		NestedProperties: []*models.NestedProperty{
			{Name: "latitude", DataType: []string{"number"}},
			{Name: "longitude", DataType: []string{"number"}},
		},
	}}
	geoSelect := search.SelectProperty{
		Name:     "location",
		IsObject: true,
		Props: []search.SelectProperty{
			{Name: "latitude", IsPrimitive: true},
			{Name: "longitude", IsPrimitive: true},
		},
	}

	phoneParent := &Property{Property: &models.Property{
		Name: "phone",
		NestedProperties: []*models.NestedProperty{
			{Name: "input", DataType: []string{"string"}},
			{Name: "internationalFormatted", DataType: []string{"string"}},
			{Name: "nationalFormatted", DataType: []string{"string"}},
			{Name: "national", DataType: []string{"number"}},
			{Name: "countryCode", DataType: []string{"number"}},
			{Name: "valid", DataType: []string{"boolean"}},
		},
	}}
	phoneSelect := search.SelectProperty{
		Name:     "phone",
		IsObject: true,
		Props: []search.SelectProperty{
			{Name: "input", IsPrimitive: true},
			{Name: "internationalFormatted", IsPrimitive: true},
			{Name: "nationalFormatted", IsPrimitive: true},
			{Name: "national", IsPrimitive: true},
			{Name: "countryCode", IsPrimitive: true},
			{Name: "valid", IsPrimitive: true},
		},
	}

	t.Run("geo-shaped object property stored as GeoCoordinates", func(t *testing.T) {
		m := NewMapping()
		in := &models.GeoCoordinates{Latitude: float32p(45.67), Longitude: float32p(-12.34)}
		out, err := m.NewNestedValue(in, schema.DataTypeObject, geoParent, geoSelect)
		require.NoError(t, err)
		require.NotNil(t, out.GetObjectValue())
		fields := out.GetObjectValue().GetFields()
		require.Equal(t, float64(*in.Latitude), fields["latitude"].GetNumberValue())
		require.Equal(t, float64(*in.Longitude), fields["longitude"].GetNumberValue())
	})

	t.Run("phone-shaped object property stored as PhoneNumber", func(t *testing.T) {
		m := NewMapping()
		in := &models.PhoneNumber{
			Input:                  "123456789",
			InternationalFormatted: "+48 12 345 67 89",
			NationalFormatted:      "12 345 67 89",
			National:               123456789,
			CountryCode:            48,
			Valid:                  true,
		}
		out, err := m.NewNestedValue(in, schema.DataTypeObject, phoneParent, phoneSelect)
		require.NoError(t, err)
		require.NotNil(t, out.GetObjectValue())
		fields := out.GetObjectValue().GetFields()
		require.Equal(t, in.Input, fields["input"].GetTextValue())
		require.Equal(t, in.InternationalFormatted, fields["internationalFormatted"].GetTextValue())
		require.Equal(t, in.NationalFormatted, fields["nationalFormatted"].GetTextValue())
		require.Equal(t, float64(in.National), fields["national"].GetNumberValue())
		require.Equal(t, float64(in.CountryCode), fields["countryCode"].GetNumberValue())
		require.Equal(t, in.Valid, fields["valid"].GetBoolValue())
	})
}
