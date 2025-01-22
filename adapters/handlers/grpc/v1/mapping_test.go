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

package v1

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
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
		tests map[bool][]innerTest
	}{
		{
			name: "bools",
			in:   []bool{true, false},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeBooleanArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
						Values: []*pb.Value{
							{Kind: &pb.Value_BoolValue{BoolValue: true}},
							{Kind: &pb.Value_BoolValue{BoolValue: false}},
						},
					}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeBooleanArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
						Kind: &pb.ListValue_BoolValues{BoolValues: &pb.BoolValues{Values: []bool{true, false}}},
					}}},
				}),
			},
		},
		{
			name: "strings",
			in:   []string{"a string", "another string"},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeDateArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_DateValue{DateValue: "a string"}},
						{Kind: &pb.Value_DateValue{DateValue: "another string"}},
					}}}},
					schema.DataTypeStringArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_StringValue{StringValue: "a string"}},
						{Kind: &pb.Value_StringValue{StringValue: "another string"}},
					}}}},
					schema.DataTypeTextArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_StringValue{StringValue: "a string"}},
						{Kind: &pb.Value_StringValue{StringValue: "another string"}},
					}}}},
					schema.DataTypeUUIDArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_UuidValue{UuidValue: "a string"}},
						{Kind: &pb.Value_UuidValue{UuidValue: "another string"}},
					}}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
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
		},
		{
			name: "float64s",
			in:   []float64{1.1, 2.2, 3.3},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeNumberArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_NumberValue{NumberValue: 1.1}},
						{Kind: &pb.Value_NumberValue{NumberValue: 2.2}},
						{Kind: &pb.Value_NumberValue{NumberValue: 3.3}},
					}}}},
					schema.DataTypeIntArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_IntValue{IntValue: 1}},
						{Kind: &pb.Value_IntValue{IntValue: 2}},
						{Kind: &pb.Value_IntValue{IntValue: 3}},
					}}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeNumberArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
						Kind: &pb.ListValue_NumberValues{NumberValues: &pb.NumberValues{Values: byteops.Fp64SliceToBytes([]float64{1.1, 2.2, 3.3})}},
					}}},
					schema.DataTypeIntArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
						Kind: &pb.ListValue_IntValues{IntValues: &pb.IntValues{Values: byteops.IntsToByteVector([]float64{1, 2, 3})}},
					}}},
				}),
			},
		},
		{
			name: "empty array",
			in:   []interface{}{},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeBooleanArray: {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeDateArray:    {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeNumberArray:  {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeIntArray:     {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeStringArray:  {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeTextArray:    {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
					schema.DataTypeUUIDArray:    {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{}}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
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
		},
		{
			name: "bool",
			in:   true,
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeBoolean: {Kind: &pb.Value_BoolValue{BoolValue: true}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeBoolean: {Kind: &pb.Value_BoolValue{BoolValue: true}},
				}),
			},
		},
		{
			name: "string",
			in:   "a string",
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeDate:   {Kind: &pb.Value_DateValue{DateValue: "a string"}},
					schema.DataTypeString: {Kind: &pb.Value_StringValue{StringValue: "a string"}},
					schema.DataTypeText:   {Kind: &pb.Value_StringValue{StringValue: "a string"}},
					schema.DataTypeUUID:   {Kind: &pb.Value_UuidValue{UuidValue: "a string"}},
					schema.DataTypeBlob:   {Kind: &pb.Value_BlobValue{BlobValue: "a string"}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeDate:   {Kind: &pb.Value_DateValue{DateValue: "a string"}},
					schema.DataTypeString: {Kind: &pb.Value_TextValue{TextValue: "a string"}},
					schema.DataTypeText:   {Kind: &pb.Value_TextValue{TextValue: "a string"}},
					schema.DataTypeUUID:   {Kind: &pb.Value_UuidValue{UuidValue: "a string"}},
					schema.DataTypeBlob:   {Kind: &pb.Value_BlobValue{BlobValue: "a string"}},
				}),
			},
		},
		{
			name: "float64",
			in:   1.1,
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeNumber: {Kind: &pb.Value_NumberValue{NumberValue: 1.1}},
					schema.DataTypeInt:    {Kind: &pb.Value_IntValue{IntValue: 1}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeNumber: {Kind: &pb.Value_NumberValue{NumberValue: 1.1}},
					schema.DataTypeInt:    {Kind: &pb.Value_IntValue{IntValue: 1}},
				}),
			},
		},
		{
			name: "geo",
			in:   &models.GeoCoordinates{Longitude: &float_val, Latitude: &float_val},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeGeoCoordinates: {Kind: &pb.Value_GeoValue{GeoValue: &pb.GeoCoordinate{Latitude: float_val, Longitude: float_val}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypeGeoCoordinates: {Kind: &pb.Value_GeoValue{GeoValue: &pb.GeoCoordinate{Latitude: float_val, Longitude: float_val}}},
				}),
			},
		},
		{
			name: "phone number",
			in:   &models.PhoneNumber{Input: "1234567890"},
			tests: map[bool][]innerTest{
				false: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypePhoneNumber: {Kind: &pb.Value_PhoneValue{PhoneValue: &pb.PhoneNumber{Input: "1234567890"}}},
				}),
				true: makeTestList(map[schema.DataType]*pb.Value{
					schema.DataTypePhoneNumber: {Kind: &pb.Value_PhoneValue{PhoneValue: &pb.PhoneNumber{Input: "1234567890"}}},
				}),
			},
		},
	}

	for _, tt := range tests {
		for uses125, innerTests := range tt.tests {
			for _, test := range innerTests {
				m := NewMapping(uses125)
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
}
