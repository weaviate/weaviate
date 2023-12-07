//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestParseArray(t *testing.T) {
	tests := []struct {
		name  string
		in    any
		tests []struct {
			datatype    schema.DataType
			out         *pb.Value
			shouldError bool
		}
	}{
		{
			name: "bool",
			in:   []bool{true, false},
			tests: []struct {
				datatype    schema.DataType
				out         *pb.Value
				shouldError bool
			}{
				{
					datatype: schema.DataTypeBoolean,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_BoolValue{BoolValue: true}},
						{Kind: &pb.Value_BoolValue{BoolValue: false}},
					}}}},
					shouldError: false,
				},
				{
					datatype:    schema.DataTypeDate,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeInt,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeNumber,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeString,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeText,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeUUID,
					out:         nil,
					shouldError: true,
				},
			},
		},
		{
			name: "float64",
			in:   []float64{1.1, 2.2, 3.3},
			tests: []struct {
				datatype    schema.DataType
				out         *pb.Value
				shouldError bool
			}{
				{
					datatype:    schema.DataTypeBoolean,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeDate,
					out:         nil,
					shouldError: true,
				},
				{
					datatype: schema.DataTypeInt,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_IntValue{IntValue: 1}},
						{Kind: &pb.Value_IntValue{IntValue: 2}},
						{Kind: &pb.Value_IntValue{IntValue: 3}},
					}}}},
					shouldError: false,
				},
				{
					datatype: schema.DataTypeNumber,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_NumberValue{NumberValue: 1.1}},
						{Kind: &pb.Value_NumberValue{NumberValue: 2.2}},
						{Kind: &pb.Value_NumberValue{NumberValue: 3.3}},
					}}}},
					shouldError: false,
				},
				{
					datatype:    schema.DataTypeString,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeText,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeUUID,
					out:         nil,
					shouldError: true,
				},
			},
		},
		{
			name: "string",
			in:   []string{"a string", "another string"},
			tests: []struct {
				datatype    schema.DataType
				out         *pb.Value
				shouldError bool
			}{
				{
					datatype:    schema.DataTypeBoolean,
					out:         nil,
					shouldError: true,
				},
				{
					datatype: schema.DataTypeDate,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_DateValue{DateValue: "a string"}},
						{Kind: &pb.Value_DateValue{DateValue: "another string"}},
					}}}},
					shouldError: false,
				},
				{
					datatype:    schema.DataTypeInt,
					out:         nil,
					shouldError: true,
				},
				{
					datatype:    schema.DataTypeNumber,
					out:         nil,
					shouldError: true,
				},
				{
					datatype: schema.DataTypeString,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_StringValue{StringValue: "a string"}},
						{Kind: &pb.Value_StringValue{StringValue: "another string"}},
					}}}},
					shouldError: false,
				},
				{
					datatype: schema.DataTypeText,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_StringValue{StringValue: "a string"}},
						{Kind: &pb.Value_StringValue{StringValue: "another string"}},
					}}}},
					shouldError: false,
				},
				{
					datatype: schema.DataTypeUUID,
					out: &pb.Value{Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{Values: []*pb.Value{
						{Kind: &pb.Value_UuidValue{UuidValue: "a string"}},
						{Kind: &pb.Value_UuidValue{UuidValue: "another string"}},
					}}}},
					shouldError: false,
				},
			},
		},
	}

	for _, tt := range tests {
		for _, test := range tt.tests {
			if tt.name == "bool" {
				testValue(t, tt.in.([]bool), test.out, test.datatype, test.shouldError)
			}
			if tt.name == "float64" {
				testValue(t, tt.in.([]float64), test.out, test.datatype, test.shouldError)
			}
			if tt.name == "string" {
				testValue(t, tt.in.([]string), test.out, test.datatype, test.shouldError)
			}
		}
	}
}

func testValue[T bool | float64 | string](t *testing.T, in []T, expected *pb.Value, dt schema.DataType, shouldError bool) {
	out, err := parseArray[T](in, dt)
	if shouldError {
		if err == nil {
			t.Logf("expected an error for %v and %s", in, dt)
		}
		require.Error(t, err)
	} else {
		require.NoError(t, err)
		require.Equal(t, expected, out)
	}
}
