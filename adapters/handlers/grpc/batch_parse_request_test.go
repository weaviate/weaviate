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

package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol"
)

const (
	UUID3 = "a4de3ca0-6975-464f-b23b-adddd83630d7"
	UUID4 = "7e10ec81-a26d-4ac7-8264-3e3e05397ddc"
)

func TestGRPCBatchRequest(t *testing.T) {
	classname := "TestClass"
	refClass1 := "OtherClass"
	refClass2 := "AnotherClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: classname,
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString()},
						{Name: "number", DataType: []string{"int"}},
						{Name: "ref", DataType: []string{refClass1}},
						{Name: "multiRef", DataType: []string{refClass1, refClass2}},
					},
				},
				{
					Class: refClass1,
					Properties: []*models.Property{
						{Name: "something", DataType: schema.DataTypeText.PropString()},
						{Name: "ref2", DataType: []string{refClass2}},
					},
				},
				{
					Class: refClass2,
					Properties: []*models.Property{
						{Name: "else", DataType: schema.DataTypeText.PropString()},
						{Name: "ref3", DataType: []string{refClass2}},
					},
				},
			},
		},
	}

	var nilMap map[string]interface{}
	tests := []struct {
		name  string
		req   []*pb.BatchObject
		out   []*models.Object
		error bool
	}{
		{
			name:  "empty object",
			req:   []*pb.BatchObject{{ClassName: classname}},
			out:   []*models.Object{{Class: classname, Properties: nilMap}},
			error: false,
		},
		{
			name: "only normal props",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				NonRefProperties: newStruct(t, map[string]interface{}{
					"name": "something",
					"age":  45,
				}),
			}}},
			out: []*models.Object{{Class: classname, Properties: map[string]interface{}{
				"name": "something",
				"age":  float64(45),
			}}},
			error: false,
		},
		{
			name: "only single refs",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				RefPropsSingle: []*pb.BatchObject_RefPropertiesSingleTarget{
					{PropName: "ref", Uuids: []string{UUID3, UUID4}},
				},
			}}},
			out: []*models.Object{{Class: classname, Properties: map[string]interface{}{
				"ref": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID3},
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID4},
				},
			}}},
			error: false,
		},
		{
			name: "only mult ref",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				RefPropsMulti: []*pb.BatchObject_RefPropertiesMultiTarget{
					{PropName: "multiRef", Uuids: []string{UUID3, UUID4}, TargetCollection: refClass2},
				},
			}}},
			out: []*models.Object{{Class: classname, Properties: map[string]interface{}{
				"multiRef": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID3},
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID4},
				},
			}}},
			error: false,
		},
		{
			name: "all property types",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				RefPropsMulti: []*pb.BatchObject_RefPropertiesMultiTarget{
					{PropName: "multiRef", Uuids: []string{UUID4, UUID3}, TargetCollection: refClass2},
				},
				RefPropsSingle: []*pb.BatchObject_RefPropertiesSingleTarget{
					{PropName: "ref", Uuids: []string{UUID4, UUID3}},
				},
				NonRefProperties: newStruct(t, map[string]interface{}{
					"name": "else",
					"age":  46,
				}),
			}}},
			out: []*models.Object{{Class: classname, Properties: map[string]interface{}{
				"multiRef": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID4},
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID3},
				},
				"ref": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID4},
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID3},
				},
				"name": "else",
				"age":  float64(46),
			}}},
			error: false,
		},
		{
			name: "mult ref to single target",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				RefPropsMulti: []*pb.BatchObject_RefPropertiesMultiTarget{
					{PropName: "ref", Uuids: []string{UUID3, UUID4}, TargetCollection: refClass2},
				},
			}}},
			out:   []*models.Object{},
			error: true,
		},
		{
			name: "single ref to multi target",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				RefPropsSingle: []*pb.BatchObject_RefPropertiesSingleTarget{
					{PropName: "multiRef", Uuids: []string{UUID3, UUID4}},
				},
			}}},
			out:   []*models.Object{},
			error: true,
		},
		{
			name: "slice props",
			req: []*pb.BatchObject{{ClassName: classname, Properties: &pb.BatchObject_Properties{
				NonRefProperties: newStruct(t, map[string]interface{}{"name": "something"}),
				BooleanArrayProperties: []*pb.BooleanArrayProperties{
					{PropName: "boolArray1", Values: []bool{true, true}},
					{PropName: "boolArray2", Values: []bool{false, true}},
				},
				IntArrayProperties: []*pb.IntArrayProperties{
					{PropName: "int1", Values: []int64{2, 3, 4}}, {PropName: "int2", Values: []int64{7, 8}},
				},
				NumberArrayProperties: []*pb.NumberArrayProperties{
					{PropName: "float1", Values: []float64{1, 2, 3}}, {PropName: "float2", Values: []float64{4, 5}},
				},
				TextArrayProperties: []*pb.TextArrayProperties{
					{PropName: "text1", Values: []string{"first", "second"}}, {PropName: "text2", Values: []string{"third"}},
				},
			}}},
			out: []*models.Object{{Class: classname, Properties: map[string]interface{}{
				"name":       "something",
				"boolArray1": []interface{}{true, true},
				"boolArray2": []interface{}{false, true},
				"int1":       []interface{}{int64(2), int64(3), int64(4)},
				"int2":       []interface{}{int64(7), int64(8)},
				"float1":     []interface{}{1., 2., 3.},
				"float2":     []interface{}{4., 5.},
				"text1":      []interface{}{"first", "second"},
				"text2":      []interface{}{"third"},
			}}},
			error: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := batchFromProto(&pb.BatchObjectsRequest{Objects: tt.req}, scheme)
			if tt.error {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.out, out)

			}
		})
	}
}
