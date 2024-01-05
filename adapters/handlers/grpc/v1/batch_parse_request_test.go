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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

const (
	UUID3 = "a4de3ca0-6975-464f-b23b-adddd83630d7"
	UUID4 = "7e10ec81-a26d-4ac7-8264-3e3e05397ddc"
)

func TestGRPCBatchRequest(t *testing.T) {
	collection := "TestClass"
	refClass1 := "OtherClass"
	refClass2 := "AnotherClass"
	scheme := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: collection,
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
		name      string
		req       []*pb.BatchObject
		out       []*models.Object
		outError  []int
		origIndex map[int]int
	}{
		{
			name: "empty object",
			req:  []*pb.BatchObject{{Collection: collection, Uuid: UUID4}},
			out:  []*models.Object{{Class: collection, Properties: nilMap, ID: UUID4}},
		},
		{
			name:     "no UUID",
			req:      []*pb.BatchObject{{Collection: collection}},
			out:      []*models.Object{},
			outError: []int{0},
		},
		{
			name: "only normal props",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				NonRefProperties: newStruct(t, map[string]interface{}{
					"name": "something",
					"age":  45,
				}),
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
				"name": "something",
				"age":  float64(45),
			}}},
		},
		{
			name: "only single refs",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				SingleTargetRefProps: []*pb.BatchObject_SingleTargetRefProps{
					{PropName: "ref", Uuids: []string{UUID3, UUID4}},
				},
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
				"ref": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID3},
					map[string]interface{}{"beacon": BEACON_START + refClass1 + "/" + UUID4},
				},
			}}},
		},
		{
			name: "only mult ref",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				MultiTargetRefProps: []*pb.BatchObject_MultiTargetRefProps{
					{PropName: "multiRef", Uuids: []string{UUID3, UUID4}, TargetCollection: refClass2},
				},
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
				"multiRef": []interface{}{
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID3},
					map[string]interface{}{"beacon": BEACON_START + refClass2 + "/" + UUID4},
				},
			}}},
		},
		{
			name: "all property types",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				MultiTargetRefProps: []*pb.BatchObject_MultiTargetRefProps{
					{PropName: "multiRef", Uuids: []string{UUID4, UUID3}, TargetCollection: refClass2},
				},
				SingleTargetRefProps: []*pb.BatchObject_SingleTargetRefProps{
					{PropName: "ref", Uuids: []string{UUID4, UUID3}},
				},
				NonRefProperties: newStruct(t, map[string]interface{}{
					"name": "else",
					"age":  46,
				}),
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
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
		},
		{
			name: "mult ref to single target",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				MultiTargetRefProps: []*pb.BatchObject_MultiTargetRefProps{
					{PropName: "ref", Uuids: []string{UUID3, UUID4}, TargetCollection: refClass2},
				},
			}}},
			out:      []*models.Object{},
			outError: []int{0},
		},
		{
			name: "single ref to multi target",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				SingleTargetRefProps: []*pb.BatchObject_SingleTargetRefProps{
					{PropName: "multiRef", Uuids: []string{UUID3, UUID4}},
				},
			}}},
			out:      []*models.Object{},
			outError: []int{0},
		},
		{
			name: "slice props",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
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
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
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
		},
		{
			name: "object props",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				ObjectProperties: []*pb.ObjectProperties{
					{
						PropName: "simpleObj", Value: &pb.ObjectPropertiesValue{
							NonRefProperties: newStruct(t, map[string]interface{}{"name": "something"}),
						},
					},
					{
						PropName: "nestedObj", Value: &pb.ObjectPropertiesValue{
							ObjectProperties: []*pb.ObjectProperties{{
								PropName: "obj", Value: &pb.ObjectPropertiesValue{
									NonRefProperties: newStruct(t, map[string]interface{}{"name": "something"}),
								},
							}},
						},
					},
				},
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
				"simpleObj": map[string]interface{}{"name": "something"},
				"nestedObj": map[string]interface{}{
					"obj": map[string]interface{}{"name": "something"},
				},
			}}},
		},
		{
			name: "object array props",
			req: []*pb.BatchObject{{Collection: collection, Uuid: UUID4, Properties: &pb.BatchObject_Properties{
				ObjectArrayProperties: []*pb.ObjectArrayProperties{
					{
						PropName: "simpleObjs", Values: []*pb.ObjectPropertiesValue{
							{
								NonRefProperties: newStruct(t, map[string]interface{}{"name": "something"}),
							},
							{
								NonRefProperties: newStruct(t, map[string]interface{}{"name": "something else"}),
							},
						},
					},
					{
						PropName: "nestedObjs", Values: []*pb.ObjectPropertiesValue{
							{
								ObjectProperties: []*pb.ObjectProperties{{
									PropName: "obj", Value: &pb.ObjectPropertiesValue{
										NonRefProperties: newStruct(t, map[string]interface{}{"name": "something"}),
									},
								}},
							},
							{
								ObjectProperties: []*pb.ObjectProperties{{
									PropName: "obj", Value: &pb.ObjectPropertiesValue{
										NonRefProperties: newStruct(t, map[string]interface{}{"name": "something else"}),
									},
								}},
							},
						},
					},
				},
			}}},
			out: []*models.Object{{Class: collection, ID: UUID4, Properties: map[string]interface{}{
				"simpleObjs": []interface{}{map[string]interface{}{"name": "something"}, map[string]interface{}{"name": "something else"}},
				"nestedObjs": []interface{}{
					map[string]interface{}{"obj": map[string]interface{}{"name": "something"}},
					map[string]interface{}{"obj": map[string]interface{}{"name": "something else"}},
				},
			}}},
		},
		{
			name:      "mix of errors and no errors",
			req:       []*pb.BatchObject{{Collection: collection, Uuid: UUID4}, {Collection: collection}, {Collection: collection}, {Collection: collection, Uuid: UUID3}},
			out:       []*models.Object{{Class: collection, Properties: nilMap, ID: UUID4}, {Class: collection, Properties: nilMap, ID: UUID3}},
			outError:  []int{1, 2},
			origIndex: map[int]int{0: 0, 1: 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, origIndex, batchErrors := batchFromProto(&pb.BatchObjectsRequest{Objects: tt.req}, scheme)
			if len(tt.outError) > 0 {
				require.NotNil(t, batchErrors)
				if len(tt.out) > 0 {
					require.Equal(t, tt.out, out)
					require.Equal(t, tt.origIndex, origIndex)
				}
			} else {
				require.Len(t, batchErrors, 0)
				require.Equal(t, tt.out, out)
			}
		})
	}
}
