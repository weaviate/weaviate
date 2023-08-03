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
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/weaviate/weaviate/modules/text2vec-kserve/grpc"
)

func Test_byteArrayToFloatArray(t *testing.T) {
	type args struct {
		arr         []byte
		floatArrLen int32
	}
	tests := []struct {
		name    string
		args    args
		want    *[]float32
		wantErr bool
	}{
		{
			name: "Empty byte array",
			args: args{
				arr:         []byte{},
				floatArrLen: 0,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "Misaligned byte array",
			args: args{
				arr:         []byte{0, 0},
				floatArrLen: 0,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Single float array",
			args: args{
				arr:         []byte{0, 0, 0, 0},
				floatArrLen: 1,
			},
			want:    &[]float32{0},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := byteArrayToFloatArray(tt.args.arr, tt.args.floatArrLen)
			hasError := err != nil
			assert.Equal(t, tt.wantErr, hasError, err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("byteArrayToFloatArray() = %v, want %v", got, tt.want)
			}
			if got != nil {
				minVal := min(*got)
				maxVal := max(*got)
				assert.GreaterOrEqual(t, float64(minVal), float64(0.0))
				assert.LessOrEqual(t, float64(maxVal), float64(1.0))
			}
		})
	}
}

func min(arr []float32) float32 {
	min := float32(math.MaxFloat32)
	for _, x := range arr {
		if x < min {
			min = x
		}
	}
	return min
}

func max(arr []float32) float32 {
	max := -1 * float32(math.MaxFloat32)
	for _, x := range arr {
		if x > max {
			max = x
		}
	}
	return max
}

func Test_stringToByteTensor(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "Empty string",
			args: args{
				s: "",
			},
			want: (&bytes.Buffer{}).Bytes(),
		},
		{
			name: "Sample string",
			args: args{
				s: "foo",
			},
			want: []byte{102, 111, 111},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stringToByteTensor(tt.args.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("string_to_byte_tensor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeInferRequest(t *testing.T) {
	type args struct {
		s          string
		model      string
		version    string
		inputName  string
		inputValue string
		outputName string
	}
	tests := []struct {
		name string
		args args
		want *pb.ModelInferRequest
	}{
		{
			name: "Empty",
			args: args{},
			want: &pb.ModelInferRequest{
				ModelName:    "",
				ModelVersion: "",
				Inputs: []*pb.ModelInferRequest_InferInputTensor{
					{
						Name:     "",
						Datatype: "BYTES",
						Shape:    []int64{1, 1},
						Contents: &pb.InferTensorContents{
							BytesContents: [][]byte{{}},
						},
					},
				},
				Outputs: []*pb.ModelInferRequest_InferRequestedOutputTensor{
					{
						Name: "",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeInferRequest(tt.args.s, tt.args.model, tt.args.version, tt.args.inputName, tt.args.inputValue, tt.args.outputName)
			assert.Equal(t, tt.args.model, got.ModelName)
			assert.Equal(t, tt.args.version, got.ModelVersion)
			assert.Equal(t, tt.args.inputName, got.Inputs[0].Name)
			assert.Equal(t, "BYTES", got.Inputs[0].Datatype)
			assert.Equal(t, []int64{1, 1}, got.Inputs[0].Shape)
			assert.Equal(t, tt.args.outputName, got.Outputs[0].Name)
		})
	}
}
