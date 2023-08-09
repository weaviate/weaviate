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

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/clients/v2/grpc/codegen"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

func Test_findTensor(t *testing.T) {
	type args struct {
		inputs []*codegen.ModelMetadataResponse_TensorMetadata
		input  string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Doesn't contain the tensor",
			args: args{
				inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
					{
						Name:     "tensor0",
						Datatype: "BYTES",
						Shape:    []int64{1, 1},
					},
				},
				input: "tensor1",
			},
			want: false,
		},
		{
			name: "Contains the tensor",
			args: args{
				inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
					{
						Name:     "tensor0",
						Datatype: "BYTES",
						Shape:    []int64{1, 1},
					},
				},
				input: "tensor0",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, findTensor(tt.args.inputs, tt.args.input) != nil, tt.want)
		})
	}
}

func Test_validate(t *testing.T) {
	type args struct {
		metadata codegen.ModelMetadataResponse
		config   ent.ModuleConfig
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Empty metadata",
			args: args{
				metadata: codegen.ModelMetadataResponse{},
				config:   ent.ModuleConfig{},
			},
			wantErr: true,
		},
		{
			name: "No matching input tensor",
			args: args{
				metadata: codegen.ModelMetadataResponse{
					Inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "input0",
							Datatype: "BYTES",
							Shape:    []int64{-1, 1},
						},
					},
					Outputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "output0",
							Datatype: "FP32",
							Shape:    []int64{-1, 768},
						},
					},
				},
				config: ent.ModuleConfig{
					Input:         "input1",
					Output:        "output0",
					EmbeddingDims: 768,
				},
			},
			wantErr: true,
		},
		{
			name: "No matching output tensor",
			args: args{
				metadata: codegen.ModelMetadataResponse{
					Inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "input0",
							Datatype: "BYTES",
							Shape:    []int64{-1, 1},
						},
					},
					Outputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "output1",
							Datatype: "FP32",
							Shape:    []int64{-1, 768},
						},
					},
				},
				config: ent.ModuleConfig{
					Input:         "input0",
					Output:        "output0",
					EmbeddingDims: 768,
				},
			},
			wantErr: true,
		},
		{
			name: "Wrong embedding dimension",
			args: args{
				metadata: codegen.ModelMetadataResponse{
					Inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "input0",
							Datatype: "BYTES",
							Shape:    []int64{-1, 1},
						},
					},
					Outputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "output0",
							Datatype: "FP32",
							Shape:    []int64{-1, 512},
						},
					},
				},
				config: ent.ModuleConfig{
					Input:         "input0",
					Output:        "output0",
					EmbeddingDims: 768,
				},
			},
			wantErr: true,
		},
		{
			name: "Passes validation",
			args: args{
				metadata: codegen.ModelMetadataResponse{
					Inputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "input0",
							Datatype: "BYTES",
							Shape:    []int64{-1, 1},
						},
					},
					Outputs: []*codegen.ModelMetadataResponse_TensorMetadata{
						{
							Name:     "output0",
							Datatype: "FP32",
							Shape:    []int64{-1, 512},
						},
					},
				},
				config: ent.ModuleConfig{
					Input:         "input0",
					Output:        "output0",
					EmbeddingDims: 512,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(tt.args.metadata, tt.args.config)
			assert.Equal(t, tt.wantErr, err != nil, err)
		})
	}
}
