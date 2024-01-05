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

package sempath

import "testing"

func TestParams_validate(t *testing.T) {
	type fields struct {
		SearchVector []float32
	}
	type args struct {
		inputSize int
		dims      int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Should validate",
			fields: fields{
				SearchVector: []float32{1.0},
			},
			args: args{
				inputSize: 25,
				dims:      0,
			},
			wantErr: false,
		},
		{
			name: "Should error with empty SearchVector",
			fields: fields{
				SearchVector: []float32{},
			},
			args: args{
				inputSize: 25,
				dims:      0,
			},
			wantErr: true,
		},
		{
			name:   "Should error with nil SearchVector",
			fields: fields{},
			args: args{
				inputSize: 25,
				dims:      0,
			},
			wantErr: true,
		},
		{
			name: "Should error with with inputSize greater then 25",
			fields: fields{
				SearchVector: []float32{1.0},
			},
			args: args{
				inputSize: 26,
				dims:      0,
			},
			wantErr: true,
		},
		{
			name: "Should error with with inputSize greater then 25 and nil SearchVector",
			fields: fields{
				SearchVector: nil,
			},
			args: args{
				inputSize: 26,
				dims:      0,
			},
			wantErr: true,
		},
		{
			name: "Should error with with inputSize greater then 25 and empty SearchVector",
			fields: fields{
				SearchVector: []float32{},
			},
			args: args{
				inputSize: 26,
				dims:      0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Params{
				SearchVector: tt.fields.SearchVector,
			}
			if err := p.validate(tt.args.inputSize, tt.args.dims); (err != nil) != tt.wantErr {
				t.Errorf("Params.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
