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

package projector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParams_validate(t *testing.T) {
	type args struct {
		inputSize int
		dims      int
	}
	tests := []struct {
		name        string
		param       *Params
		args        args
		wantErr     bool
		errContains []string
	}{
		{
			name:  "Should validate properly with default Params",
			param: generateParamWithDefaultValues(1, 3),
			args: args{
				inputSize: 1,
				dims:      3,
			},
			wantErr: false,
		},
		{
			name:  "Should validate properly with default Params with higher inputs",
			param: generateParamWithDefaultValues(100, 50),
			args: args{
				inputSize: 100,
				dims:      50,
			},
			wantErr: false,
		},
		{
			name:  "Should not validate - dimensions must be higher then 2",
			param: generateParamWithDefaultValues(100, 2),
			args: args{
				inputSize: 100,
				dims:      2,
			},
			wantErr: true,
		},
		{
			name:  "Should not validate - with dimensions equal to 0",
			param: generateParamWithValues(true, "tsne", 0, 5, 100, 25, true),
			args: args{
				inputSize: 100,
				dims:      2,
			},
			wantErr: true,
			errContains: []string{
				"dimensions must be at least 1, got: 0",
			},
		},
		{
			name:  "Should not validate - with all wrong values",
			param: generateParamWithValues(true, "unknown", 5, 5, 0, 0, true),
			args: args{
				inputSize: 4,
				dims:      2,
			},
			wantErr: true,
			errContains: []string{
				"algorithm unknown is not supported: must be one of: tsne",
				"perplexity must be smaller than amount of items: 5 >= 4",
				"iterations must be at least 1, got: 0",
				"learningRate must be at least 1, got: 0",
				"dimensions must be smaller than source dimensions: 5 >= 2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.param
			err := p.validate(tt.args.inputSize, tt.args.dims)
			if (err != nil) != tt.wantErr {
				t.Errorf("Params.validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && len(tt.errContains) > 0 {
				for _, containsString := range tt.errContains {
					assert.Contains(t, err.Error(), containsString)
				}
			}
		})
	}
}

func generateParamWithDefaultValues(inputSize, dims int) *Params {
	p := &Params{}
	p.setDefaults(inputSize, dims)
	return p
}

func generateParamWithValues(
	enabled bool,
	algorithm string,
	dims, perplexity, iterations, learningRate int,
	includeNeighbors bool,
) *Params {
	p := &Params{
		Enabled:          enabled,
		Algorithm:        &algorithm,
		Dimensions:       &dims,
		Perplexity:       &perplexity,
		Iterations:       &iterations,
		LearningRate:     &learningRate,
		IncludeNeighbors: includeNeighbors,
	}
	return p
}
