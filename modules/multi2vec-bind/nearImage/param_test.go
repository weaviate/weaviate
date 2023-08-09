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

package nearImage

import "testing"

func Test_validateNearImageFn(t *testing.T) {
	type args struct {
		param interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "should pass with proper values",
			args: args{
				param: &NearImageParams{
					Image: "base64;enncoded",
				},
			},
		},
		{
			name: "should not pass with empty image",
			args: args{
				param: &NearImageParams{
					Image: "",
				},
			},
			wantErr: true,
		},
		{
			name: "should not pass with nil image",
			args: args{
				param: &NearImageParams{},
			},
			wantErr: true,
		},
		{
			name: "should not pass with struct param, not a pointer to struct",
			args: args{
				param: NearImageParams{
					Image: "image",
				},
			},
			wantErr: true,
		},
		{
			name: "should not pass with certainty and distance",
			args: args{
				param: NearImageParams{
					Image:        "image",
					Distance:     0.9,
					WithDistance: true,
					Certainty:    0.1,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateNearImageFn(tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("validateNearImageFn() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
