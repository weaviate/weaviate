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

package nearAudio

import "testing"

func Test_validateNearAudioFn(t *testing.T) {
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
				param: &NearAudioParams{
					Audio: "base64;enncoded",
				},
			},
		},
		{
			name: "should not pass with empty image",
			args: args{
				param: &NearAudioParams{
					Audio: "",
				},
			},
			wantErr: true,
		},
		{
			name: "should not pass with nil image",
			args: args{
				param: &NearAudioParams{},
			},
			wantErr: true,
		},
		{
			name: "should not pass with struct param, not a pointer to struct",
			args: args{
				param: NearAudioParams{
					Audio: "image",
				},
			},
			wantErr: true,
		},
		{
			name: "should not pass with certainty and distance",
			args: args{
				param: NearAudioParams{
					Audio:        "image",
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
			if err := ValidateNearAudioFn(tt.args.param); (err != nil) != tt.wantErr {
				t.Errorf("ValidateNearAudioFn() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
