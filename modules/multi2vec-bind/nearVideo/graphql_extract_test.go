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

package nearVideo

import (
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearVideo"
)

func Test_extractNearVideoFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "should extract properly with distance and video params set",
			args: args{
				source: map[string]interface{}{
					"video":    "base64;encoded",
					"distance": float64(0.9),
				},
			},
			want: &nearVideo.NearVideoParams{
				Video:        "base64;encoded",
				Distance:     0.9,
				WithDistance: true,
			},
		},
		{
			name: "should extract properly with certainty and video params set",
			args: args{
				source: map[string]interface{}{
					"video":     "base64;encoded",
					"certainty": float64(0.9),
				},
			},
			want: &nearVideo.NearVideoParams{
				Video:     "base64;encoded",
				Certainty: 0.9,
			},
		},
		{
			name: "should extract properly with only video set",
			args: args{
				source: map[string]interface{}{
					"video": "base64;encoded",
				},
			},
			want: &nearVideo.NearVideoParams{
				Video: "base64;encoded",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractNearVideoFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractNearVideoFn() = %v, want %v", got, tt.want)
			}
		})
	}
}
