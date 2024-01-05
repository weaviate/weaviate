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

package nearImage

import (
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearImage"
)

func Test_extractNearImageFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "should extract properly with distance and image params set",
			args: args{
				source: map[string]interface{}{
					"image":    "base64;encoded",
					"distance": float64(0.9),
				},
			},
			want: &nearImage.NearImageParams{
				Image:        "base64;encoded",
				Distance:     0.9,
				WithDistance: true,
			},
		},
		{
			name: "should extract properly with certainty and image params set",
			args: args{
				source: map[string]interface{}{
					"image":     "base64;encoded",
					"certainty": float64(0.9),
				},
			},
			want: &nearImage.NearImageParams{
				Image:     "base64;encoded",
				Certainty: 0.9,
			},
		},
		{
			name: "should extract properly with only image set",
			args: args{
				source: map[string]interface{}{
					"image": "base64;encoded",
				},
			},
			want: &nearImage.NearImageParams{
				Image: "base64;encoded",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractNearImageFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractNearImageFn() = %v, want %v", got, tt.want)
			}
		})
	}
}
