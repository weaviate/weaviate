//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package nearImage

import (
	"reflect"
	"testing"
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
			name: "should extract properly with all params set",
			args: args{
				source: map[string]interface{}{
					"image":     "base64;encoded",
					"certainty": float64(0.9),
				},
			},
			want: &NearImageParams{
				Image:     "base64;encoded",
				Certainty: 0.9,
			},
		},
		{
			name: "should extract properly with image set",
			args: args{
				source: map[string]interface{}{
					"image": "base64;encoded",
				},
			},
			want: &NearImageParams{
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
