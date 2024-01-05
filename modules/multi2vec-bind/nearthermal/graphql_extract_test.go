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

package nearthermal

import (
	"reflect"
	"testing"
)

func Test_extractNearThermalFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "should extract properly with distance and thermal params set",
			args: args{
				source: map[string]interface{}{
					"thermal":  "base64;encoded",
					"distance": float64(0.9),
				},
			},
			want: &NearThermalParams{
				Thermal:      "base64;encoded",
				Distance:     0.9,
				WithDistance: true,
			},
		},
		{
			name: "should extract properly with certainty and thermal params set",
			args: args{
				source: map[string]interface{}{
					"thermal":   "base64;encoded",
					"certainty": float64(0.9),
				},
			},
			want: &NearThermalParams{
				Thermal:   "base64;encoded",
				Certainty: 0.9,
			},
		},
		{
			name: "should extract properly with only thermal set",
			args: args{
				source: map[string]interface{}{
					"thermal": "base64;encoded",
				},
			},
			want: &NearThermalParams{
				Thermal: "base64;encoded",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractNearThermalFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractNearThermalFn() = %v, want %v", got, tt.want)
			}
		})
	}
}
