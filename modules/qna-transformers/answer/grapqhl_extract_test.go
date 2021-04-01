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

package answer

import (
	"reflect"
	"testing"
)

func Test_extractAnswerFn(t *testing.T) {
	type args struct {
		source map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "should parse properly with only question",
			args: args{
				source: map[string]interface{}{
					"question": "some question",
				},
			},
			want: &AnswerParams{
				Question: "some question",
				Limit:    1,
			},
		},
		{
			name: "should parse properly with question and certainty",
			args: args{
				source: map[string]interface{}{
					"question":  "some question",
					"certainty": 0.8,
				},
			},
			want: &AnswerParams{
				Question:  "some question",
				Certainty: 0.8,
				Limit:     1,
			},
		},
		{
			name: "should parse properly without params",
			args: args{
				source: map[string]interface{}{},
			},
			want: &AnswerParams{
				Limit: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractAnswerFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("extractAnswerFn() = %v, want %v", got, tt.want)
			}
		})
	}
}
