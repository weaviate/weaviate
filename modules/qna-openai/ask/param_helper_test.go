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

package ask

import (
	"reflect"
	"testing"
)

func TestParamsHelper_GetQuestion(t *testing.T) {
	type args struct {
		params interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should get question with certainty",
			args: args{
				params: &AskParams{
					Question:  "question",
					Certainty: 0.8,
				},
			},
			want: "question",
		},
		{
			name: "should get question with distance",
			args: args{
				params: &AskParams{
					Question: "question",
					Distance: 0.8,
				},
			},
			want: "question",
		},
		{
			name: "should get empty string when empty params",
			args: args{
				params: &AskParams{},
			},
			want: "",
		},
		{
			name: "should get empty string when nil params",
			args: args{
				params: nil,
			},
			want: "",
		},
		{
			name: "should get empty string when passed a struct, not a pointer to struct",
			args: args{
				params: AskParams{},
			},
			want: "",
		},
		{
			name: "should get empty string when passed a struct with question, not a pointer to struct",
			args: args{
				params: AskParams{
					Question: "question?",
				},
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParamsHelper{}
			if got := p.GetQuestion(tt.args.params); got != tt.want {
				t.Errorf("ParamsHelper.GetQuestion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParamsHelper_GetProperties(t *testing.T) {
	type args struct {
		params interface{}
	}
	tests := []struct {
		name string
		p    *ParamsHelper
		args args
		want []string
	}{
		{
			name: "should get properties with distance",
			args: args{
				params: &AskParams{
					Question:   "question",
					Properties: []string{"prop1", "prop2"},
					Distance:   0.8,
				},
			},
			want: []string{"prop1", "prop2"},
		},
		{
			name: "should get properties with certainty",
			args: args{
				params: &AskParams{
					Question:   "question",
					Properties: []string{"prop1", "prop2"},
					Certainty:  0.8,
				},
			},
			want: []string{"prop1", "prop2"},
		},
		{
			name: "should get nil properties with empty pointer to AskParams",
			args: args{
				params: &AskParams{},
			},
			want: nil,
		},
		{
			name: "should get nil properties with empty AskParams",
			args: args{
				params: AskParams{},
			},
			want: nil,
		},
		{
			name: "should get nil properties with nil params",
			args: args{
				params: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ParamsHelper{}
			if got := p.GetProperties(tt.args.params); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParamsHelper.GetProperties() = %v, want %v", got, tt.want)
			}
		})
	}
}
