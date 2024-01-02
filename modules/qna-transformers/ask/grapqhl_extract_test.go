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

func Test_extractAskFn(t *testing.T) {
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
			want: &AskParams{
				Question: "some question",
			},
		},
		{
			name: "should parse properly with question and distance",
			args: args{
				source: map[string]interface{}{
					"question": "some question",
					"distance": 0.8,
				},
			},
			want: &AskParams{
				Question:     "some question",
				Distance:     0.8,
				WithDistance: true,
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
			want: &AskParams{
				Question:  "some question",
				Certainty: 0.8,
			},
		},
		{
			name: "should parse properly without params",
			args: args{
				source: map[string]interface{}{},
			},
			want: &AskParams{},
		},
		{
			name: "should parse properly with question, distance, and properties",
			args: args{
				source: map[string]interface{}{
					"question":   "some question",
					"distance":   0.8,
					"properties": []interface{}{"prop1", "prop2"},
				},
			},
			want: &AskParams{
				Question:     "some question",
				Distance:     0.8,
				WithDistance: true,
				Properties:   []string{"prop1", "prop2"},
			},
		},
		{
			name: "should parse properly with question and certainty and properties",
			args: args{
				source: map[string]interface{}{
					"question":   "some question",
					"certainty":  0.8,
					"properties": []interface{}{"prop1", "prop2"},
				},
			},
			want: &AskParams{
				Question:   "some question",
				Certainty:  0.8,
				Properties: []string{"prop1", "prop2"},
			},
		},
		{
			name: "should parse properly with question, distance, properties, and rerank",
			args: args{
				source: map[string]interface{}{
					"question":   "some question",
					"distance":   0.8,
					"properties": []interface{}{"prop1", "prop2"},
					"rerank":     true,
				},
			},
			want: &AskParams{
				Question:     "some question",
				Distance:     0.8,
				WithDistance: true,
				Properties:   []string{"prop1", "prop2"},
				Rerank:       true,
			},
		},
		{
			name: "should parse properly with question and certainty and properties and rerank",
			args: args{
				source: map[string]interface{}{
					"question":   "some question",
					"certainty":  0.8,
					"properties": []interface{}{"prop1", "prop2"},
					"rerank":     true,
				},
			},
			want: &AskParams{
				Question:   "some question",
				Certainty:  0.8,
				Properties: []string{"prop1", "prop2"},
				Rerank:     true,
			},
		},
	}

	testsWithAutocorrect := []struct {
		name string
		args args
		want interface{}
	}{
		{
			name: "should parse properly with only question and autocorrect",
			args: args{
				source: map[string]interface{}{
					"question":    "some question",
					"autocorrect": true,
				},
			},
			want: &AskParams{
				Question:    "some question",
				Autocorrect: true,
			},
		},
		{
			name: "should parse properly and transform text in question",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"autocorrect": true,
				},
			},
			want: &AskParams{
				Question:    "transformed text",
				Autocorrect: true,
			},
		},
		{
			name: "should parse properly and not transform text in question",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"autocorrect": false,
				},
			},
			want: &AskParams{
				Question:    "transform this",
				Autocorrect: false,
			},
		},
		{
			name: "should parse properly with question, distance, properties, and autocorrect",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"distance":    0.8,
					"properties":  []interface{}{"prop1", "prop2"},
					"autocorrect": true,
				},
			},
			want: &AskParams{
				Question:     "transformed text",
				Distance:     0.8,
				WithDistance: true,
				Properties:   []string{"prop1", "prop2"},
				Autocorrect:  true,
			},
		},
		{
			name: "should parse properly with question and certainty and properties and autocorrect",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"certainty":   0.8,
					"properties":  []interface{}{"prop1", "prop2"},
					"autocorrect": true,
				},
			},
			want: &AskParams{
				Question:    "transformed text",
				Certainty:   0.8,
				Properties:  []string{"prop1", "prop2"},
				Autocorrect: true,
			},
		},
		{
			name: "should parse properly with question, distance, properties, autocorrect, and rerank",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"distance":    0.8,
					"properties":  []interface{}{"prop1", "prop2"},
					"autocorrect": true,
					"rerank":      true,
				},
			},
			want: &AskParams{
				Question:     "transformed text",
				Distance:     0.8,
				WithDistance: true,
				Properties:   []string{"prop1", "prop2"},
				Autocorrect:  true,
				Rerank:       true,
			},
		},
		{
			name: "should parse properly with question and certainty and properties and autocorrect and rerank",
			args: args{
				source: map[string]interface{}{
					"question":    "transform this",
					"certainty":   0.8,
					"properties":  []interface{}{"prop1", "prop2"},
					"autocorrect": true,
					"rerank":      true,
				},
			},
			want: &AskParams{
				Question:    "transformed text",
				Certainty:   0.8,
				Properties:  []string{"prop1", "prop2"},
				Autocorrect: true,
				Rerank:      true,
			},
		},
	}

	testsWithAutocorrect = append(testsWithAutocorrect, tests...)

	t.Run("should extract without text transformer", func(t *testing.T) {
		provider := New(nil)
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := provider.extractAskFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("extractAskFn() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	t.Run("should extract with text transformer", func(t *testing.T) {
		provider := New(&fakeTransformer{})
		for _, tt := range testsWithAutocorrect {
			t.Run(tt.name, func(t *testing.T) {
				if got := provider.extractAskFn(tt.args.source); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("extractAskFn() = %v, want %v", got, tt.want)
				}
			})
		}
	})
}
