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

package vectorizer

import (
	"testing"
)

func Test_joinSentences(t *testing.T) {
	type args struct {
		input []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "No sentences to join",
			args: args{
				input: []string{},
			},
			want: "",
		},
		{
			name: "Single sentence",
			args: args{
				input: []string{"Hello, world."},
			},
			want: "Hello, world.",
		},
		{
			name: "Two sentences",
			args: args{
				input: []string{"Hello,", "world"},
			},
			want: "Hello, world",
		},
		{
			name: "Two sentences with punctuation",
			args: args{
				input: []string{"Hello.", "world."},
			},
			want: "Hello. world.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinSentences(tt.args.input); got != tt.want {
				t.Errorf("joinSentences() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_endsWithPunctuation(t *testing.T) {
	type args struct {
		sent string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Empty string",
			args: args{
				sent: "",
			},
			want: true,
		},
		{
			name: "No punctuation",
			args: args{
				sent: "Hello",
			},
			want: false,
		},
		{
			name: "Ends with '.'",
			args: args{
				sent: "Hello, world.",
			},
			want: true,
		},
		{
			name: "Ends with ','",
			args: args{
				sent: "Hello,",
			},
			want: true,
		},
		{
			name: "Ends with '?'",
			args: args{
				sent: "Hello?",
			},
			want: true,
		},
		{
			name: "Ends with '!'",
			args: args{
				sent: "Hello!",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := endsWithPunctuation(tt.args.sent); got != tt.want {
				t.Errorf("endsWithPunctuation() = %v, want %v", got, tt.want)
			}
		})
	}
}
