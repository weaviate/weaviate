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

package schema

import (
	"testing"
)

func TestIsArrayDataType(t *testing.T) {
	type args struct {
		dt []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "is string array",
			args: args{
				dt: DataTypeTextArray.PropString(),
			},
			want: true,
		},
		{
			name: "is not string array",
			args: args{
				dt: DataTypeText.PropString(),
			},
			want: false,
		},
		{
			name: "is text array",
			args: args{
				dt: []string{"text[]"},
			},
			want: true,
		},
		{
			name: "is not text array",
			args: args{
				dt: []string{"text"},
			},
			want: false,
		},
		{
			name: "is number array",
			args: args{
				dt: []string{"number[]"},
			},
			want: true,
		},
		{
			name: "is not number array",
			args: args{
				dt: []string{"number"},
			},
			want: false,
		},
		{
			name: "is int array",
			args: args{
				dt: []string{"int[]"},
			},
			want: true,
		},
		{
			name: "is not int array",
			args: args{
				dt: []string{"int"},
			},
			want: false,
		},
		{
			name: "is not uuid array",
			args: args{
				dt: []string{"uuid"},
			},
			want: false,
		},
		{
			name: "is uuid array",
			args: args{
				dt: []string{"uuid[]"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArrayDataType(tt.args.dt); got != tt.want {
				t.Errorf("IsArrayDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}
