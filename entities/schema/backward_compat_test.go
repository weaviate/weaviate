//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import "testing"

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
				dt: []string{"string[]"},
			},
			want: true,
		},
		{
			name: "is not string array",
			args: args{
				dt: []string{"string"},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArrayDataType(tt.args.dt); got != tt.want {
				t.Errorf("IsArrayDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}
