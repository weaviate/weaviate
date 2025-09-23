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

package rest

import (
	"testing"
)

func TestParseMemLimit_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{
			name:    "valid GiB",
			input:   "2GiB",
			want:    2 * 1024 * 1024 * 1024,
			wantErr: false,
		},
		{
			name:    "valid MiB",
			input:   "512MiB",
			want:    512 * 1024 * 1024,
			wantErr: false,
		},
		{
			name:    "valid bytes",
			input:   "1048576",
			want:    1048576,
			wantErr: false,
		},
		{
			name:    "invalid format",
			input:   "invalid",
			want:    0,
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMemLimit(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMemLimit() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseMemLimit() = %v, want %v", got, tt.want)
			}
		})
	}
}
