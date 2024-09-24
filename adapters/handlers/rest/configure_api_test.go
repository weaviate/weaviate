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

//go:build linux
// +build linux

package rest

import (
	"testing"
)

func TestGetCores(t *testing.T) {
	tests := []struct {
		name     string
		cpuset   string
		expected int
		wantErr  bool
	}{
		{"Single core", "0", 1, false},
		{"Multiple cores", "0,1,2,3", 4, false},
		{"Range of cores", "0-3", 4, false},
		{"Multiple ranges", "0-3,5-7", 7, false},
		{"Mixed format", "0-2,4,6-7", 6, false},
		{"Mixed format 2", "0,2-4,7", 5, false},
		{"Empty cpuset", "", 0, false},
		{"Invalid format", "0-2-4", 0, true},
		{"Non-numeric", "a-b", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calcCPUs(tt.cpuset)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCores() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("getCores() = %v, want %v", got, tt.expected)
			}
		})
	}
}
