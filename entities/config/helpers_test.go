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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnabled(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{
			name: "true", value: "true", want: true,
		},
		{
			name: "True", value: "True", want: true,
		},
		{
			name: "TRUE", value: "TRUE", want: true,
		},
		{
			name: "enabled", value: "enabled", want: true,
		},
		{
			name: "Enabled", value: "Enabled", want: true,
		},
		{
			name: "ENABLED", value: "ENABLED", want: true,
		},
		{
			name: "on", value: "on", want: true,
		},
		{
			name: "On", value: "On", want: true,
		},
		{
			name: "ON", value: "ON", want: true,
		},
		{
			name: "1", value: "1", want: true,
		},
		{
			name: "empty", value: "", want: false,
		},
		{
			name: "other", value: "other", want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Enabled(tt.value))
		})
	}
}
