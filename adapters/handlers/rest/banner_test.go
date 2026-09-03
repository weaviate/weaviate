//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBannerDisabled(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "unset", env: "", want: false},
		{name: "true", env: "true", want: true},
		{name: "1", env: "1", want: true},
		{name: "on", env: "on", want: true},
		{name: "false", env: "false", want: false},
		{name: "garbage", env: "nope", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DISABLE_STARTUP_BANNER", tt.env)
			assert.Equal(t, tt.want, bannerDisabled())
		})
	}
}
