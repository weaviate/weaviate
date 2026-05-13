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

package rbacconf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRoot(t *testing.T) {
	populated := Config{
		RootUsers:  []string{"alice", "customer1:bob"},
		RootGroups: []string{"WeaviateOps", "PlatformAdmins"},
	}

	tests := []struct {
		name     string
		cfg      Config
		username string
		groups   []string
		want     bool
	}{
		{
			name:     "username matches RootUsers",
			cfg:      populated,
			username: "alice",
			want:     true,
		},
		{
			name:     "qualified username matches RootUsers",
			cfg:      populated,
			username: "customer1:bob",
			want:     true,
		},
		{
			name:     "group matches RootGroups",
			cfg:      populated,
			username: "carol",
			groups:   []string{"WeaviateOps"},
			want:     true,
		},
		{
			name:     "any group in RootGroups is enough",
			cfg:      populated,
			username: "carol",
			groups:   []string{"engineers", "PlatformAdmins"},
			want:     true,
		},
		{
			name:     "neither user nor group matches",
			cfg:      populated,
			username: "carol",
			groups:   []string{"engineers"},
			want:     false,
		},
		{
			name:     "empty groups, non-root username",
			cfg:      populated,
			username: "carol",
			want:     false,
		},
		{
			name:     "empty config rejects every input",
			cfg:      Config{},
			username: "alice",
			groups:   []string{"WeaviateOps"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.IsRoot(tt.username, tt.groups))
		})
	}
}
