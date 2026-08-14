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
			assert.Equal(t, tt.want, tt.cfg.IsRootUser(tt.username, tt.groups))
		})
	}
}

func TestConfigIsRootUser(t *testing.T) {
	cfg := Config{
		RootUsers:  []string{"root-user"},
		RootGroups: []string{"root-group"},
	}

	tcs := map[string]struct {
		username string
		groups   []string
		expect   bool
	}{
		"root user":            {username: "root-user", expect: true},
		"member of root group": {username: "alice", groups: []string{"root-group"}, expect: true},
		"non-root user":        {username: "alice", expect: false},
		"non-root group":       {username: "alice", groups: []string{"other-group"}, expect: false},
		"empty username":       {username: "", expect: false},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expect, cfg.IsRootUser(tc.username, tc.groups))
		})
	}
}
