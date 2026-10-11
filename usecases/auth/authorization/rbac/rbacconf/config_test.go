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
	"strings"
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

func TestValidateRejectsUnstorableSubjects(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{name: "plain subjects", cfg: Config{
			RootUsers: []string{"alice", "customer1:bob"}, RootGroups: []string{"/org/ops team"},
			ReadOnlyGroups: []string{"readers"}, ViewerUsers: []string{"viewer@example.com"}, AdminUsers: []string{"admin"},
		}},
		{name: "empty entry from a trailing comma", cfg: Config{RootUsers: []string{"alice", ""}}},
		// applyPredefinedRoles skips a blank entry, so it never reaches the file.
		{name: "blank entry with a line break", cfg: Config{RootUsers: []string{"alice", "\n"}}},
		{name: "quoted root group", cfg: Config{RootGroups: []string{`"admins"`}}, wantErr: "root_groups"},
		// As a root group its row would be 65535 bytes, the longest line loadPolicyFile
		// reads. The "role:read-only" row is 5 bytes longer.
		{name: "read-only group too long for its own row", cfg: Config{ReadOnlyGroups: []string{strings.Repeat("a", 65515)}}, wantErr: "readonly_groups"},
		// A YAML list item can hold a comma, which would grant the group root.
		{name: "read-only group with comma", cfg: Config{ReadOnlyGroups: []string{"x, role:root"}}, wantErr: "readonly_groups"},
		{name: "root user with LF", cfg: Config{RootUsers: []string{"a\nb"}}, wantErr: "root_users"},
		// A lone '\r' loads back intact. An env file with CRLF line ends can leave one.
		{name: "root user with CR", cfg: Config{RootUsers: []string{"alice\r"}}},
		{name: "viewer user with CRLF", cfg: Config{ViewerUsers: []string{"a\r\nb"}}, wantErr: "viewer_users"},
		{name: "admin user with quote", cfg: Config{AdminUsers: []string{`a"b`}}, wantErr: `admin_users: policy row "g, oidc:a\"b, role:admin"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
