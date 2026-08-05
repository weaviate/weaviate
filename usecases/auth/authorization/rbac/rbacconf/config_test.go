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
