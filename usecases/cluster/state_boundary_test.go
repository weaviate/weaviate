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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBasicAuthEnabled pins that basic auth is enabled when EITHER credential is
// set (`Username != "" || Password != ""`), so each operand of the disjunction
// matters.
func TestBasicAuthEnabled(t *testing.T) {
	assert.False(t, BasicAuth{}.Enabled())
	assert.True(t, BasicAuth{Username: "u"}.Enabled(), "username alone enables auth")
	assert.True(t, BasicAuth{Password: "p"}.Enabled(), "password alone enables auth")
	assert.True(t, BasicAuth{Username: "u", Password: "p"}.Enabled())
}

// TestValidateClusterConfigPortBoundaries pins the inclusive 1024..65535 port
// bounds: the boundary values themselves must be accepted, distinguishing
// `< 1024` from `<= 1024` and `> 65535` from `>= 65535` on every port field.
func TestValidateClusterConfigPortBoundaries(t *testing.T) {
	for _, port := range []int{1024, 65535} {
		assert.NoErrorf(t, validateClusterConfig(Config{Hostname: "n", GossipBindPort: port}),
			"GossipBindPort %d is within the valid range", port)
		assert.NoErrorf(t, validateClusterConfig(Config{Hostname: "n", DataBindPort: port}),
			"DataBindPort %d is within the valid range", port)
		assert.NoErrorf(t, validateClusterConfig(Config{Hostname: "n", AdvertisePort: port}),
			"AdvertisePort %d is within the valid range", port)
	}
}
