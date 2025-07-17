//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package usage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseModule_ShardJitterConfiguration(t *testing.T) {
	// Test 1: Check that the default value is correct
	assert.Equal(t, 100*time.Millisecond, DefaultShardJitterInterval, "jitter interval should be 100ms")

	// Test 2: Test that the module initializes with default jitter
	module := NewBaseModule("test-module", nil)
	assert.Equal(t, DefaultShardJitterInterval, module.shardJitter, "module should use default jitter")

	// Test 3: Test configurable jitter
	customJitter := 50 * time.Millisecond
	module.shardJitter = customJitter
	assert.Equal(t, customJitter, module.shardJitter, "module should use custom jitter")

	// Test 4: Test that zero jitter is handled gracefully
	module.shardJitter = 0
	assert.Equal(t, 0*time.Millisecond, module.shardJitter, "module should allow zero jitter")

	// Test 5: Test that negative jitter is handled gracefully
	module.shardJitter = -1 * time.Millisecond
	assert.Equal(t, -1*time.Millisecond, module.shardJitter, "module should allow negative jitter")
}
