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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestShardConvertQueueWithoutCheckpoints pins that a shard with no checkpoint
// store converts nothing. NewShard calls this on a goroutine, so a nil crashes
// the process rather than failing a caller.
func TestShardConvertQueueWithoutCheckpoints(t *testing.T) {
	tests := []struct {
		name         string
		targetVector string
	}{
		{name: "legacy vector", targetVector: ""},
		{name: "named vector", targetVector: "custom"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Shard{
				name: "shard1",
				index: &Index{
					Config:               IndexConfig{ClassName: schema.ClassName("MyClass")},
					AsyncIndexingEnabled: true,
				},
			}

			require.NoError(t, s.ConvertQueue(test.targetVector))
		})
	}
}
