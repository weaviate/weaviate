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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// WithRandomAccess must reach the segment group (and thus every segment
// open, where the fadvise/madvise hints are applied). The syscalls are
// advisory and platform-specific, so the test pins the plumbing, not the
// kernel behavior.
func TestWithRandomAccessPlumbing(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("enabled", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(context.Background(), t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace), WithRandomAccess(true))
		require.NoError(t, err)
		defer b.Shutdown(context.Background())

		assert.True(t, b.randomAccess)
		assert.True(t, b.disk.randomAccess)

		// flushing creates a segment through the same config path
		require.NoError(t, b.Put([]byte("k"), []byte("v")))
		require.NoError(t, b.FlushMemtable())
		require.Len(t, b.disk.segments, 1)
	})

	t.Run("default off", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(context.Background(), t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace))
		require.NoError(t, err)
		defer b.Shutdown(context.Background())

		assert.False(t, b.randomAccess)
		assert.False(t, b.disk.randomAccess)
	})
}
