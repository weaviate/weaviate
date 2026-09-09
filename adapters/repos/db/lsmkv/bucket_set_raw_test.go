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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucketSetRawListReadsFlushedSegment reads a posting that lives in a
// flushed segment, which resolves through the disk index rather than the
// memtable path SetRawList otherwise takes.
func TestBucketSetRawListReadsFlushedSegment(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	postings := map[string][][]byte{
		"key-02": {[]byte("a"), []byte("b")},
		"key-04": {[]byte("c")},
		"key-06": {[]byte("d"), []byte("e"), []byte("f")},
	}
	for key, values := range postings {
		require.NoError(t, b.SetAdd([]byte(key), values))
	}
	// on disk, so the read resolves through the segment rather than the memtable
	require.NoError(t, b.FlushAndSwitch())

	for key, want := range postings {
		t.Run(key, func(t *testing.T) {
			got, err := b.SetRawList([]byte(key))
			require.NoError(t, err)
			require.ElementsMatch(t, want, got)
		})
	}

	t.Run("absent key", func(t *testing.T) {
		got, err := b.SetRawList([]byte("key-99"))
		require.NoError(t, err)
		require.Empty(t, got)
	})
}
