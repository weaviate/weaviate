//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
)

func TestBucket_WasDeleted(t *testing.T) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	b, err := NewBucket(context.Background(), tmpDir, "", logger, nil)
	require.Nil(t, err)

	var (
		key = []byte("key")
		val = []byte("value")
	)

	t.Run("insert object", func(t *testing.T) {
		err = b.Put(key, val)
		require.Nil(t, err)
	})

	t.Run("assert object was not deleted yet", func(t *testing.T) {
		deleted, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.False(t, deleted)
	})

	t.Run("delete object", func(t *testing.T) {
		err = b.Delete(key)
		require.Nil(t, err)
	})

	t.Run("assert object was deleted", func(t *testing.T) {
		deleted, err := b.WasDeleted(key)
		require.Nil(t, err)
		assert.True(t, deleted)
	})

	t.Run("assert a nonexistent object is not detected as deleted", func(t *testing.T) {
		deleted, err := b.WasDeleted([]byte("DNE"))
		require.Nil(t, err)
		assert.False(t, deleted)
	})
}

func TestBucket_MemtableCountWithFlushing(t *testing.T) {
	b := Bucket{
		// by using an empty segment group for the disk portion, we can test the
		// memtable portion in isolation
		disk: &SegmentGroup{},
	}

	tests := []struct {
		name                string
		current             *countStats
		previous            *countStats
		expectedNetActive   int
		expectedNetPrevious int
		expectedNetTotal    int
	}{
		{
			name: "only active, only additions",
			current: &countStats{
				upsertKeys: [][]byte{[]byte("key-1")},
			},
			expectedNetActive: 1,
		},
		{
			name: "only active, both additions and deletions",
			current: &countStats{
				upsertKeys: [][]byte{[]byte("key-1")},
				// no key with key-2 ever existed, so this does not alter the net count
				tombstonedKeys: [][]byte{[]byte("key-2")},
			},
			expectedNetActive: 1,
		},
		{
			name: "an deletion that was previously added",
			current: &countStats{
				tombstonedKeys: [][]byte{[]byte("key-a")},
			},
			previous: &countStats{
				upsertKeys: [][]byte{[]byte("key-a")},
			},
			expectedNetActive:   -1,
			expectedNetPrevious: 1,
			expectedNetTotal:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualActive := b.memtableNetCount(tt.current, tt.previous)
			assert.Equal(t, tt.expectedNetActive, actualActive)

			if tt.previous != nil {
				actualPrevious := b.memtableNetCount(tt.previous, nil)
				assert.Equal(t, tt.expectedNetPrevious, actualPrevious)

				assert.Equal(t, tt.expectedNetTotal, actualPrevious+actualActive)
			}
		})
	}
}
