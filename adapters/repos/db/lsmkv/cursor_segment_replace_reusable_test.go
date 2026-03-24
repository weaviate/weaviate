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

//go:build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// TestSegmentCursorReplaceReusable_MatchesOldCursor creates on-disk segments
// using a Bucket and then verifies that the reusable cursor produces the exact
// same sequence of keys, values, and errors as the original segmentCursorReplace
// cursor for a variety of edge cases.
func TestSegmentCursorReplaceReusable_MatchesOldCursor(t *testing.T) {
	tests := []struct {
		name string
		// setup returns a bucket with at least one flushed segment.
		setup func(t *testing.T, ctx context.Context) *Bucket
	}{
		{
			name: "single key",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				return createReplaceBucket(t, ctx, map[string]string{
					"key-000": "val-000",
				})
			},
		},
		{
			name: "many keys sorted",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				kv := make(map[string]string, 200)
				for i := 0; i < 200; i++ {
					kv[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%03d", i)
				}
				return createReplaceBucket(t, ctx, kv)
			},
		},
		{
			name: "keys with tombstones",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				for i := 0; i < 20; i++ {
					key := []byte(fmt.Sprintf("key-%03d", i))
					val := []byte(fmt.Sprintf("value-%03d", i))
					require.NoError(t, bucket.Put(key, val))
				}
				// Delete every other key
				for i := 0; i < 20; i += 2 {
					key := []byte(fmt.Sprintf("key-%03d", i))
					require.NoError(t, bucket.Delete(key))
				}
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
		{
			name: "all tombstones",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				for i := 0; i < 10; i++ {
					key := []byte(fmt.Sprintf("key-%03d", i))
					val := []byte(fmt.Sprintf("value-%03d", i))
					require.NoError(t, bucket.Put(key, val))
				}
				for i := 0; i < 10; i++ {
					key := []byte(fmt.Sprintf("key-%03d", i))
					require.NoError(t, bucket.Delete(key))
				}
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
		{
			name: "large values",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				for i := 0; i < 5; i++ {
					key := []byte(fmt.Sprintf("key-%03d", i))
					val := make([]byte, 10_000)
					for j := range val {
						val[j] = byte(i)
					}
					require.NoError(t, bucket.Put(key, val))
				}
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
		{
			name: "empty key and value",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				// Put a key with empty value
				require.NoError(t, bucket.Put([]byte("a"), nil))
				require.NoError(t, bucket.Put([]byte("b"), []byte{}))
				require.NoError(t, bucket.Put([]byte("c"), []byte("nonempty")))
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
		{
			name: "single tombstone only",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				require.NoError(t, bucket.Put([]byte("key"), []byte("value")))
				require.NoError(t, bucket.Delete([]byte("key")))
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
		{
			name: "two keys same prefix",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				return createReplaceBucket(t, ctx, map[string]string{
					"prefix":       "short",
					"prefix-extra": "long",
				})
			},
		},
		{
			name: "binary keys and values",
			setup: func(t *testing.T, ctx context.Context) *Bucket {
				bucket := createReplaceBucketEmpty(t, ctx)
				for i := 0; i < 10; i++ {
					key := []byte{byte(i), 0x00, 0xFF, byte(i)}
					val := []byte{0xFF, byte(i), 0x00, byte(i), 0xFE}
					require.NoError(t, bucket.Put(key, val))
				}
				require.NoError(t, bucket.FlushAndSwitch())
				return bucket
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := tt.setup(t, ctx)
			defer bucket.Shutdown(ctx)

			require.GreaterOrEqual(t, len(bucket.disk.segments), 1,
				"expected at least one flushed segment")

			for segIdx, seg := range bucket.disk.segments {
				realSeg, ok := seg.(*segment)
				if !ok {
					continue
				}

				t.Run(fmt.Sprintf("segment_%d", segIdx), func(t *testing.T) {
					// Collect all entries from the old cursor.
					oldCursor := realSeg.newCursor()
					var oldEntries []cursorEntry
					k, v, err := oldCursor.first()
					for k != nil {
						oldEntries = append(oldEntries, cursorEntry{
							key:       copySlice(k),
							value:     copySlice(v),
							tombstone: err == lsmkv.Deleted,
						})
						if err != nil && err != lsmkv.Deleted {
							break
						}
						k, v, err = oldCursor.next()
					}

					// Collect all entries from the reusable cursor.
					reusable := realSeg.newReplaceCursorReusable()
					var newEntries []cursorEntry
					node, err := reusable.first()
					for node != nil {
						newEntries = append(newEntries, cursorEntry{
							key:       copySlice(node.primaryKey),
							value:     copySlice(node.value),
							tombstone: err == lsmkv.Deleted,
						})
						if err != nil && err != lsmkv.Deleted {
							break
						}
						node, err = reusable.next()
					}

					require.Equal(t, len(oldEntries), len(newEntries),
						"entry count mismatch")

					for i, old := range oldEntries {
						got := newEntries[i]
						assert.Equal(t, old.key, got.key,
							"key mismatch at index %d", i)
						assert.Equal(t, old.tombstone, got.tombstone,
							"tombstone mismatch at index %d", i)
						// The old cursor returns nil value for tombstones,
						// whereas the reusable cursor exposes the raw node
						// value. Only compare values for live entries.
						if !old.tombstone {
							assert.Equal(t, old.value, got.value,
								"value mismatch at index %d", i)
						}
					}
				})
			}
		})
	}
}

// TestSegmentCursorReplaceReusable_EmptySegment verifies both cursors handle
// a segment where all keys are tombstones (closest to "empty" data).
// Note: flushing a truly empty memtable does not create a segment file, so we
// create a segment with only tombstones instead.
func TestSegmentCursorReplaceReusable_EmptySegment(t *testing.T) {
	ctx := context.Background()
	bucket := createReplaceBucketEmpty(t, ctx)
	defer bucket.Shutdown(ctx)

	require.NoError(t, bucket.Put([]byte("only-key"), []byte("only-val")))
	require.NoError(t, bucket.Delete([]byte("only-key")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.GreaterOrEqual(t, len(bucket.disk.segments), 1)

	seg, ok := bucket.disk.segments[0].(*segment)
	if !ok {
		t.Skip("segment is not a *segment (lazy?)")
	}

	// Old cursor: first returns key with Deleted error
	oldCur := seg.newCursor()
	k, v, err := oldCur.first()
	assert.NotNil(t, k, "old cursor: key should be non-nil for tombstone")
	assert.Nil(t, v, "old cursor: value should be nil for tombstone")
	assert.ErrorIs(t, err, lsmkv.Deleted)

	// Next should return NotFound
	k, _, err = oldCur.next()
	assert.Nil(t, k)
	assert.ErrorIs(t, err, lsmkv.NotFound)

	// Reusable cursor
	reusable := seg.newReplaceCursorReusable()
	node, err := reusable.first()
	assert.NotNil(t, node, "reusable cursor: node should be non-nil for tombstone")
	assert.True(t, node.tombstone)
	assert.ErrorIs(t, err, lsmkv.Deleted)

	node, err = reusable.next()
	assert.Nil(t, node)
	assert.ErrorIs(t, err, lsmkv.NotFound)
}

// TestSegmentCursorReplaceReusable_FullIteration verifies that calling
// first() then next() until exhaustion produces the same sequence from
// both cursor types. Values must be copied immediately from the reusable
// cursor because the returned pointer is reused on each call.
func TestSegmentCursorReplaceReusable_FullIteration(t *testing.T) {
	ctx := context.Background()
	bucket := createReplaceBucket(t, ctx, map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	})
	defer bucket.Shutdown(ctx)

	require.GreaterOrEqual(t, len(bucket.disk.segments), 1)

	seg, ok := bucket.disk.segments[0].(*segment)
	if !ok {
		t.Skip("segment is not a *segment")
	}

	// Collect from old cursor
	oldCur := seg.newCursor()
	var oldKeys, oldVals []string
	k, v, err := oldCur.first()
	for k != nil {
		oldKeys = append(oldKeys, string(k))
		oldVals = append(oldVals, string(v))
		if err != nil && err != lsmkv.Deleted {
			break
		}
		k, v, err = oldCur.next()
	}
	assert.ErrorIs(t, err, lsmkv.NotFound)

	// Collect from reusable cursor, copying immediately
	reusable := seg.newReplaceCursorReusable()
	var newKeys, newVals []string
	node, err := reusable.first()
	for node != nil {
		newKeys = append(newKeys, string(copySlice(node.primaryKey)))
		newVals = append(newVals, string(copySlice(node.value)))
		if err != nil && err != lsmkv.Deleted {
			break
		}
		node, err = reusable.next()
	}
	assert.ErrorIs(t, err, lsmkv.NotFound)

	assert.Equal(t, oldKeys, newKeys)
	assert.Equal(t, oldVals, newVals)
}

// TestSegmentCursorReplaceReusable_MultipleFirstCalls verifies that calling
// first() multiple times resets the cursor to the beginning each time.
func TestSegmentCursorReplaceReusable_MultipleFirstCalls(t *testing.T) {
	ctx := context.Background()
	bucket := createReplaceBucket(t, ctx, map[string]string{
		"key-a": "val-a",
		"key-b": "val-b",
	})
	defer bucket.Shutdown(ctx)

	require.GreaterOrEqual(t, len(bucket.disk.segments), 1)

	seg, ok := bucket.disk.segments[0].(*segment)
	if !ok {
		t.Skip("segment is not a *segment")
	}

	reusable := seg.newReplaceCursorReusable()

	// First call
	n1, err1 := reusable.first()
	require.NoError(t, err1)
	key1 := copySlice(n1.primaryKey)

	// Advance
	_, _ = reusable.next()

	// Call first() again — should return the same first key
	n2, err2 := reusable.first()
	require.NoError(t, err2)
	assert.Equal(t, key1, n2.primaryKey)
}

// TestSegmentCursorReplaceReusable_KeyCount verifies the keyCount() method.
func TestSegmentCursorReplaceReusable_KeyCount(t *testing.T) {
	tests := []struct {
		name     string
		kvCount  int
		expected int // may differ due to tombstones in the index
	}{
		{"single key", 1, 1},
		{"ten keys", 10, 10},
		{"hundred keys", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			kv := make(map[string]string, tt.kvCount)
			for i := 0; i < tt.kvCount; i++ {
				kv[fmt.Sprintf("key-%04d", i)] = fmt.Sprintf("val-%04d", i)
			}
			bucket := createReplaceBucket(t, ctx, kv)
			defer bucket.Shutdown(ctx)

			seg, ok := bucket.disk.segments[0].(*segment)
			if !ok {
				t.Skip("segment is not a *segment")
			}

			reusable := seg.newReplaceCursorReusable()
			assert.Equal(t, tt.expected, reusable.keyCount())
		})
	}
}

// TestSegmentCursorReplaceReusable_ReusableNodeOverwrite verifies the ownership
// contract: the returned node pointer is reused on each call, so previous
// values are overwritten.
func TestSegmentCursorReplaceReusable_ReusableNodeOverwrite(t *testing.T) {
	ctx := context.Background()
	bucket := createReplaceBucket(t, ctx, map[string]string{
		"key-a": "val-a",
		"key-b": "val-b",
		"key-c": "val-c",
	})
	defer bucket.Shutdown(ctx)

	seg, ok := bucket.disk.segments[0].(*segment)
	if !ok {
		t.Skip("segment is not a *segment")
	}

	reusable := seg.newReplaceCursorReusable()

	n1, err := reusable.first()
	require.NoError(t, err)
	// Save the pointer — should be the same pointer on next call
	ptr1 := n1

	n2, err := reusable.next()
	require.NoError(t, err)

	// The pointers should be the same (reused internal node)
	assert.Same(t, ptr1, n2, "expected reusable cursor to return same pointer")
}

// TestSegmentCursorReplaceReusable_MixedTombstonesAndValues exercises
// interleaved tombstones and live values to verify error handling matches.
func TestSegmentCursorReplaceReusable_MixedTombstonesAndValues(t *testing.T) {
	ctx := context.Background()
	bucket := createReplaceBucketEmpty(t, ctx)
	defer bucket.Shutdown(ctx)

	// Put 20 keys, then delete odd-indexed ones before flushing
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		require.NoError(t, bucket.Put(key, val))
	}
	for i := 1; i < 20; i += 2 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, bucket.Delete(key))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	seg, ok := bucket.disk.segments[0].(*segment)
	if !ok {
		t.Skip("segment is not a *segment")
	}

	// Iterate old cursor
	oldCur := seg.newCursor()
	var oldTombstoneKeys []string
	var oldLiveKeys []string
	k, _, err := oldCur.first()
	for k != nil {
		if err == lsmkv.Deleted {
			oldTombstoneKeys = append(oldTombstoneKeys, string(k))
		} else {
			oldLiveKeys = append(oldLiveKeys, string(k))
		}
		k, _, err = oldCur.next()
	}

	// Iterate reusable cursor
	reusable := seg.newReplaceCursorReusable()
	var newTombstoneKeys []string
	var newLiveKeys []string
	node, err := reusable.first()
	for node != nil {
		if err == lsmkv.Deleted {
			newTombstoneKeys = append(newTombstoneKeys, string(node.primaryKey))
		} else {
			newLiveKeys = append(newLiveKeys, string(node.primaryKey))
		}
		node, err = reusable.next()
	}

	assert.Equal(t, oldTombstoneKeys, newTombstoneKeys)
	assert.Equal(t, oldLiveKeys, newLiveKeys)
	assert.Equal(t, 10, len(oldTombstoneKeys), "expected 10 tombstones")
	assert.Equal(t, 10, len(oldLiveKeys), "expected 10 live keys")
}

// --- helpers ---

type cursorEntry struct {
	key       []byte
	value     []byte
	tombstone bool
}

func copySlice(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func createReplaceBucketEmpty(t *testing.T, ctx context.Context) *Bucket {
	t.Helper()
	dir := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	return b
}

func createReplaceBucket(t *testing.T, ctx context.Context, kv map[string]string) *Bucket {
	t.Helper()
	bucket := createReplaceBucketEmpty(t, ctx)
	for k, v := range kv {
		require.NoError(t, bucket.Put([]byte(k), []byte(v)))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	return bucket
}
