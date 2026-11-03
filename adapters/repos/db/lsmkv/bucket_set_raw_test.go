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
	"encoding/binary"
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

// TestCollectionStratParseDataBytesSkipsTombstones decodes set nodes holding
// tombstones, which a flush writes next to the live values of the same key.
func TestCollectionStratParseDataBytesSkipsTombstones(t *testing.T) {
	type entry struct {
		value     string
		tombstone bool
	}

	encode := func(entries []entry) []byte {
		out := binary.LittleEndian.AppendUint64(nil, uint64(len(entries)))
		for _, e := range entries {
			if e.tombstone {
				out = append(out, 0x01)
			} else {
				out = append(out, 0x00)
			}
			out = binary.LittleEndian.AppendUint64(out, uint64(len(e.value)))
			out = append(out, e.value...)
		}
		return out
	}

	tests := []struct {
		name    string
		entries []entry
		want    [][]byte
	}{
		{
			name:    "no tombstones",
			entries: []entry{{value: "aaaa"}, {value: "bb"}},
			want:    [][]byte{[]byte("aaaa"), []byte("bb")},
		},
		{
			name:    "tombstone first",
			entries: []entry{{value: "dddddddddddd", tombstone: true}, {value: "aaaa"}},
			want:    [][]byte{[]byte("aaaa")},
		},
		{
			name:    "tombstone in the middle",
			entries: []entry{{value: "aaaa"}, {value: "dddddddddddd", tombstone: true}, {value: "bb"}},
			want:    [][]byte{[]byte("aaaa"), []byte("bb")},
		},
		{
			name:    "tombstone last",
			entries: []entry{{value: "aaaa"}, {value: "dddddddddddd", tombstone: true}},
			want:    [][]byte{[]byte("aaaa")},
		},
		{
			name:    "consecutive tombstones",
			entries: []entry{{value: "d1", tombstone: true}, {value: "dddddddddddd", tombstone: true}, {value: "aaaa"}},
			want:    [][]byte{[]byte("aaaa")},
		},
		{
			name:    "only tombstones",
			entries: []entry{{value: "dddddddddddd", tombstone: true}},
			want:    [][]byte{},
		},
		{
			name:    "empty tombstoned value",
			entries: []entry{{value: "", tombstone: true}, {value: "aaaa"}},
			want:    [][]byte{[]byte("aaaa")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&segment{}).collectionStratParseDataBytes(encode(tt.entries))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestBucketSetRawListReadsFlushedTombstone reads a key whose flushed node
// carries a tombstone, in the first segment and in a later one.
func TestBucketSetRawListReadsFlushedTombstone(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	key := []byte("key")
	require.NoError(t, b.SetAdd(key, [][]byte{[]byte("kept-value-1"), []byte("gone-value-1")}))
	require.NoError(t, b.SetDeleteSingle(key, []byte("gone-value-1")))
	require.NoError(t, b.FlushAndSwitch())

	got, err := b.SetRawList(key)
	require.NoError(t, err)
	require.Contains(t, got, []byte("kept-value-1"))

	require.NoError(t, b.SetAdd(key, [][]byte{[]byte("kept-value-2")}))
	require.NoError(t, b.SetDeleteSingle(key, []byte("kept-value-1")))
	require.NoError(t, b.FlushAndSwitch())

	got, err = b.SetRawList(key)
	require.NoError(t, err)
	require.Contains(t, got, []byte("kept-value-2"))
}
