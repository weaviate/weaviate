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
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestCommitlogParserRoaringSetLegacyNode pins WAL replay of legacy
// CommitTypeRoaringSet records against SegmentNode's nil-bitmap accessor
// contract: a record whose additions or deletions region is empty
// (zero-length, e.g. a deletions-only write) decodes to a nil bitmap, and
// replay must consume it as empty rather than fail.
func TestCommitlogParserRoaringSetLegacyNode(t *testing.T) {
	tests := []struct {
		name      string
		additions []uint64
		deletions []uint64
	}{
		{name: "deletions only", deletions: []uint64{7, 8}},
		{name: "additions only", additions: []uint64{1, 2, 3}},
		{name: "additions and deletions", additions: []uint64{1, 2}, deletions: []uint64{3}},
		{name: "both empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := []byte("k1")
			sn, err := roaringset.NewSegmentNode(key,
				roaringset.NewBitmap(tt.additions...),
				roaringset.NewBitmap(tt.deletions...))
			require.NoError(t, err)

			var gotKey []byte
			var gotAdditions, gotDeletions []uint64
			prs := &commitlogParserRoaringSet{
				consume: func(key []byte, additions, deletions []uint64) error {
					gotKey = key
					gotAdditions = additions
					gotDeletions = deletions
					return nil
				},
			}

			require.NoError(t, prs.parseNode(bytes.NewReader(sn.ToBuffer())))
			assert.Equal(t, key, gotKey)
			assert.Equal(t, tt.additions, gotAdditions)
			assert.Equal(t, tt.deletions, gotDeletions)
		})
	}
}

// recordReader keeps the last buffer the parser read into, so the test can
// overwrite the record after the parse.
type recordReader struct {
	*bytes.Reader
	dst []byte
}

func (r *recordReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.dst = p[:n]
	return n, err
}

// TestCommitlogParserRoaringSetCopiesKey pins that a replayed key is a copy
// rather than a window into the record, which the memtable would keep alive.
func TestCommitlogParserRoaringSetCopiesKey(t *testing.T) {
	docIDs := []uint64{1, 2, 3}
	wantKey := []byte("some-primary-key")

	tests := []struct {
		name   string
		record func(t *testing.T) []byte
		parse  func(prs *commitlogParserRoaringSet, r io.Reader) error
	}{
		{
			name: "list record",
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNodeList(wantKey, docIDs, nil)
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error {
				return prs.parseNodeList(r)
			},
		},
		{
			name: "bitmap record",
			record: func(t *testing.T) []byte {
				n, err := roaringset.NewSegmentNode(wantKey,
					roaringset.NewBitmap(docIDs...), roaringset.NewBitmap())
				require.NoError(t, err)
				return n.ToBuffer()
			},
			parse: func(prs *commitlogParserRoaringSet, r io.Reader) error {
				return prs.parseNode(r)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotKey []byte
			prs := &commitlogParserRoaringSet{
				consume: func(key []byte, _, _ []uint64) error {
					gotKey = key
					return nil
				},
			}

			reader := &recordReader{Reader: bytes.NewReader(tt.record(t))}
			require.NoError(t, tt.parse(prs, reader))

			// a key that still reads correctly after this is a copy
			for i := range reader.dst {
				reader.dst[i] = 0xff
			}
			require.Equal(t, wantKey, gotKey)
		})
	}
}
