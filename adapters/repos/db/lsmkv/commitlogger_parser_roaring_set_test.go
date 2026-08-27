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
