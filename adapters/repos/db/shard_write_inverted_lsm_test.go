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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// A version-1 shard's little-endian doc IDs must keep coming out byte for
// byte: they address postings and bitmaps of data already on disk.
func TestSearchableDocIDKeyBytes(t *testing.T) {
	docIDs := []uint64{0, 1, 42, 1 << 40, ^uint64(0)}

	tests := []struct {
		name    string
		version uint16
		encode  func(dst []byte, docID uint64)
	}{
		{name: "version 1", version: 1, encode: binary.LittleEndian.PutUint64},
		{name: "version 2", version: 2, encode: binary.BigEndian.PutUint64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Shard{versioner: &shardVersioner{version: tt.version}}

			for _, docID := range docIDs {
				want := make([]byte, 8)
				tt.encode(want, docID)

				got := make([]byte, 8)
				binary.BigEndian.PutUint64(got, s.searchableDocID(docID))
				require.Equal(t, want, got, "docID %d", docID)

				// the map strategy writes the same key through its own encoder
				require.Equal(t, want, s.pairPropertyWithFrequency(docID, 1, 2).Key, "docID %d", docID)
			}
		})
	}
}
