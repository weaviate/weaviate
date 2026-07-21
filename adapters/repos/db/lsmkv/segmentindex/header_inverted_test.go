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

package segmentindex

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeaderInvertedValidateOffsetsInBounds(t *testing.T) {
	t.Run("all offsets within bounds: accepted", func(t *testing.T) {
		h := &HeaderInverted{KeysOffset: 45, TombstoneOffset: 100, PropertyLengthsOffset: 150}
		require.NoError(t, h.ValidateOffsetsInBounds(500))
	})

	t.Run("KeysOffset past contentsLen: rejected", func(t *testing.T) {
		h := &HeaderInverted{KeysOffset: 600, TombstoneOffset: 100, PropertyLengthsOffset: 150}
		err := h.ValidateOffsetsInBounds(500)
		require.Error(t, err)
		require.Contains(t, err.Error(), "keys offset")
	})

	t.Run("TombstoneOffset past contentsLen: rejected", func(t *testing.T) {
		h := &HeaderInverted{KeysOffset: 45, TombstoneOffset: 600, PropertyLengthsOffset: 150}
		err := h.ValidateOffsetsInBounds(500)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tombstone offset")
	})

	t.Run("PropertyLengthsOffset past contentsLen: rejected", func(t *testing.T) {
		h := &HeaderInverted{KeysOffset: 45, TombstoneOffset: 100, PropertyLengthsOffset: 600}
		err := h.ValidateOffsetsInBounds(500)
		require.Error(t, err)
		require.Contains(t, err.Error(), "property lengths offset")
	})

	t.Run("an offset exactly at contentsLen: accepted", func(t *testing.T) {
		h := &HeaderInverted{KeysOffset: 45, TombstoneOffset: 100, PropertyLengthsOffset: 500}
		require.NoError(t, h.ValidateOffsetsInBounds(500))
	})
}

func TestHeaderInvertedValidateNonEmptyIndex(t *testing.T) {
	// buildContents lays out a minimal buffer with a real, non-zero
	// propLengthCount at PropertyLengthsOffset+8, matching flushDataInverted's
	// byte layout: avg(8) + count(8) + propertyLengthsSize(8) starting at
	// PropertyLengthsOffset.
	buildContents := func(propertyLengthsOffset uint64, propLengthCount uint64, totalLen uint64) []byte {
		contents := make([]byte, totalLen)
		binary.LittleEndian.PutUint64(contents[propertyLengthsOffset+8:], propLengthCount)
		return contents
	}

	t.Run("QA Claude's exact repro shape: real (non-tombstone) flush, empty primary index: rejected", func(t *testing.T) {
		h := &HeaderInverted{PropertyLengthsOffset: 100}
		contents := buildContents(100, 3, 200) // propLengthCount=3: real entries existed
		err := h.ValidateNonEmptyIndex(contents, 0, 272)
		require.Error(t, err)
		require.Contains(t, err.Error(), "primary index is empty even though")
	})

	t.Run("genuinely all-tombstone flush, empty primary index: accepted", func(t *testing.T) {
		h := &HeaderInverted{PropertyLengthsOffset: 100}
		contents := buildContents(100, 0, 200) // propLengthCount=0: no real entries ever existed
		err := h.ValidateNonEmptyIndex(contents, 0, 272)
		require.NoError(t, err)
	})

	t.Run("non-empty primary index: accepted regardless of propLengthCount", func(t *testing.T) {
		h := &HeaderInverted{PropertyLengthsOffset: 100}
		contents := buildContents(100, 0, 200)
		require.NoError(t, h.ValidateNonEmptyIndex(contents, 43, 272))
	})

	t.Run("IndexStart == HeaderSize (legitimate empty segment): accepted without reading contents", func(t *testing.T) {
		h := &HeaderInverted{PropertyLengthsOffset: 100}
		// contents too short to hold a valid count field at PropertyLengthsOffset -
		// must not be read at all when IndexStart == HeaderSize.
		contents := make([]byte, 10)
		require.NoError(t, h.ValidateNonEmptyIndex(contents, 0, uint64(HeaderSize)))
	})

	t.Run("PropertyLengthsOffset+16 past contents length: rejected as corrupt, not read out of range", func(t *testing.T) {
		h := &HeaderInverted{PropertyLengthsOffset: 190}
		contents := make([]byte, 200) // PropertyLengthsOffset+16 = 206 > 200
		err := h.ValidateNonEmptyIndex(contents, 0, 272)
		require.Error(t, err)
		require.Contains(t, err.Error(), "property lengths count field")
	})
}
