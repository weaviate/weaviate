//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	expectedStartErrMsg  = "expected start to be %v, got %v"
	unexpectedLeafErrMsg = "unexpected error for leaf %d: %v"
)

// incrementUUID returns the next UUID in lexicographic (big-endian) order.
// It treats the UUID as a 128-bit unsigned integer and adds 1 to it.
//
// This is equivalent to incrementing a big-endian 16-byte integer, propagating carry
// from the least significant byte (end of the array) to the most significant byte
// (beginning of the array).
//
// Example:
//
//	Input:  UUID{0x00, 0x00, ..., 0x01, 0xFF}
//	Output: UUID{0x00, 0x00, ..., 0x02, 0x00}
func incrementUUID(u UUID) UUID {
	res := u

	// Iterate from least significant byte (LSB) to most significant byte (MSB)
	// because UUIDs are big-endian: most significant byte is at index 0
	for i := uuidLen - 1; i >= 0; i-- {
		res[i]++         // Add 1 to the current byte
		if res[i] != 0 { // If no overflow occurred, we're done
			break
		}
		// If overflow (was 0xFF), continue carry to next more significant byte
	}

	return res
}

func TestNewUUIDTreeMapInvalidHeightPanics(t *testing.T) {
	heights := []int{-1, 65, 100}
	for _, height := range heights {
		t.Run("invalid height", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					require.Fail(t, fmt.Sprintf("expected panic for treeHeight=%d, but did not panic", height))
				}
			}()
			NewUUIDTreeMap(height)
		})
	}
}

func TestNewUUIDTreeMapValidHeightsDoNotPanic(t *testing.T) {
	for height := 0; height < 64; height++ {
		t.Run("valid height", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.Fail(t, fmt.Sprintf("unexpected panic for treeHeight=%d: %v", height, r))
				}
			}()
			NewUUIDTreeMap(height)
		})
	}
}

func TestUUIDTreeMapRangeHeight0(t *testing.T) {
	m := NewUUIDTreeMap(0)
	r, err := m.Range(0)
	if err != nil {
		t.Fatal(err)
	}

	expectedStart := UUID{}
	require.Equal(t, expectedStart, r.Start, expectedStartErrMsg, expectedStart, r.Start)

	expectedEnd := UUID{}
	for i := range expectedEnd {
		expectedEnd[i] = 0xFF
	}
	require.Equal(t, expectedEnd, r.End, expectedStartErrMsg, expectedEnd, r.End)
}

func TestUUIDTreeMapRangeHeight1(t *testing.T) {
	m := NewUUIDTreeMap(1)
	var expected byte

	// Leaf 0 - first bit must be 0
	r0, _ := m.Range(0)
	expected = byte(0x00)
	require.Equal(t, expected, r0.Start[0]&0x80, expectedStartErrMsg, expected, r0.Start[0]&0x80)
	require.Equal(t, expected, r0.End[0]&0x80, expectedStartErrMsg, expected, r0.End[0]&0x80)

	// Leaf 1 - first bit must be 1
	r1, _ := m.Range(1)
	expected = byte(0x80)
	require.Equal(t, expected, r1.Start[0]&0x80, expectedStartErrMsg, expected, r1.Start[0]&0x80)
	require.Equal(t, expected, r1.End[0]&0x80, expectedStartErrMsg, expected, r1.End[0]&0x80)
}

func TestUUIDTreeMapRangeHeight64(t *testing.T) {
	m := NewUUIDTreeMap(64)
	leaf := uint64(0x0123456789ABCDEF)
	r, err := m.Range(leaf)
	require.NoError(t, err)

	startPrefix := binary.BigEndian.Uint64(r.Start[:8])
	endPrefix := binary.BigEndian.Uint64(r.End[:8])

	require.Equal(t, leaf, startPrefix, "expected start prefix to match leaf")
	require.Equal(t, leaf, endPrefix, "expected end prefix to match leaf")

	// At treeHeight = 64, the UUID space is partitioned on the full 64-bit prefix,
	// meaning each leaf owns a strictly fixed upper 64 bits.
	//
	// However, we still set the lower 64 bits of r.End to 0xFF (not 0x00),
	// to maintain the invariant that r.End represents the inclusive upper bound
	// of the 128-bit UUID range. This allows consistent use of r.Contains(uuid)
	// and ensures that r.Start <= uuid <= r.End works at all heights.
	expectedEnd := r.Start
	copy(expectedEnd[8:], []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	require.Equal(t, expectedEnd, r.End, "expected lower 64 bits of r.End to be all 0xFF")
}

func TestUUIDTreeMapRangeMaxLeaf64(t *testing.T) {
	m := NewUUIDTreeMap(64)
	var maxLeaf uint64 = math.MaxUint64 // math.MaxUint64

	r, err := m.Range(maxLeaf)
	require.NoErrorf(t, err, "unexpected error for max leaf %d: %v", maxLeaf, err)

	startPrefix := binary.BigEndian.Uint64(r.Start[:8])
	require.Equal(t, maxLeaf, startPrefix, "expected start to be %v, got %v", maxLeaf, startPrefix)

	endPrefix := binary.BigEndian.Uint64(r.End[:8])
	require.Equal(t, maxLeaf, endPrefix, "expected end to be %v, got %v", maxLeaf, endPrefix)

	for i := 8; i < 16; i++ {
		require.Equal(t, byte(0xFF), r.End[i], "expected end to be %v, got %v", 0xFF, r.End[i])
	}
}

func TestUUIDTreeMapRangeHeight5(t *testing.T) {
	m := NewUUIDTreeMap(5)

	// Leaf 3 (binary: 00011)
	leaf := uint64(3)
	r, err := m.Range(leaf)
	require.NoErrorf(t, err, unexpectedLeafErrMsg, leaf, err)

	// Check that prefix bits match expected range
	prefix := leaf << (64 - 5)
	startPrefix := binary.BigEndian.Uint64(r.Start[:8])
	require.Equal(t, prefix, startPrefix, "expected start to be %v, got %v", prefix, startPrefix)

	expectedSuffix := (uint64(1) << (64 - 5)) - 1
	endPrefix := binary.BigEndian.Uint64(r.End[:8])
	require.Equal(t, prefix|expectedSuffix, endPrefix, "expected end to be %v, got %v", prefix|expectedSuffix, endPrefix)
}

func TestUUIDTreeMapRangeExhaustiveHeight3(t *testing.T) {
	m := NewUUIDTreeMap(3)

	var ranges []UUIDRange
	for i := uint64(0); i < 8; i++ {
		r, err := m.Range(i)
		require.NoErrorf(t, err, unexpectedLeafErrMsg, i, err)
		ranges = append(ranges, r)
	}

	expectedStart := UUID{}
	require.Equal(t, expectedStart, ranges[0].Start, "expected start to be %v, got %v", expectedStart, ranges[0].Start)

	expectedEnd := UUID{}
	for i := range expectedEnd {
		expectedEnd[i] = 0xFF
	}
	lastRange := ranges[len(ranges)-1]
	require.Equal(t, expectedEnd, lastRange.End, "expected last range to be %v, got %v", expectedEnd, lastRange.End)

	for i := 0; i < len(ranges)-1; i++ {
		curr := ranges[i]
		next := ranges[i+1]

		// Ranges are sorted and non-overlapping
		require.True(t, bytes.Compare(curr.End[:], next.Start[:]) < 0,
			"range %d and %d overlap or are out of order: %v >= %v", i, i+1, curr.End, next.Start)

	}
}

func TestUUIDTreeMapConsecutiveLeafRanges(t *testing.T) {
	m := NewUUIDTreeMap(5)

	for i := uint64(0); i < (1<<5)-1; i++ {
		curr, err := m.Range(i)
		require.NoErrorf(t, err, unexpectedLeafErrMsg, i, err)
		next, err := m.Range(i + 1)
		require.NoErrorf(t, err, unexpectedLeafErrMsg, i, err)
		require.True(t, bytes.Compare(curr.End[:], next.Start[:]) < 0, "leaf %d and %d ranges overlap or are out of order: %v >= %v", i, i+1, curr.End, next.Start)

		// Check that curr.End + 1 == next.Start
		endPlusOne := incrementUUID(curr.End)
		require.True(t, bytes.Equal(next.Start[:], endPlusOne[:]), "expected end+1 of leaf %d to equal start of leaf %d\n  got %v\n want %v", i, i+1, next.Start, endPlusOne)
	}
}

func TestUUIDRangeContains(t *testing.T) {
	m := NewUUIDTreeMap(3)
	r, _ := m.Range(5) // leaf 5 → prefix 0b101

	// UUID with prefix 0b101 matches
	u := UUID{}
	u[0] = 0xA0 // binary: 1010 0000
	require.True(t, r.Contains(u), "expected UUID %v to be in range %v", u, r)

	// UUID with prefix 0b111 does not match
	u[0] = 0xE0
	require.False(t, r.Contains(u), "expected UUID %v NOT to be in range %v", u, r)
}

func TestUUIDRangeLeafOutOfBounds(t *testing.T) {
	m := NewUUIDTreeMap(2)
	_, err := m.Range(4)
	require.Errorf(t, err, "expected error for leaf 4 to be in range %v", m)
}

// generateRandomUUIDs generates n random UUIDs
func generateRandomUUIDs(n int) []UUID {
	uuids := make([]UUID, n)
	for i := range uuids {
		_, _ = rand.Read(uuids[i][:])
	}
	return uuids
}

func BenchmarkUUIDRangeContains(b *testing.B) {
	type benchmarkConfig struct {
		treeHeight int
		uuidCount  int
	}

	benchConfigs := []benchmarkConfig{
		{8, 10_000},
		{8, 100_000},
		{8, 1_000_000},
		{16, 10_000},
		{16, 100_000},
		{16, 1_000_000},
		{32, 10_000},
		{32, 100_000},
		{32, 1_000_000},
		{64, 10_000},
		{64, 100_000},
		{64, 1_000_000},
	}

	for _, benchConfig := range benchConfigs {
		b.Run(benchmarkName(benchConfig.treeHeight, benchConfig.uuidCount),
			containsBenchmarkFn(benchConfig.treeHeight, benchConfig.uuidCount))
	}
}

func containsBenchmarkFn(treeHeight, uuidCount int) func(b *testing.B) {
	return func(b *testing.B) {
		m := NewUUIDTreeMap(treeHeight)
		r, err := m.Range(0)
		require.NoErrorf(b, err, "error generating range for tree height %d", treeHeight)

		uuids := generateRandomUUIDs(uuidCount)
		uuids = append(uuids, r.Start) // ensure at least one UUID is in the range
		b.ResetTimer()

		count := 0
		for i := 0; i < b.N; i++ {
			for _, uuid := range uuids {
				if r.Contains(uuid) {
					count++
				}
			}
		}

		require.Greater(b, count, 0, "expected at least one UUID to match the range")
	}
}

func benchmarkName(height, n int) string {
	return "tree_height_" + strconv.FormatInt(int64(height), 10) + "_uuid_count_" + strconv.FormatInt(int64(n), 10)
}
