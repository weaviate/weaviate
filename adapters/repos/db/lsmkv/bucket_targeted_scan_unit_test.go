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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// stubDiskIndex yields a fixed node list, so a scan can be driven over ranges a
// well-formed index would never produce.
type stubDiskIndex struct {
	nodes []segmentNodeRange
}

func (s *stubDiskIndex) ForEachNodeInRange(from, to int, fn func(key []byte, start, end uint64) error) error {
	for _, n := range s.nodes {
		if err := fn(n.Key, n.Start, n.End); err != nil {
			return err
		}
	}
	return nil
}

func (s *stubDiskIndex) SplitNodeRanges(parts int) [][2]int        { panic("not implemented") }
func (s *stubDiskIndex) Contains(key []byte) (bool, error)         { panic("not implemented") }
func (s *stubDiskIndex) Get(key []byte) (segmentindex.Node, error) { panic("not implemented") }

func (s *stubDiskIndex) GetOffsets(key []byte) (start, end uint64, err error) {
	panic("not implemented")
}

func (s *stubDiskIndex) Seek(key []byte) (segmentindex.Node, error) { panic("not implemented") }
func (s *stubDiskIndex) Next(key []byte) (segmentindex.Node, error) { panic("not implemented") }
func (s *stubDiskIndex) AllKeys() ([][]byte, error)                 { panic("not implemented") }
func (s *stubDiskIndex) Size() int                                  { panic("not implemented") }
func (s *stubDiskIndex) KeyCount() int                              { panic("not implemented") }
func (s *stubDiskIndex) QuantileKeys(q int) [][]byte                { panic("not implemented") }
func (s *stubDiskIndex) ForEachKey(fn func(key []byte))             { panic("not implemented") }

// TestScanIndexNodesBoundsGuard: a node range that a corrupt index could report
// must abort the scan before it reaches the callback, where it would size a read
// against bytes outside the segment's data section.
func TestScanIndexNodesBoundsGuard(t *testing.T) {
	const dataStart, dataEnd = 100, 1000

	tests := []struct {
		name        string
		start, end  uint64
		expectError bool
	}{
		{name: "well-formed node", start: 200, end: 300},
		{name: "empty node", start: 200, end: 200, expectError: true},
		{name: "reversed bounds", start: 300, end: 200, expectError: true},
		{name: "smaller than its header", start: 200, end: 208, expectError: true},
		{name: "exactly its header", start: 200, end: 209},
		{name: "starts before the data section", start: dataStart - 1, end: 300, expectError: true},
		{name: "ends past the data section", start: 900, end: dataEnd + 1, expectError: true},
		{name: "flush against both bounds", start: dataStart, end: dataEnd},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &segment{
				index:        &stubDiskIndex{nodes: []segmentNodeRange{{Key: []byte("k"), Start: test.start, End: test.end}}},
				dataStartPos: dataStart,
				dataEndPos:   dataEnd,
			}

			served := 0
			err := s.scanIndexNodes(0, 0, func(n segmentNodeRange) error {
				served++
				return nil
			})

			if test.expectError {
				require.ErrorContains(t, err, "outside data bounds")
				require.Zero(t, served, "callback must not run for an out-of-bounds node")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, served)
		})
	}
}

func TestCheckNodeValueLen(t *testing.T) {
	// a node holds a 9-byte header plus its value
	n := segmentNodeRange{Start: 500, End: 600}

	require.NoError(t, checkNodeValueLen(0, n))
	require.NoError(t, checkNodeValueLen(91, n))
	require.Error(t, checkNodeValueLen(92, n))
	require.Error(t, checkNodeValueLen(^uint64(0), n))
}

type probeErrSegment struct {
	*fakeSegment
	err error
}

func (s *probeErrSegment) indexContainsKey(key []byte) (bool, error) { return false, s.err }

type oneNodeSegment struct {
	*fakeSegment
	node segmentNodeRange
}

func (s *oneNodeSegment) scanIndexNodes(from, to int, fn func(n segmentNodeRange) error) error {
	return fn(s.node)
}

// TestScanTargetedSegmentRangeProbeError: an index error while probing a newer
// segment must abort the scan. Treating it as "key absent" would serve a row the
// newer segment supersedes.
func TestScanTargetedSegmentRangeProbeError(t *testing.T) {
	sentinel := errors.New("index read failed")
	task := targetedScanTask{
		seg:   &oneNodeSegment{node: segmentNodeRange{Key: []byte("key"), Start: 100, End: 200}},
		newer: []Segment{&probeErrSegment{err: sentinel}},
	}

	served := 0
	err := scanTargetedSegmentRange(context.Background(), task, 16, nil,
		func(e *TargetedScanEntry) error {
			served++
			return nil
		})

	require.ErrorIs(t, err, sentinel)
	require.Zero(t, served, "callback must not run when a newer segment cannot be probed")
}
