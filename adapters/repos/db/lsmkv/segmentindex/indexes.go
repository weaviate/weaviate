//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/pkg/errors"
)

type Indexes struct {
	Keys                []Key
	SecondaryIndexCount uint16
	ScratchSpacePath    string
	ObserveWrite        prometheus.Observer
	AllocChecker        memwatch.AllocChecker
}

const WriteToMemoryMaxSize = 10 * 1024 * 1024

func (s *Indexes) WriteTo(w io.Writer, expectedSize uint64) (int64, error) {
	if s.SecondaryIndexCount == 0 {
		// In case there are no secondary indices present, we can write the primary index directly to the writer without
		// all the extra steps
		return s.buildAndMarshalPrimary(w, s.Keys)
	}

	// the expectedSize is only used to decide if we should use the more efficient (but memory-intensive) in-Memory code path
	// or the one with scratch files which is less efficient (more write operations) but can handle any size
	if expectedSize < WriteToMemoryMaxSize && (s.AllocChecker == nil || s.AllocChecker.CheckAlloc(int64(expectedSize)) == nil) {
		return s.writeToMemory(w)
	} else {
		return s.writeToScratchFiles(w)
	}
}

func (s *Indexes) writeToMemory(w io.Writer) (int64, error) {
	var currentOffset uint64 = HeaderSize
	if len(s.Keys) > 0 {
		currentOffset = uint64(s.Keys[len(s.Keys)-1].ValueEnd)
	}
	secondaryIndexCountSize := uint64(s.SecondaryIndexCount) * byteops.Uint64Len
	currentOffset += secondaryIndexCountSize

	primaryIndex := s.buildPrimary(s.Keys)
	primaryIndexSize := uint64(primaryIndex.Size())
	currentOffset += primaryIndexSize

	offsetSecondaryStart := currentOffset
	secondaryIndexSize := uint64(0)
	var secondaryTrees []*Tree
	if s.SecondaryIndexCount > 0 {
		secondaryTrees = make([]*Tree, s.SecondaryIndexCount)
		for pos := range secondaryTrees {
			secondary, err := s.buildSecondary(s.Keys, pos)
			if err != nil {
				return 0, err
			}
			secondaryTrees[pos] = &secondary
			secondaryIndexSize += uint64(secondary.Size())
		}
	}

	buf := make([]byte, primaryIndexSize+secondaryIndexCountSize+secondaryIndexSize)
	rw := byteops.NewReadWriter(buf)

	for _, secondary := range secondaryTrees {
		rw.WriteUint64(offsetSecondaryStart)
		offsetSecondaryStart += uint64(secondary.Size())
	}

	_, err := primaryIndex.MarshalBinaryInto(&rw)
	if err != nil {
		return 0, err
	}

	for _, secondary := range secondaryTrees {
		_, err := secondary.MarshalBinaryInto(&rw)
		if err != nil {
			return 0, err
		}
	}
	written, err := w.Write(rw.Buffer)
	if err != nil {
		return 0, err
	}

	return int64(written), nil
}

func (s *Indexes) buildSecondary(keys []Key, pos int) (Tree, error) {
	keyNodes := make([]Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = Node{
			Key:   key.SecondaryKeys[pos],
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
		i++
	}

	keyNodes = keyNodes[:i]

	index := NewBalanced(keyNodes)
	return index, nil
}

// assumes sorted keys and does NOT sort them again
func (s *Indexes) buildPrimary(keys []Key) Tree {
	keyNodes := make([]Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = Node{
			Key:   key.Key,
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
	}
	index := NewBalanced(keyNodes)

	return index
}

// WriteTo writes the indices (primary and secondary) to the writer
//
// The segment files look like this:
// - header (already written)
// - secondary indexes offset (if present)
// - primary index
// - secondary indexes (if present)
//
// We first write the primary index to a scratch file to know the positions of the secondary indices. Only then we know
// the offsets of the secondary indices.
func (s *Indexes) writeToScratchFiles(w io.Writer) (int64, error) {
	var currentOffset uint64 = HeaderSize
	if len(s.Keys) > 0 {
		currentOffset = uint64(s.Keys[len(s.Keys)-1].ValueEnd)
	}
	var written int64

	if _, err := os.Stat(s.ScratchSpacePath); err == nil {
		// exists, we need to delete
		// This could be the case if Weaviate shut down unexpectedly (i.e. crashed)
		// while a compaction was running. We can safely discard the contents of
		// the scratch space.

		if err := os.RemoveAll(s.ScratchSpacePath); err != nil {
			return written, errors.Wrap(err, "clean up previous scratch space")
		}
	} else if os.IsNotExist(err) {
		// does not exist yet, nothing to - will be created in the next step
	} else {
		return written, errors.Wrap(err, "check for scratch space directory")
	}

	if err := os.Mkdir(s.ScratchSpacePath, 0o777); err != nil {
		return written, errors.Wrap(err, "create scratch space")
	}

	primaryFileName := filepath.Join(s.ScratchSpacePath, "primary")
	primaryFD, err := os.Create(primaryFileName)
	if err != nil {
		return written, err
	}
	primaryFDBuffered := bufio.NewWriter(diskio.NewMeteredWriter(primaryFD, func(written int64) {
		s.ObserveWrite.Observe(float64(written))
	}))

	n, err := s.buildAndMarshalPrimary(primaryFDBuffered, s.Keys)
	if err != nil {
		return written, err
	}

	if err := primaryFDBuffered.Flush(); err != nil {
		return written, err
	}

	if _, err := primaryFD.Seek(0, io.SeekStart); err != nil {
		return written, fmt.Errorf("seek to start of primary scratch space: %w", err)
	}

	// pretend that primary index was already written, then also account for the
	// additional offset pointers (one for each secondary index)
	currentOffset = currentOffset + uint64(n) +
		uint64(s.SecondaryIndexCount)*8

	secondaryFileName := filepath.Join(s.ScratchSpacePath, "secondary")
	secondaryFD, err := os.Create(secondaryFileName)
	if err != nil {
		return written, err
	}

	secondaryFDBuffered := bufio.NewWriter(diskio.NewMeteredWriter(secondaryFD, func(written int64) {
		s.ObserveWrite.Observe(float64(written))
	}))

	if s.SecondaryIndexCount > 0 {
		offsets := make([]uint64, s.SecondaryIndexCount)
		for pos := range offsets {
			n, err := s.buildAndMarshalSecondary(secondaryFDBuffered, pos, s.Keys)
			if err != nil {
				return written, err
			} else {
				written += int64(n)
			}

			offsets[pos] = currentOffset
			currentOffset = offsets[pos] + uint64(n)
		}

		if err := binary.Write(w, binary.LittleEndian, &offsets); err != nil {
			return written, err
		}

		written += int64(len(offsets)) * 8
	}

	if err := secondaryFDBuffered.Flush(); err != nil {
		return written, err
	}

	if _, err := secondaryFD.Seek(0, io.SeekStart); err != nil {
		return written, fmt.Errorf("seek to start of secondary scratch space: %w", err)
	}

	if n, err := io.Copy(w, primaryFD); err != nil {
		return written, err
	} else {
		written += int64(n)
	}

	if n, err := io.Copy(w, secondaryFD); err != nil {
		return written, err
	} else {
		written += int64(n)
	}

	if err := primaryFD.Close(); err != nil {
		return written, err
	}

	if err := secondaryFD.Close(); err != nil {
		return written, err
	}

	if err := os.RemoveAll(s.ScratchSpacePath); err != nil {
		return written, err
	}

	return written, nil
}

// pos indicates the position of a secondary index, assumes unsorted keys and
// sorts them
func (s *Indexes) buildAndMarshalSecondary(w io.Writer, pos int,
	keys []Key,
) (int64, error) {
	keyNodes := make([]Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = Node{
			Key:   key.SecondaryKeys[pos],
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
		i++
	}

	keyNodes = keyNodes[:i]

	index := NewBalanced(keyNodes)
	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// assumes sorted keys and does NOT sort them again
func (s *Indexes) buildAndMarshalPrimary(w io.Writer, keys []Key) (int64, error) {
	keyNodes := make([]Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = Node{
			Key:   key.Key,
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
	}
	index := NewBalanced(keyNodes)

	n, err := index.MarshalBinaryInto(w)
	if err != nil {
		return -1, err
	}

	return n, nil
}
