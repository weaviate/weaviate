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

package segmentindex

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
)

type Indexes struct {
	Keys                []Key
	SecondaryIndexCount uint16
	ScratchSpacePath    string
}

func (s Indexes) WriteTo(w io.Writer) (int64, error) {
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

	primaryFDBuffered := bufio.NewWriter(primaryFD)

	n, err := s.buildAndMarshalPrimary(primaryFDBuffered, s.Keys)
	if err != nil {
		return written, err
	}

	if err := primaryFDBuffered.Flush(); err != nil {
		return written, err
	}

	primaryFD.Seek(0, io.SeekStart)

	// pretend that primary index was already written, then also account for the
	// additional offset pointers (one for each secondary index)
	currentOffset = currentOffset + uint64(n) +
		uint64(s.SecondaryIndexCount)*8

	// secondaryIndicesBytes := bytes.NewBuffer(nil)
	secondaryFileName := filepath.Join(s.ScratchSpacePath, "secondary")
	secondaryFD, err := os.Create(secondaryFileName)
	if err != nil {
		return written, err
	}

	secondaryFDBuffered := bufio.NewWriter(secondaryFD)

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

	secondaryFD.Seek(0, io.SeekStart)

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

	sort.Slice(keyNodes, func(a, b int) bool {
		return bytes.Compare(keyNodes[a].Key, keyNodes[b].Key) < 0
	})

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
