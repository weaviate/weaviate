//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (s *segment) getCollection(key []byte) ([]value, error) {
	if s.strategy != segmentindex.StrategySetCollection &&
		s.strategy != segmentindex.StrategyMapCollection {
		return nil, fmt.Errorf("get only possible for strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	if !s.bloomFilter.Test(key) {
		return nil, lsmkv.NotFound
	}

	node, err := s.index.Get(key)
	if err != nil {
		return nil, err
	}

	// We need to copy the data we read from the segment exactly once in this
	// place. This means that future processing can share this memory as much as
	// it wants to, as it can now be considered immutable. If we didn't copy in
	// this place it would only be safe to hold this data while still under the
	// protection of the segmentGroup.maintenanceLock. This lock makes sure that
	// no compaction is started during an ongoing read. However, as we could show
	// as part of https://github.com/weaviate/weaviate/issues/1837
	// further processing, such as map-decoding and eventually map-merging would
	// happen inside the bucket.MapList() method. This scope has its own lock,
	// but that lock can only protecting against flushing (i.e. changing the
	// active/flushing memtable), not against removing the disk segment. If a
	// compaction completes and the old segment is removed, we would be accessing
	// invalid memory without the copy, thus leading to a SEGFAULT.
	contentsCopy := make([]byte, node.End-node.Start)
	if err = s.pread(contentsCopy, node.Start, node.End); err != nil {
		return nil, fmt.Errorf("pread: %w", err)
	}

	return s.collectionStratParseData(contentsCopy)
}

func (s *segment) collectionStratParseData(in []byte) ([]value, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}

	offset := 0

	valuesLen := binary.LittleEndian.Uint64(in[offset : offset+8])
	offset += 8

	values := make([]value, valuesLen)
	valueIndex := 0
	for valueIndex < int(valuesLen) {
		values[valueIndex].tombstone = in[offset] == 0x01
		offset += 1

		valueLen := binary.LittleEndian.Uint64(in[offset : offset+8])
		offset += 8

		values[valueIndex].value = in[offset : offset+int(valueLen)]
		offset += int(valueLen)

		valueIndex++
	}

	return values, nil
}

func (s *segment) bytesReaderFrom(in []byte) (*bytes.Reader, error) {
	if len(in) == 0 {
		return nil, lsmkv.NotFound
	}
	return bytes.NewReader(in), nil
}

func (s *segment) bufferedReaderAt(offset uint64) (*bufio.Reader, error) {
	if s.contentFile == nil {
		return nil, fmt.Errorf("nil contentFile for segment at %s", s.path)
	}

	if _, err := s.contentFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil,
			fmt.Errorf("bufferedReaderAt: seek to offset: %w", err)
	}

	return bufio.NewReader(s.contentFile), nil
}
