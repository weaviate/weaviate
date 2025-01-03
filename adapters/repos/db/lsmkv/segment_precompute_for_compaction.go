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

package lsmkv

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// preComputeSegmentMeta has no side-effects for an already running store. As a
// result this can be run without the need to obtain any locks. All files
// created will have a .tmp suffix so they don't interfere with existing
// segments that might have a similar name.
//
// This function is a partial copy of what happens in newSegment(). Any changes
// made here should likely be made in newSegment, and vice versa. This is
// absolutely not ideal, but in the short time I was able to consider this, I wasn't
// able to find a way to unify the two -- there are subtle differences.
func preComputeSegmentMeta(path string, updatedCountNetAdditions int,
	logger logrus.FieldLogger, useBloomFilter bool, calcCountNetAdditions bool,
	disableChecksumValidation bool,
) ([]string, error) {
	out := []string{path}

	// as a guardrail validate that the segment is considered a .tmp segment.
	// This way we can be sure that we're not accidentally operating on a live
	// segment as the segment group completely ignores .tmp segment files
	if !strings.HasSuffix(path, ".tmp") {
		return nil, fmt.Errorf("pre computing a segment expects a .tmp segment path")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}
	size := fileInfo.Size()

	contents, err := mmap.MapRegion(file, int(fileInfo.Size()), mmap.RDONLY, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap file: %w", err)
	}

	defer contents.Unmap()

	header, err := segmentindex.ParseHeader(bytes.NewReader(contents[:segmentindex.HeaderSize]))
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	if err := segmentindex.CheckExpectedStrategy(header.Strategy); err != nil {
		return nil, fmt.Errorf("unsupported strategy in segment: %w", err)
	}

	if header.Version >= segmentindex.SegmentV1 && !disableChecksumValidation {
		segmentFile := segmentindex.NewSegmentFile(segmentindex.WithReader(file))
		if err := segmentFile.ValidateChecksum(fileInfo); err != nil {
			return nil, fmt.Errorf("validate segment %q: %w", path, err)
		}
	}

	primaryIndex, err := header.PrimaryIndex(contents)
	if err != nil {
		return nil, fmt.Errorf("extract primary index position: %w", err)
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	dataStartPos := uint64(segmentindex.HeaderSize)
	dataEndPos := header.IndexStart

	var invertedHeader *segmentindex.HeaderInverted
	if header.Strategy == segmentindex.StrategyInverted {
		invertedHeader, err = segmentindex.LoadHeaderInverted(contents[segmentindex.HeaderSize : segmentindex.HeaderSize+segmentindex.HeaderInvertedSize])
		if err != nil {
			return nil, errors.Wrap(err, "load inverted header")
		}
		dataStartPos = invertedHeader.KeysOffset
		dataEndPos = invertedHeader.TombstoneOffset
	}

	seg := &segment{
		level: header.Level,
		// trim the .tmp suffix to make sure the naming rules for the files we
		// pre-compute later on still apply they will in turn be suffixed with
		// .tmp, but that is supposed to be the end of the file. if we didn't trim
		// the path here, we would end up with filenames like
		// segment.tmp.bloom.tmp, whereas we want to end up with segment.bloom.tmp
		path:                  strings.TrimSuffix(path, ".tmp"),
		contents:              contents,
		contentFile:           file,
		version:               header.Version,
		secondaryIndexCount:   header.SecondaryIndices,
		segmentStartPos:       header.IndexStart,
		segmentEndPos:         uint64(size),
		size:                  size,
		strategy:              header.Strategy,
		dataStartPos:          dataStartPos,
		dataEndPos:            dataEndPos,
		index:                 primaryDiskIndex,
		logger:                logger,
		useBloomFilter:        useBloomFilter,
		calcCountNetAdditions: calcCountNetAdditions,
		invertedHeader:        invertedHeader,
		invertedData:          &segmentInvertedData{},
	}

	if seg.secondaryIndexCount > 0 {
		seg.secondaryIndices = make([]diskIndex, seg.secondaryIndexCount)
		for i := range seg.secondaryIndices {
			secondary, err := header.SecondaryIndex(contents, uint16(i))
			if err != nil {
				return nil, errors.Wrapf(err, "get position for secondary index at %d", i)
			}
			seg.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
		}
	}

	if seg.useBloomFilter {
		files, err := seg.precomputeBloomFilters()
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}
	if seg.calcCountNetAdditions {
		files, err := seg.precomputeCountNetAdditions(updatedCountNetAdditions)
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}

	return out, nil
}
