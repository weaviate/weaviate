//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/sirupsen/logrus"
	"github.com/willf/bloom"
)

// preComputeSegmentMeta has no side-effects for an already running store. As a
// result this can be run without the need to obtain any locks. All files
// created will have a .tmp suffix so they don't interfere with existing
// segments that might have a similar name.
func preComputeSegmentMeta(path string, updatedCountNetAdditions int,
	logger logrus.FieldLogger,
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

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	content, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap file: %w", err)
	}

	header, err := parseSegmentHeader(bytes.NewReader(content[:SegmentHeaderSize]))
	if err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	switch header.strategy {
	case SegmentStrategyReplace, SegmentStrategySetCollection,
		SegmentStrategyMapCollection:
	default:
		return nil, fmt.Errorf("unsupported strategy in segment")
	}

	primaryIndex, err := header.PrimaryIndex(content)
	if err != nil {
		return nil, fmt.Errorf("extract primary index position: %w", err)
	}

	primaryDiskIndex := segmentindex.NewDiskTree(primaryIndex)

	ind := &segment{
		level: header.level,
		// trim the .tmp suffix to make sure the naming rules for the files we
		// pre-compute later on still apply they will in turn be suffixed with
		// .tmp, but that is supposed to be the end of the file. if we didn't trim
		// the path here, we would end up with filenames like
		// segment.tmp.bloom.tmp, whereas we want to end up with segment.bloom.tmp
		path:                strings.TrimSuffix(path, ".tmp"),
		contents:            content,
		version:             header.version,
		secondaryIndexCount: header.secondaryIndices,
		segmentStartPos:     header.indexStart,
		segmentEndPos:       uint64(len(content)),
		strategy:            header.strategy,
		dataStartPos:        SegmentHeaderSize, // fixed value that's the same for all strategies
		dataEndPos:          header.indexStart,
		index:               primaryDiskIndex,
		logger:              logger,
	}

	if ind.secondaryIndexCount > 0 {
		ind.secondaryIndices = make([]diskIndex, ind.secondaryIndexCount)
		ind.secondaryBloomFilters = make([]*bloom.BloomFilter, ind.secondaryIndexCount)
		for i := range ind.secondaryIndices {
			secondary, err := header.SecondaryIndex(content, uint16(i))
			if err != nil {
				return nil, errors.Wrapf(err, "get position for secondary index at %d", i)
			}

			ind.secondaryIndices[i] = segmentindex.NewDiskTree(secondary)
			if err := ind.precomputeSecondaryBloomFilter(i); err != nil {
				return nil, errors.Wrapf(err, "init bloom filter for secondary index at %d", i)
			}

			out = append(out, fmt.Sprintf("%s.tmp", ind.bloomFilterSecondaryPath(i)))
		}
	}

	if err := ind.precomputeBloomFilter(); err != nil {
		return nil, err
	}

	out = append(out, fmt.Sprintf("%s.tmp", ind.bloomFilterPath()))

	if ind.strategy != SegmentStrategyReplace {
		// only "replace" has count net additions, so we are done
		return out, nil
	}

	cnaPath := fmt.Sprintf("%s.tmp", ind.countNetPath())
	if err := storeCountNetOnDisk(cnaPath, updatedCountNetAdditions); err != nil {
		return nil, err
	}

	out = append(out, cnaPath)
	return out, nil
}
