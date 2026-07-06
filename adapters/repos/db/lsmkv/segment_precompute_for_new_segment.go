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
	"errors"
	"fmt"
)

func (sg *SegmentGroup) initAndPrecomputeNewSegment(path string) (*segment, error) {
	// It is now safe to hold the RLock on the maintenanceLock because we know
	// that the compaction routine will not try to obtain the Lock() until we
	// have released the flushVsCompactLock.
	segments, release, err := sg.getConsistentViewOfSegments()
	deferCountNetAdditions := false
	if errors.Is(err, ErrShuttingDown) {
		// A flush racing shutdown can no longer obtain a consistent view of the
		// lower segments (the refusal is what keeps shutdown's refcount wait
		// race-free). We still complete the flush against an empty baseline so the
		// switched-out memtable is cleared from b.flushing and Bucket.Shutdown does
		// not hang waiting for it — but that empty baseline makes the derived
		// count-net-additions overstated (every overwrite looks net-new), so we
		// must not persist it. deferCountNetAdditions skips writing the CNA; it is
		// recomputed against the real segment neighbors on the next open.
		segments, release = nil, func() {}
		deferCountNetAdditions = true
	} else if err != nil {
		return nil, err
	}
	defer release()

	segment, err := newSegment(path, sg.logger,
		sg.metrics, sg.makeExistsOn(segments),
		segmentConfig{
			mmapContents:             sg.mmapContents,
			useBloomFilter:           sg.useBloomFilter,
			calcCountNetAdditions:    sg.calcCountNetAdditions,
			overwriteDerived:         true,
			enableChecksumValidation: sg.enableChecksumValidation,
			MinMMapSize:              sg.MinMMapSize,
			allocChecker:             sg.allocChecker,
			writeMetadata:            sg.writeMetadata,
			deleteMarkerCounter:      sg.deleteMarkerCounter.Add(1),
			deferCountNetAdditions:   deferCountNetAdditions,
		})
	if err != nil {
		return nil, fmt.Errorf("init and pre-compute new segment %s: %w", path, err)
	}

	return segment, nil
}
