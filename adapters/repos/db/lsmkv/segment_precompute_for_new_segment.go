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

package lsmkv

import "fmt"

func (sg *SegmentGroup) initAndPrecomputeNewSegment(path string) (*segment, error) {
	// It is now safe to hold the RLock on the maintenanceLock because we know
	// that the compaction routine will not try to obtain the Lock() until we
	// have released the flushVsCompactLock.
	segments, release := sg.getConsistentViewOfSegments()
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
		})
	if err != nil {
		return nil, fmt.Errorf("init and pre-compute new segment %s: %w", path, err)
	}

	return segment, nil
}
