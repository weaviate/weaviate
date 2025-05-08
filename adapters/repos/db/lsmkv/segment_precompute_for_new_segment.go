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

import "fmt"

func (sg *SegmentGroup) initAndPrecomputeNewSegment(path string) (*segment, error) {
	// During this entire operation we need to make sure that no compaction
	// happens, otherwise we get a race between the existsOnLower func and
	// the meta count init.
	//
	// Normal operations (user CRUD) are fine.

	// We can't simply hold an RLock on the maintenanceLock without coordinating
	// with potential Lock() callers. Otherwise if we hold the RLock for minutes
	// and someone else calls Lock() we will deadlock.
	//
	// The only known caller of Lock() is the compaction routine, so we can
	// synchronize with it by holding the flushVsCompactLock.
	sg.flushVsCompactLock.Lock()
	defer sg.flushVsCompactLock.Unlock()

	// It is now safe to hold the RLock on the maintenanceLock because we know
	// that the compaction routine will not try to obtain the Lock() until we
	// have released the flushVsCompactLock.
	segments, release := sg.getAndLockSegments()
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
		})
	if err != nil {
		return nil, fmt.Errorf("init and pre-compute new segment %s: %w", path, err)
	}

	return segment, nil
}
