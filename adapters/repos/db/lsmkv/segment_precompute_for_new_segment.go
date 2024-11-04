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
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	newSegmentIndex := len(sg.segments)

	segment, err := newSegment(path, sg.logger,
		sg.metrics, sg.makeExistsOnLower(newSegmentIndex),
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, true)
	if err != nil {
		return nil, fmt.Errorf("init and pre-compute new segment %s: %w", path, err)
	}

	return segment, nil
}
