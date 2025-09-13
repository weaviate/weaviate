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
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/diskio"
)

func newSegmentReplacer(sg *SegmentGroup, old1, old2 int, newSeg Segment) *segmentReplacer {
	return &segmentReplacer{
		sg:     sg,
		old1:   old1,
		old2:   old2,
		newSeg: newSeg,
	}
}

type segmentReplacer struct {
	sg         *SegmentGroup
	old1, old2 int
	newSeg     Segment
}

const replaceSegmentWarnThreshold = 300 * time.Millisecond

// replaceCompactedSegmentsOnDisk performs the segment switch on disk without
// affecting the currently running app. Therefore it is non-blocking to the
// current application. The in-memory switch has to be done separately.
func (sr *segmentReplacer) switchOnDisk() (*segment, *segment, error) {
	sr.sg.maintenanceLock.RLock()
	leftSegment := sr.sg.segments[sr.old1]
	rightSegment := sr.sg.segments[sr.old2]
	sr.sg.maintenanceLock.RUnlock()

	newSeg := sr.newSeg.getSegment() // TODO: This prevents testing with a fake

	if err := leftSegment.markForDeletion(); err != nil {
		return nil, nil, errors.Wrap(err, "drop disk segment")
	}

	if err := rightSegment.markForDeletion(); err != nil {
		return nil, nil, errors.Wrap(err, "drop disk segment")
	}

	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment

	newPath, err := sr.sg.stripTmpExtension(newSeg.path, segmentID(leftSegment.getPath()), segmentID(rightSegment.getPath()))
	if err != nil {
		return nil, nil, errors.Wrap(err, "strip .tmp extension of new segment")
	}
	newSeg.path = newPath

	for i, pth := range newSeg.metaPaths {
		updated, err := sr.sg.stripTmpExtension(pth, segmentID(leftSegment.getPath()), segmentID(rightSegment.getPath()))
		if err != nil {
			return nil, nil, errors.Wrap(err, "strip .tmp extension of new segment")
		}
		newSeg.metaPaths[i] = updated
	}

	err = diskio.Fsync(sr.sg.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("fsync segment directory %s: %w", sr.sg.dir, err)
	}

	return leftSegment.getSegment(), rightSegment.getSegment(), nil
}

func (sr *segmentReplacer) switchInMemory(oldLeft, oldRight Segment) error {
	// TODO: Is this lock still needed?
	// We need a maintenanceLock.Lock() to switch segments, however, we can't
	// simply call Lock(). Due to the write-preferring nature of the RWMutex this
	// would mean that if any RLock() holder still holds the lock, all future
	// RLock() holders would be blocked until we release the Lock() again.
	//
	// Typical RLock() holders are user operations that are short-lived. However,
	// the flush routine also requires an RLock() and could potentially hold it
	// for minutes. This is problematic, so we need to synchronize with the flush
	// routine by obtaining the flushVsCompactLock.
	//
	// This gives us the guarantee that – until we have released the
	// flushVsCompactLock – no flush routine will try to obtain a long-lived
	// maintenanceLock.RLock().
	sr.sg.flushVsCompactLock.Lock()
	defer sr.sg.flushVsCompactLock.Unlock()

	start := time.Now()
	beforeMaintenanceLock := time.Now()
	sr.sg.maintenanceLock.Lock()
	if time.Since(beforeMaintenanceLock) > 100*time.Millisecond {
		sr.sg.logger.WithField("duration", time.Since(beforeMaintenanceLock)).
			Debug("compaction took more than 100ms to acquire maintenance lock")
	}
	defer sr.sg.maintenanceLock.Unlock()
	sr.sg.segments[sr.old1] = nil
	sr.sg.segments[sr.old2] = nil

	sr.sg.segments[sr.old2] = sr.newSeg

	sr.sg.segments = append(sr.sg.segments[:sr.old1], sr.sg.segments[sr.old1+1:]...)

	sr.observeReplaceCompactedDuration(start, sr.old1, oldLeft, oldRight)
	return nil
}

func (sr *segmentReplacer) observeReplaceCompactedDuration(
	start time.Time, segmentIdx int, left, right Segment,
) {
	// observe duration - warn if it took too long
	took := time.Since(start)
	fields := sr.sg.logger.WithFields(logrus.Fields{
		"action":        "lsm_replace_compacted_segments_blocking",
		"segment_index": segmentIdx,
		"path_left":     left.getPath(),
		"path_right":    right.getPath(),
		"took":          took,
	})
	msg := fmt.Sprintf("replacing compacted segments took %s", took)
	if took > replaceSegmentWarnThreshold {
		fields.Warn(msg)
	} else {
		fields.Debug(msg)
	}
}
