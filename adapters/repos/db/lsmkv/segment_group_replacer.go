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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/diskio"
)

func newSegmentReplacer(sg *SegmentGroup, oldLeftPos, oldRightPos int, newSeg Segment) *segmentReplacer {
	return &segmentReplacer{
		sg:                   sg,
		replaceSingleSegment: oldLeftPos == oldRightPos,
		oldLeftPos:           oldLeftPos,
		oldRightPos:          oldRightPos,
		newSeg:               newSeg,
	}
}

type segmentReplacer struct {
	sg                        *SegmentGroup
	replaceSingleSegment      bool
	oldLeftPos, oldRightPos   int
	oldLeftPath, oldRightPath string
	newSeg                    Segment
}

const replaceSegmentWarnThreshold = 300 * time.Millisecond

// replaceCompactedSegmentsOnDisk performs the segment switch on disk without
// affecting the currently running app. Therefore it is non-blocking to the
// current application. The in-memory switch has to be done separately.
func (sr *segmentReplacer) switchOnDisk() (*segment, *segment, error) {
	var leftSegment, rightSegment Segment
	var leftSegID, rightSegID string
	var leftSegImpl, rightSegImpl *segment

	sr.sg.maintenanceLock.RLock()
	if !sr.replaceSingleSegment {
		leftSegment = sr.sg.segments[sr.oldLeftPos]
	}
	rightSegment = sr.sg.segments[sr.oldRightPos]
	sr.sg.maintenanceLock.RUnlock()

	if !sr.replaceSingleSegment {
		if err := leftSegment.markForDeletion(); err != nil {
			return nil, nil, errors.Wrap(err, "drop disk segment")
		}
		sr.oldLeftPath = leftSegment.getPath()
		leftSegID = segmentID(sr.oldLeftPath)
		leftSegImpl = leftSegment.getSegment()
	}

	if err := rightSegment.markForDeletion(); err != nil {
		return nil, nil, errors.Wrap(err, "drop disk segment")
	}
	sr.oldRightPath = rightSegment.getPath()
	rightSegID = segmentID(sr.oldRightPath)
	rightSegImpl = rightSegment.getSegment()

	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment
	newSeg := sr.newSeg.getSegment() // TODO: This prevents testing with a fake
	newPath, err := sr.stripTmpExtension(newSeg.path, leftSegID, rightSegID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "strip .tmp extension of new segment")
	}
	newSeg.path = newPath

	for i, pth := range newSeg.metaPaths {
		updated, err := sr.stripTmpExtension(pth, leftSegID, rightSegID)
		if err != nil {
			return nil, nil, errors.Wrap(err, "strip .tmp extension of new segment")
		}
		newSeg.metaPaths[i] = updated
	}

	err = diskio.Fsync(sr.sg.dir)
	if err != nil {
		return nil, nil, fmt.Errorf("fsync segment directory %s: %w", sr.sg.dir, err)
	}

	return leftSegImpl, rightSegImpl, nil
}

func (sr *segmentReplacer) switchInMemory() error {
	start := time.Now()

	sr.sg.maintenanceLock.Lock()
	if time.Since(start) > 100*time.Millisecond {
		sr.sg.logger.WithField("duration", time.Since(start)).
			Debug("compaction took more than 100ms to acquire maintenance lock")
	}
	defer sr.sg.maintenanceLock.Unlock()

	sr.sg.segments[sr.oldRightPos] = sr.newSeg
	if !sr.replaceSingleSegment {
		sr.sg.segments = append(sr.sg.segments[:sr.oldLeftPos], sr.sg.segments[sr.oldLeftPos+1:]...)
	}

	sr.observeReplaceDuration(start)
	return nil
}

func (sr *segmentReplacer) observeReplaceDuration(start time.Time) {
	// observe duration - warn if it took too long
	took := time.Since(start)
	fields := sr.sg.logger.WithFields(logrus.Fields{
		"action":        "lsm_replace_segments_blocking",
		"segment_index": sr.oldLeftPos,
		"path_left":     sr.oldLeftPath,
		"path_right":    sr.oldRightPath,
		"took":          took,
	})
	msg := fmt.Sprintf("replacing segments took %s", took)
	if took > replaceSegmentWarnThreshold {
		fields.Warn(msg)
	} else {
		fields.Debug(msg)
	}
}

func (sr *segmentReplacer) stripTmpExtension(oldPath, left, right string) (string, error) {
	ext := filepath.Ext(oldPath)
	if ext != ".tmp" {
		return "", errors.Errorf("segment %q did not have .tmp extension", oldPath)
	}
	newPath := oldPath[:len(oldPath)-len(ext)]

	newPath = strings.ReplaceAll(newPath, fmt.Sprintf("%s_%s", left, right), right)

	if err := os.Rename(oldPath, newPath); err != nil {
		return "", errors.Wrapf(err, "rename %q -> %q", oldPath, newPath)
	}

	return newPath, nil
}
