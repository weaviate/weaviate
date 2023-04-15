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
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func (sg *SegmentGroup) eligibleForCompaction() bool {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// if true, the parent shard has indicated that it has
	// entered an immutable state. During this time, the
	// SegmentGroup should refrain from flushing until its
	// shard indicates otherwise
	if sg.isReadyOnly() {
		return false
	}

	// if there are at least two segments of the same level a regular compaction
	// can be performed

	levels := map[uint16]int{}

	for _, segment := range sg.segments {
		levels[segment.level]++
		if levels[segment.level] > 1 {
			return true
		}
	}

	return false
}

func (sg *SegmentGroup) bestCompactionCandidatePair() []int {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// first determine the lowest level with candidates
	levels := map[uint16]int{}

	for _, segment := range sg.segments {
		levels[segment.level]++
	}

	currLowestLevel := uint16(math.MaxUint16)
	found := false
	for level, count := range levels {
		if count < 2 {
			continue
		}

		if level < currLowestLevel {
			currLowestLevel = level
			found = true
		}
	}

	if !found {
		return nil
	}

	// now pick any two segements which match the level
	var res []int

	for i, segment := range sg.segments {
		if len(res) >= 2 {
			break
		}

		if segment.level == currLowestLevel {
			res = append(res, i)
		}
	}

	return res
}

// segmentAtPos retrieves the segment for the given position using a read-lock
func (sg *SegmentGroup) segmentAtPos(pos int) *segment {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return sg.segments[pos]
}

func (sg *SegmentGroup) compactOnce() error {
	// Is it safe to only occasionally lock instead of the entire duration? Yes,
	// because other than compaction the only change to the segments array could
	// be an append because of a new flush cycle, so we do not need to guarantee
	// that the array contents stay stable over the duration of an entire
	// compaction. We do however need to protect against a read-while-write (race
	// condition) on the array. Thus any read from sg.segments need to protected
	pair := sg.bestCompactionCandidatePair()
	if pair == nil {
		// nothing to do
		return nil
	}

	path := fmt.Sprintf("%s.tmp", sg.segmentAtPos(pair[1]).path)
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	scratchSpacePath := sg.segmentAtPos(pair[1]).path + "compaction.scratch.d"

	// the assumption is that both pairs are of the same level, so we can just
	// take either value. If we want to support asymmetric compaction, then we
	// might have to choose this value more intelligently
	level := sg.segmentAtPos(pair[0]).level
	secondaryIndices := sg.segmentAtPos(pair[0]).secondaryIndexCount

	strategy := sg.segmentAtPos(pair[0]).strategy

	switch strategy {

	// TODO: call metrics just once with variable strategy label

	case segmentindex.StrategyReplace:
		c := newCompactorReplace(f, sg.segmentAtPos(pair[0]).newCursor(),
			sg.segmentAtPos(pair[1]).newCursor(), level, secondaryIndices, scratchSpacePath)

		if sg.metrics != nil {
			sg.metrics.CompactionReplace.With(prometheus.Labels{"path": sg.dir}).Set(1)
			defer sg.metrics.CompactionReplace.With(prometheus.Labels{"path": sg.dir}).Set(0)
		}

		if err := c.do(); err != nil {
			return err
		}
	case segmentindex.StrategySetCollection:
		c := newCompactorSetCollection(f, sg.segmentAtPos(pair[0]).newCollectionCursor(),
			sg.segmentAtPos(pair[1]).newCollectionCursor(), level, secondaryIndices,
			scratchSpacePath)

		if sg.metrics != nil {
			sg.metrics.CompactionSet.With(prometheus.Labels{"path": sg.dir}).Set(1)
			defer sg.metrics.CompactionSet.With(prometheus.Labels{"path": sg.dir}).Set(0)
		}

		if err := c.do(); err != nil {
			return err
		}
	case segmentindex.StrategyMapCollection:
		c := newCompactorMapCollection(f,
			sg.segmentAtPos(pair[0]).newCollectionCursorReusable(),
			sg.segmentAtPos(pair[1]).newCollectionCursorReusable(),
			level, secondaryIndices, scratchSpacePath, sg.mapRequiresSorting)

		if sg.metrics != nil {
			sg.metrics.CompactionMap.With(prometheus.Labels{"path": sg.dir}).Set(1)
			defer sg.metrics.CompactionMap.With(prometheus.Labels{"path": sg.dir}).Set(0)
		}

		if err := c.do(); err != nil {
			return err
		}
	case segmentindex.StrategyRoaringSet:
		leftSegment := sg.segmentAtPos(pair[0])
		rightSegment := sg.segmentAtPos(pair[1])

		leftCursor := leftSegment.newRoaringSetCursor()
		rightCursor := rightSegment.newRoaringSetCursor()

		c := roaringset.NewCompactor(f, leftCursor, rightCursor,
			level, scratchSpacePath)

		if sg.metrics != nil {
			sg.metrics.CompactionRoaringSet.With(prometheus.Labels{"path": sg.dir}).Set(1)
			defer sg.metrics.CompactionRoaringSet.With(prometheus.Labels{"path": sg.dir}).Set(0)
		}

		if err := c.Do(); err != nil {
			return err
		}

	default:
		return errors.Errorf("unrecognized strategy %v", strategy)
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "close compacted segment file")
	}

	if err := sg.replaceCompactedSegments(pair[0], pair[1], path); err != nil {
		return errors.Wrap(err, "replace compacted segments")
	}

	return nil
}

func (sg *SegmentGroup) replaceCompactedSegments(old1, old2 int,
	newPathTmp string,
) error {
	sg.maintenanceLock.RLock()
	updatedCountNetAdditions := sg.segments[old1].countNetAdditions +
		sg.segments[old2].countNetAdditions
	sg.maintenanceLock.RUnlock()

	precomputedFiles, err := preComputeSegmentMeta(newPathTmp,
		updatedCountNetAdditions, sg.logger)
	if err != nil {
		return fmt.Errorf("precompute segment meta: %w", err)
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	if err := sg.segments[old1].close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := sg.segments[old2].close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := sg.segments[old1].drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	if err := sg.segments[old2].drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	sg.segments[old1] = nil
	sg.segments[old2] = nil

	var newPath string
	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment
	for i, path := range precomputedFiles {
		updated, err := sg.stripTmpExtension(path)
		if err != nil {
			return errors.Wrap(err, "strip .tmp extension of new segment")
		}

		if i == 0 {
			// the first element in the list is the segment itself
			newPath = updated
		}
	}

	seg, err := newSegment(newPath, sg.logger, sg.metrics, nil)
	if err != nil {
		return errors.Wrap(err, "create new segment")
	}

	sg.segments[old2] = seg

	sg.segments = append(sg.segments[:old1], sg.segments[old1+1:]...)

	return nil
}

func (sg *SegmentGroup) stripTmpExtension(oldPath string) (string, error) {
	ext := filepath.Ext(oldPath)
	if ext != ".tmp" {
		return "", errors.Errorf("segment %q did not have .tmp extension", oldPath)
	}
	newPath := oldPath[:len(oldPath)-len(ext)]

	if err := os.Rename(oldPath, newPath); err != nil {
		return "", errors.Wrapf(err, "rename %q -> %q", oldPath, newPath)
	}

	return newPath, nil
}

func (sg *SegmentGroup) compactIfLevelsMatch(shouldBreak cyclemanager.ShouldBreakFunc) bool {
	sg.monitorSegments()

	if sg.eligibleForCompaction() {
		if err := sg.compactOnce(); err != nil {
			sg.logger.WithField("action", "lsm_compaction").
				WithField("path", sg.dir).
				WithError(err).
				Errorf("compaction failed")
		}
		return true
	}

	sg.logger.WithField("action", "lsm_compaction").
		WithField("path", sg.dir).
		Trace("no segment eligible for compaction")
	return false
}

func (sg *SegmentGroup) Len() int {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return len(sg.segments)
}

func (sg *SegmentGroup) monitorSegments() {
	if sg.metrics == nil {
		return
	}

	sg.metrics.ActiveSegments.With(prometheus.Labels{
		"strategy": sg.strategy,
		"path":     sg.dir,
	}).Set(float64(sg.Len()))

	stats := sg.segmentLevelStats()
	stats.fillMissingLevels()
	stats.report(sg.metrics, sg.strategy, sg.dir)
}

type segmentLevelStats struct {
	indexes  map[uint16]int
	payloads map[uint16]int
	count    map[uint16]int
}

func newSegmentLevelStats() segmentLevelStats {
	return segmentLevelStats{
		indexes:  map[uint16]int{},
		payloads: map[uint16]int{},
		count:    map[uint16]int{},
	}
}

func (sg *SegmentGroup) segmentLevelStats() segmentLevelStats {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	stats := newSegmentLevelStats()

	for _, seg := range sg.segments {
		stats.count[seg.level]++

		cur := stats.indexes[seg.level]
		cur += seg.index.Size()
		stats.indexes[seg.level] = cur

		cur = stats.payloads[seg.level]
		cur += seg.PayloadSize()
		stats.payloads[seg.level] = cur
	}

	return stats
}

// fill missing levels
//
// Imagine we had exactly two segments of level 4 before, and there were just
// compacted to single segment of level 5. As a result, there should be no
// more segments of level 4. However, our current logic only loops over
// existing segments. As a result, we need to check what the highest level
// is, then for every level lower than the highest check if we are missing
// data. If yes, we need to explicitly set the gauges to 0.
func (s *segmentLevelStats) fillMissingLevels() {
	maxLevel := uint16(0)
	for level := range s.count {
		if level > maxLevel {
			maxLevel = level
		}
	}

	if maxLevel > 0 {
		for level := uint16(0); level < maxLevel; level++ {
			if _, ok := s.count[level]; ok {
				continue
			}

			// there is no entry for this level, we must explicitly set it to 0
			s.count[level] = 0
			s.indexes[level] = 0
			s.payloads[level] = 0
		}
	}
}

func (s *segmentLevelStats) report(metrics *Metrics,
	strategy, dir string,
) {
	for level, size := range s.indexes {
		metrics.SegmentSize.With(prometheus.Labels{
			"strategy": strategy,
			"unit":     "index",
			"level":    fmt.Sprint(level),
			"path":     dir,
		}).Set(float64(size))
	}

	for level, size := range s.payloads {
		metrics.SegmentSize.With(prometheus.Labels{
			"strategy": strategy,
			"unit":     "payload",
			"level":    fmt.Sprint(level),
			"path":     dir,
		}).Set(float64(size))
	}

	for level, count := range s.count {
		metrics.SegmentCount.With(prometheus.Labels{
			"strategy": strategy,
			"level":    fmt.Sprint(level),
			"path":     dir,
		}).Set(float64(count))
	}
}
