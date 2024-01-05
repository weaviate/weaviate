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
	"math"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func (sg *SegmentGroup) bestCompactionCandidatePair() []int {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// if true, the parent shard has indicated that it has
	// entered an immutable state. During this time, the
	// SegmentGroup should refrain from flushing until its
	// shard indicates otherwise
	if sg.isReadyOnly() {
		return nil
	}

	// Nothing to compact
	if len(sg.segments) < 2 {
		return nil
	}

	// first determine the lowest level with candidates
	levels := map[uint16]int{}
	lowestPairLevel := uint16(math.MaxUint16)
	lowestLevel := uint16(math.MaxUint16)
	lowestIndex := -1
	secondLowestIndex := -1
	pairExists := false

	for ind, seg := range sg.segments {
		levels[seg.level]++
		val := levels[seg.level]
		if val > 1 {
			if seg.level < lowestPairLevel {
				lowestPairLevel = seg.level
				pairExists = true
			}
		}

		if seg.level < lowestLevel {
			secondLowestIndex = lowestIndex
			lowestLevel = seg.level
			lowestIndex = ind
		}
	}

	if pairExists {
		// now pick any two segments which match the level
		var res []int

		for i, segment := range sg.segments {
			if len(res) >= 2 {
				break
			}

			if segment.level == lowestPairLevel {
				res = append(res, i)
			}
		}

		return res
	} else {
		if sg.compactLeftOverSegments {
			// Some segments exist, but none are of the same level
			// Merge the two lowest segments

			return []int{secondLowestIndex, lowestIndex}
		} else {
			// No segments of the same level exist, and we are not allowed to merge the lowest segments
			// This means we cannot compact.  Set COMPACT_LEFTOVER_SEGMENTS to true to compact the remaining segments
			return nil
		}
	}
}

// segmentAtPos retrieves the segment for the given position using a read-lock
func (sg *SegmentGroup) segmentAtPos(pos int) *segment {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return sg.segments[pos]
}

func (sg *SegmentGroup) compactOnce() (bool, error) {
	// Is it safe to only occasionally lock instead of the entire duration? Yes,
	// because other than compaction the only change to the segments array could
	// be an append because of a new flush cycle, so we do not need to guarantee
	// that the array contents stay stable over the duration of an entire
	// compaction. We do however need to protect against a read-while-write (race
	// condition) on the array. Thus any read from sg.segments need to protected
	pair := sg.bestCompactionCandidatePair()
	if pair == nil {
		// nothing to do
		return false, nil
	}

	path := fmt.Sprintf("%s.tmp", sg.segmentAtPos(pair[1]).path)
	f, err := os.Create(path)
	if err != nil {
		return false, err
	}

	scratchSpacePath := sg.segmentAtPos(pair[1]).path + "compaction.scratch.d"

	// the assumption is that the first element is older, and/or a higher level
	level := sg.segmentAtPos(pair[0]).level
	secondaryIndices := sg.segmentAtPos(pair[0]).secondaryIndexCount

	if level == sg.segmentAtPos(pair[1]).level {
		level = level + 1
	}

	strategy := sg.segmentAtPos(pair[0]).strategy
	cleanupTombstones := !sg.keepTombstones && pair[0] == 0

	pathLabel := "n/a"
	if sg.metrics != nil && !sg.metrics.groupClasses {
		pathLabel = sg.dir
	}
	switch strategy {

	// TODO: call metrics just once with variable strategy label

	case segmentindex.StrategyReplace:
		c := newCompactorReplace(f, sg.segmentAtPos(pair[0]).newCursor(),
			sg.segmentAtPos(pair[1]).newCursor(), level, secondaryIndices, scratchSpacePath, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionReplace.With(prometheus.Labels{"path": pathLabel}).Inc()
			defer sg.metrics.CompactionReplace.With(prometheus.Labels{"path": pathLabel}).Dec()
		}

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategySetCollection:
		c := newCompactorSetCollection(f, sg.segmentAtPos(pair[0]).newCollectionCursor(),
			sg.segmentAtPos(pair[1]).newCollectionCursor(), level, secondaryIndices,
			scratchSpacePath, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionSet.With(prometheus.Labels{"path": pathLabel}).Inc()
			defer sg.metrics.CompactionSet.With(prometheus.Labels{"path": pathLabel}).Dec()
		}

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyMapCollection:
		c := newCompactorMapCollection(f,
			sg.segmentAtPos(pair[0]).newCollectionCursorReusable(),
			sg.segmentAtPos(pair[1]).newCollectionCursorReusable(),
			level, secondaryIndices, scratchSpacePath, sg.mapRequiresSorting, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Inc()
			defer sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Dec()
		}

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyRoaringSet:
		leftSegment := sg.segmentAtPos(pair[0])
		rightSegment := sg.segmentAtPos(pair[1])

		leftCursor := leftSegment.newRoaringSetCursor()
		rightCursor := rightSegment.newRoaringSetCursor()

		c := roaringset.NewCompactor(f, leftCursor, rightCursor,
			level, scratchSpacePath, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionRoaringSet.With(prometheus.Labels{"path": pathLabel}).Set(1)
			defer sg.metrics.CompactionRoaringSet.With(prometheus.Labels{"path": pathLabel}).Set(0)
		}

		if err := c.Do(); err != nil {
			return false, err
		}

	default:
		return false, errors.Errorf("unrecognized strategy %v", strategy)
	}

	if err := f.Close(); err != nil {
		return false, errors.Wrap(err, "close compacted segment file")
	}

	if err := sg.replaceCompactedSegments(pair[0], pair[1], path); err != nil {
		return false, errors.Wrap(err, "replace compacted segments")
	}

	return true, nil
}

func (sg *SegmentGroup) replaceCompactedSegments(old1, old2 int,
	newPathTmp string,
) error {
	sg.maintenanceLock.RLock()
	updatedCountNetAdditions := sg.segments[old1].countNetAdditions +
		sg.segments[old2].countNetAdditions
	sg.maintenanceLock.RUnlock()

	precomputedFiles, err := preComputeSegmentMeta(newPathTmp,
		updatedCountNetAdditions, sg.logger,
		sg.useBloomFilter, sg.calcCountNetAdditions)
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

	seg, err := newSegment(newPath, sg.logger, sg.metrics, nil,
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions)
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

func (sg *SegmentGroup) compactIfLevelsMatch(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	sg.monitorSegments()

	compacted, err := sg.compactOnce()
	if err != nil {
		sg.logger.WithField("action", "lsm_compaction").
			WithField("path", sg.dir).
			WithError(err).
			Errorf("compaction failed")
	}

	if compacted {
		return true
	} else {
		sg.logger.WithField("action", "lsm_compaction").
			WithField("path", sg.dir).
			Trace("no segment eligible for compaction")
		return false
	}
}

func (sg *SegmentGroup) Len() int {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return len(sg.segments)
}

func (sg *SegmentGroup) monitorSegments() {
	if sg.metrics == nil || sg.metrics.groupClasses {
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
