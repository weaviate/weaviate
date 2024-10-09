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

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

// findCompactionCandidates looks for pair of segments eligible for compaction
// into single segment.
// Segments use level property to mark how many times they were compacted.
//
// By default pair of segments with lowest matching levels is searched. If there are more
// than 2 segments of the same level, the oldest ones are picked.
// Behaviour of the method can be changed with 2 segment group settings:
// - maxSegmentSize (prevents segments being compacted into single segment of too large size)
// - compactLeftOverSegments (allows picking for compaction segments of not equal levels)
// Regardless of compaction settings, following constraints have to be met:
// - only consecutive segments can be merged to keep order of creations/updates/deletions of data stored
// - newer segments have levels lower or equal than levels of older segments (descendent order is kept)
//
// E.g. out of segments: s1(4), s2(3), s3(3), s4(2), s5(2), s6(2), s7(1), s8(0), s4+s5 will be
// selected for compaction first producing single segment s4s5(3), then s2+s3 producing s2s3(4),
// then s1+s2s3 producing s1s2s3(5). Until new segment s9(0) will be added to segment group,
// no next pair will be returned, as all segments will have different levels.
//
// If maxSegmentSize is set, estimated total size of compacted segment must not exceed given limit.
// Only pair of segments having sum of sizes <= maxSegmentSize can be returned by the method. If there
// exist older segments with same lavel as level of selected pair, level of compacted segment will
// not be changed to ensure no new segment have higher level than older ones. If there is no segment
// having same level as selected pair, new segment's level will be incremented.
// E.g. out of segments: s1(4), s2(3), s3(3), s4(2), s5(2), s6(2), s7(1), s8(0),
// when s4.size+s5.size > maxSegmentSize, but s5.size+s6.size <= maxSegmentSize, s5+s6 will be selected
// first for compaction producing segment s5s6(2) (note same level, due to older s2(2)).
// If s2.size+s3.size <= maxSegmentSize, then s2+s3 producing s2s3(4) (note incremented level,
// due to no older segment of level 3). If s1.size+s2s3.size <= maxSegmentSize s1s2s3(5) will be produced,
// if not, no other pair will be returned until new segments will be added to segment group.
//
// If compactLeftOverSegments is set, pair of segments of lowest levels, though similar in sizes
// can be returned if no pair of matching levels will be found. Segment sizes need to be close to each
// other to prevent merging large segments (GiB) with tiny one (KiB). Level of newly produced segment
// will be the same as level of larger(left) segment.
// maxSegmentSize ise respected for pair of leftover segments.
func (sg *SegmentGroup) findCompactionCandidates() (pair []int, level uint16) {
	// if true, the parent shard has indicated that it has
	// entered an immutable state. During this time, the
	// SegmentGroup should refrain from flushing until its
	// shard indicates otherwise
	if sg.isReadyOnly() {
		return nil, 0
	}

	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// Nothing to compact
	if len(sg.segments) < 2 {
		return nil, 0
	}

	matchingPairFound := false
	leftoverPairFound := false
	var matchingLeftId, leftoverLeftId int
	var matchingLevel, leftoverLevel uint16

	// as newest segments are prioritized, loop in reverse order
	for leftId := len(sg.segments) - 2; leftId >= 0; leftId-- {
		left, right := sg.segments[leftId], sg.segments[leftId+1]

		if left.level == right.level {
			if sg.compactionFitsSizeLimit(left, right) {
				// max size not exceeded
				matchingPairFound = true
				matchingLeftId = leftId
				matchingLevel = left.level + 1
			} else if matchingPairFound {
				// older segment of same level as pair's level exist.
				// keep unchanged level
				matchingLevel = left.level
			}
		} else {
			if matchingPairFound {
				// moving to segments of higher level, but matching pair is already found.
				// stop further search
				break
			}
			if sg.compactLeftOverSegments && !leftoverPairFound {
				// eftover segments enabled, none leftover pair found yet
				if sg.compactionFitsSizeLimit(left, right) && isSimilarSegmentSizes(left.size, right.size) {
					// max size not exceeded, segment sizes similar despite different levels
					leftoverPairFound = true
					leftoverLeftId = leftId
					leftoverLevel = left.level
				}
			}
		}
	}

	if matchingPairFound {
		return []int{matchingLeftId, matchingLeftId + 1}, matchingLevel
	}
	if leftoverPairFound {
		return []int{leftoverLeftId, leftoverLeftId + 1}, leftoverLevel
	}
	return nil, 0
}

func isSimilarSegmentSizes(leftSize, rightSize int64) bool {
	MiB := int64(1024 * 1024)
	GiB := 1024 * MiB

	threshold1 := 10 * MiB
	threshold2 := 100 * MiB
	threshold3 := GiB
	threshold4 := 10 * GiB

	factor2 := int64(10)
	factor3 := int64(5)
	factor4 := int64(3)
	factorDef := int64(2)

	// if both sizes less then 10 MiB
	if leftSize <= threshold1 && rightSize <= threshold1 {
		return true
	}

	lowerSize, higherSize := leftSize, rightSize
	if leftSize > rightSize {
		lowerSize, higherSize = rightSize, leftSize
	}

	// if higher size less than 100 MiB and not 10x bigger than lower
	if higherSize <= threshold2 && lowerSize*factor2 >= higherSize {
		return true
	}
	// if higher size less than 1 GiB and not 5x bigger than lower
	if higherSize <= threshold3 && lowerSize*factor3 >= higherSize {
		return true
	}
	// if higher size less than 10 GiB and not 3x bigger than lower
	if higherSize <= threshold4 && lowerSize*factor4 >= higherSize {
		return true
	}
	// if higher size not 2x bigger than lower
	return lowerSize*factorDef >= higherSize
}

// segmentAtPos retrieves the segment for the given position using a read-lock
func (sg *SegmentGroup) segmentAtPos(pos int) *segment {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return sg.segments[pos]
}

func segmentID(path string) string {
	filename := filepath.Base(path)
	return strings.TrimSuffix(strings.TrimPrefix(filename, "segment-"), ".db")
}

func (sg *SegmentGroup) compactOnce() (bool, error) {
	// Is it safe to only occasionally lock instead of the entire duration? Yes,
	// because other than compaction the only change to the segments array could
	// be an append because of a new flush cycle, so we do not need to guarantee
	// that the array contents stay stable over the duration of an entire
	// compaction. We do however need to protect against a read-while-write (race
	// condition) on the array. Thus any read from sg.segments need to protected

	pair, level := sg.findCompactionCandidates()
	if pair == nil {
		// nothing to do
		return false, nil
	}

	if sg.allocChecker != nil {
		// allocChecker is optional
		if err := sg.allocChecker.CheckAlloc(100 * 1024 * 1024); err != nil {
			// if we don't have at least 100MB to spare, don't start a compaction. A
			// compaction does not actually need a 100MB, but it will create garbage
			// that needs to be cleaned up. If we're so close to the memory limit, we
			// can increase stability by preventing anything that's not strictly
			// necessary. Compactions can simply resume when the cluster has been
			// scaled.
			sg.logger.WithFields(logrus.Fields{
				"action": "lsm_compaction",
				"event":  "compaction_skipped_oom",
				"path":   sg.dir,
			}).WithError(err).
				Warnf("skipping compaction due to memory pressure")

			return false, nil
		}
	}

	leftSegment := sg.segmentAtPos(pair[0])
	rightSegment := sg.segmentAtPos(pair[1])

	path := filepath.Join(sg.dir, "segment-"+segmentID(leftSegment.path)+"_"+segmentID(rightSegment.path)+".db.tmp")

	f, err := os.Create(path)
	if err != nil {
		return false, err
	}

	scratchSpacePath := rightSegment.path + "compaction.scratch.d"

	strategy := leftSegment.strategy
	secondaryIndices := leftSegment.secondaryIndexCount
	cleanupTombstones := !sg.keepTombstones && pair[0] == 0

	pathLabel := "n/a"
	if sg.metrics != nil && !sg.metrics.groupClasses {
		pathLabel = sg.dir
	}
	switch strategy {

	// TODO: call metrics just once with variable strategy label

	case segmentindex.StrategyReplace:
		c := newCompactorReplace(f, leftSegment.newCursor(),
			rightSegment.newCursor(), level, secondaryIndices, scratchSpacePath, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionReplace.With(prometheus.Labels{"path": pathLabel}).Inc()
			defer sg.metrics.CompactionReplace.With(prometheus.Labels{"path": pathLabel}).Dec()
		}

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategySetCollection:
		c := newCompactorSetCollection(f, leftSegment.newCollectionCursor(),
			rightSegment.newCollectionCursor(), level, secondaryIndices,
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
			leftSegment.newCollectionCursorReusable(),
			rightSegment.newCollectionCursorReusable(),
			level, secondaryIndices, scratchSpacePath, sg.mapRequiresSorting, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Inc()
			defer sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Dec()
		}

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyRoaringSet:
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

	case segmentindex.StrategyRoaringSetRange:
		leftCursor := leftSegment.newRoaringSetRangeCursor()
		rightCursor := rightSegment.newRoaringSetRangeCursor()

		c := roaringsetrange.NewCompactor(f, leftCursor, rightCursor,
			level, cleanupTombstones)

		if sg.metrics != nil {
			sg.metrics.CompactionRoaringSetRange.With(prometheus.Labels{"path": pathLabel}).Set(1)
			defer sg.metrics.CompactionRoaringSetRange.With(prometheus.Labels{"path": pathLabel}).Set(0)
		}

		if err := c.Do(); err != nil {
			return false, err
		}

	default:
		return false, errors.Errorf("unrecognized strategy %v", strategy)
	}

	if err := f.Sync(); err != nil {
		return false, errors.Wrap(err, "fsync compacted segment file")
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

	// WIP: we could add a random suffix to the tmp file to avoid conflicts
	precomputedFiles, err := preComputeSegmentMeta(newPathTmp,
		updatedCountNetAdditions, sg.logger,
		sg.useBloomFilter, sg.calcCountNetAdditions)
	if err != nil {
		return fmt.Errorf("precompute segment meta: %w", err)
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	leftSegment := sg.segments[old1]
	rightSegment := sg.segments[old2]

	if err := leftSegment.close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := rightSegment.close(); err != nil {
		return errors.Wrap(err, "close disk segment")
	}

	if err := leftSegment.drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	if err := rightSegment.drop(); err != nil {
		return errors.Wrap(err, "drop disk segment")
	}

	err = fsync(sg.dir)
	if err != nil {
		return fmt.Errorf("fsync segment directory %s: %w", sg.dir, err)
	}

	sg.segments[old1] = nil
	sg.segments[old2] = nil

	var newPath string
	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment
	for i, path := range precomputedFiles {
		updated, err := sg.stripTmpExtension(path, segmentID(leftSegment.path), segmentID(rightSegment.path))
		if err != nil {
			return errors.Wrap(err, "strip .tmp extension of new segment")
		}

		if i == 0 {
			// the first element in the list is the segment itself
			newPath = updated
		}
	}

	seg, err := newSegment(newPath, sg.logger, sg.metrics, nil,
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
	if err != nil {
		return errors.Wrap(err, "create new segment")
	}

	sg.segments[old2] = seg

	sg.segments = append(sg.segments[:old1], sg.segments[old1+1:]...)

	return nil
}

func (sg *SegmentGroup) stripTmpExtension(oldPath, left, right string) (string, error) {
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

func (sg *SegmentGroup) compactionFitsSizeLimit(left, right *segment) bool {
	if sg.maxSegmentSize == 0 {
		// no limit is set, always return true
		return true
	}

	totalSize := left.size + right.size
	return totalSize <= sg.maxSegmentSize
}
