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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/config"
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
		sg.logger.WithField("action", "lsm_compaction").
			WithField("path", sg.dir).
			Debug("compaction halted due to shard READONLY status")
		return nil, 0
	}

	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// Nothing to compact
	if len(sg.segments) < 2 {
		return nil, 0
	}

	// Due to sg.segments array being sometimes build in the incorrect order (not by ascending
	// timestamps), segments' levels due to ongoing compactions might have gotten mixed up resulting
	// in some segments never being picked up for further compactions.
	// Introduced change fixes segment's levels by selecting pairs with unordered levels and computing
	// new level for compacted segment to restore descending order of segments' levels eventually.
	// Unordered segments are compacted only when ordered pair was not found (fixed in lazy manner).
	// Segment's size limit is IGNORED when unordered segments are merged.
	//
	// For given segments and their levels
	// 		s20 s19 s18 s17 s16 s15 s14 s13 s12 s11 s10 s09 s08 s07 s06 s05 s04 s03 s02 s01
	//		 06  07  09  08  07  06  05  04  03  05  04  03  02  07  06  12  11  10  09  08
	// ordered segments are s05-s01, unordered are s20-s06.
	// lastOrderedPos=15 (s05), lastOrderedLvl=12 (s05)
	//
	// More examples of candidates selection for further compactions can be found in
	// TestSegmentGroup_CompactionPairToFixLevelsOrder
	isUnordered := false
	var lPos, lastOrderedPos int
	var lLvl, rLvl, lastOrderedLvl uint16
	var lSeg, rSeg Segment

	matchingPairFound := false
	leftoverPairFound := false
	var matchingPos, leftoverPos int
	var matchingLvl, leftoverLvl uint16

	// as newest segments are prioritized, loop in reverse order
	for i := len(sg.segments) - 2; i >= 0; i-- {
		lPos = i
		lSeg, rSeg = sg.segments[lPos], sg.segments[lPos+1]
		lLvl, rLvl = lSeg.getLevel(), rSeg.getLevel()

		// unordered levels discovered, stop further search and move on
		// to fixing segment levels if no matching/leftover pair was found so far
		if lLvl < rLvl {
			isUnordered = true
			lastOrderedLvl = rLvl
			lastOrderedPos = lPos + 1
			break
		}

		if lLvl == rLvl {
			if sg.compactionFitsSizeLimit(lSeg, rSeg) {
				// max size not exceeded
				matchingPairFound = true
				matchingPos = lPos
				matchingLvl = lLvl + 1
				// this is for bucket migrations with re-ingestion, specifically
				// for the new incoming data (ingest) bucket.
				// we don't want to change the level of the segments on ingest data,
				// so that, when we copy the segments to the bucket with the reingested
				// data, the levels are all still at zero, and they can be compacted
				// with the existing re-ingested segments.
				if sg.keepLevelCompaction {
					matchingLvl = lLvl
				}
			} else if matchingPairFound {
				// older segment of same level as pair's level exist.
				// keep unchanged level
				matchingLvl = lLvl
			}
		} else {
			if matchingPairFound {
				// moving to segments of higher level, but matching pair is already found.
				// stop further search
				break
			}
			if sg.compactLeftOverSegments && !leftoverPairFound {
				// leftover segments enabled, none leftover pair found yet
				if sg.compactionFitsSizeLimit(lSeg, rSeg) && isSimilarSegmentSizes(lSeg.Size(), rSeg.Size()) {
					// max size not exceeded, segment sizes similar despite different levels
					leftoverPairFound = true
					leftoverPos = lPos
					leftoverLvl = lLvl
				}
			}
		}
	}

	if matchingPairFound {
		return []int{matchingPos, matchingPos + 1}, matchingLvl
	}
	if leftoverPairFound {
		return []int{leftoverPos, leftoverPos + 1}, leftoverLvl
	}

	// no pair found, but unordered levels discovered
	if isUnordered {
		// just one unordered segment (left). merge with right one, keep right one's level
		if lastOrderedPos == 1 {
			return []int{0, 1}, lastOrderedLvl
		}

		var candidatePos int
		var candidateLvl uint16
		candidatePairFound := false
		for i := lastOrderedPos - 2; i >= 0; i-- {
			lPos = i
			lLvl, rLvl = sg.segments[lPos].getLevel(), sg.segments[lPos+1].getLevel()

			if lLvl > rLvl {
				if !candidatePairFound {
					// if left segment is the 1st one (right being 2nd), take max ordered level new compacted one
					// to match ordered segments, otherwise keep left+right ones' level
					if lPos == 0 && lastOrderedLvl > lLvl {
						return []int{0, 1}, lastOrderedLvl
					}
					return []int{lPos, lPos + 1}, lLvl
				}
				break
			}
			if lLvl == rLvl {
				candidatePos = lPos
				candidateLvl = lLvl + 1
				candidatePairFound = true
			}
		}
		if candidatePairFound {
			return []int{candidatePos, candidatePos + 1}, candidateLvl
		}
		// no pair was found, meaning left level < right level
		// if only 2 segments are unordered, new level should match last one ordered
		if lastOrderedPos == 2 {
			return []int{0, 1}, lastOrderedLvl
		}
		// if more segments are unordered, get right level as new one
		return []int{0, 1}, rLvl
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

func segmentID(path string) string {
	filename := filepath.Base(path)
	filename, _, _ = strings.Cut(filename, ".")
	return strings.TrimPrefix(filename, "segment-")
}

// segmentExtraInfo returns the extra filename components for a segment,
// encoding the level, strategy, and optionally the secondary-tombstone marker.
//
// The ".d1" suffix stands for "delete format version 1": it indicates that
// every tombstone in the segment also carries a secondary-key tombstone, so
// GetBySecondary can skip the expensive primary-key existence recheck. The
// marker is only meaningful for buckets with secondary indices (e.g. the
// objects bucket). Segments without the marker (implicitly "d0") rely on the
// existsWithConsistentView fallback to detect primary-only tombstones.
func segmentExtraInfo(level uint16, strategy segmentindex.Strategy, hasSecondaryTombstones bool) string {
	if hasSecondaryTombstones {
		return fmt.Sprintf(".l%d.s%d.d1", level, strategy)
	}
	return fmt.Sprintf(".l%d.s%d", level, strategy)
}

func (sg *SegmentGroup) compactOnce() (compacted bool, err error) {
	// Is it safe to only occasionally lock instead of the entire duration? Yes,
	// because other than compaction the only change to the segments array could
	// be an append because of a new flush cycle, so we do not need to guarantee
	// that the array contents stay stable over the duration of an entire
	// compaction. We do however need to protect against a read-while-write (race
	// condition) on the array. Thus any read from sg.segments need to protected
	start := time.Now()

	sg.metrics.IncCompactionCount(sg.strategy)
	sg.metrics.IncCompactionInProgress(sg.strategy)

	defer func() {
		sg.metrics.DecCompactionInProgress(sg.strategy)

		if err != nil {
			sg.metrics.IncCompactionFailureCount(sg.strategy)
			return
		}

		if !compacted {
			sg.metrics.IncCompactionNoOp(sg.strategy)
			return
		}

		sg.metrics.ObserveCompactionDuration(sg.strategy, time.Since(start))
	}()

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

	var left, right Segment
	func() {
		sg.maintenanceLock.RLock()
		defer sg.maintenanceLock.RUnlock()

		left = sg.segments[pair[0]]
		right = sg.segments[pair[1]]
	}()

	strategy := left.getStrategy()
	leftPath := left.getPath()
	rightPath := right.getPath()

	secondaryIndices := left.getSecondaryIndexCount()

	var filename string
	if sg.writeSegmentInfoIntoFileName {
		// The .d1 marker on a segment means secondary lookups can skip
		// the existsWithConsistentView fallback. It only applies to
		// buckets with secondary indices (e.g. the objects bucket).
		// The output carries .d1 when either input already has it.
		//
		// d0+d0 → d0: both inputs are pre-.d1 segments. Their tombstones
		//   may use the old-style empty secondary key. The output stays d0,
		//   so secondary lookups on it will use existsWithConsistentView.
		//
		// d0+d1 → d1: the compactor is a pure merge sort — it passes
		//   through data verbatim. Old-style tombstones from the d0 input
		//   can end up in the d1 output. This is safe because any stale
		//   live entry shadowed by such a tombstone is in an even older
		//   segment, which is always d0 (the .d1 boundary only moves left
		//   over time, so segments older than this output are d0). Secondary
		//   lookups on d0 segments use existsWithConsistentView, which
		//   finds the tombstone via primary-key lookup regardless of
		//   secondary key style.
		//
		// d1+d1 → d1: both inputs have new-style tombstones. Trivially
		//   correct.
		//
		// Note: the objects bucket has keepTombstones=true, so old-style
		// tombstones are never cleaned up and will persist through
		// compaction into d1 segments. This is safe for the reason above.
		secTombstones := left.hasSecondaryTombstones() || right.hasSecondaryTombstones()
		filename = "segment-" + segmentID(leftPath) + "_" + segmentID(rightPath) + segmentExtraInfo(level, strategy, secTombstones) + ".db.tmp"
	} else {
		filename = "segment-" + segmentID(leftPath) + "_" + segmentID(rightPath) + ".db.tmp"
	}
	path := filepath.Join(sg.dir, filename)

	f, err := os.Create(path)
	if err != nil {
		return false, err
	}

	scratchSpacePath := rightPath + "compaction.scratch.d"
	cleanupTombstones := !sg.keepTombstones && pair[0] == 0
	maxNewFileSize := left.Size() + right.Size()

	switch strategy {

	// TODO: call metrics just once with variable strategy label

	case segmentindex.StrategyReplace:
		c := newCompactorReplace(f, left.newCursor(), right.newCursor(),
			level, secondaryIndices, scratchSpacePath, cleanupTombstones,
			sg.enableChecksumValidation, maxNewFileSize, sg.allocChecker)

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategySetCollection:
		c := newCompactorSetCollection(f, left.newCollectionCursorReusable(), right.newCollectionCursorReusable(),
			level, secondaryIndices, scratchSpacePath, cleanupTombstones,
			sg.enableChecksumValidation, maxNewFileSize, sg.allocChecker, sg.shouldSkipKey)

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyMapCollection:
		c := newCompactorMapCollection(f, left.newCollectionCursorReusable(), right.newCollectionCursorReusable(),
			level, secondaryIndices, scratchSpacePath, sg.mapRequiresSorting, cleanupTombstones,
			sg.enableChecksumValidation, maxNewFileSize, sg.allocChecker)

		if err := c.do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyRoaringSet:
		c := roaringset.NewCompactor(f, left.newRoaringSetCursor(), right.newRoaringSetCursor(),
			level, scratchSpacePath, cleanupTombstones,
			sg.enableChecksumValidation, maxNewFileSize, sg.allocChecker)

		if err := c.Do(); err != nil {
			return false, err
		}

	case segmentindex.StrategyRoaringSetRange:
		c := roaringsetrange.NewCompactor(f, left.newRoaringSetRangeCursor(), right.newRoaringSetRangeCursor(),
			level, cleanupTombstones, sg.enableChecksumValidation, maxNewFileSize)

		if err := c.Do(); err != nil {
			return false, err
		}
	case segmentindex.StrategyInverted:
		avgPropLen, _ := sg.GetAveragePropertyLength()
		b := float64(config.DefaultBM25b)
		k1 := float64(config.DefaultBM25k1)
		if sg.bm25config != nil {
			b = sg.bm25config.B
			k1 = sg.bm25config.K1
		}

		c := newCompactorInverted(f, left.newInvertedCursorReusable(), right.newInvertedCursorReusable(),
			level, secondaryIndices, scratchSpacePath, cleanupTombstones,
			k1, b, avgPropLen, maxNewFileSize, sg.allocChecker, sg.enableChecksumValidation)

		if err := c.do(); err != nil {
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

	newSegment, err := sg.preinitializeNewSegment(path, pair[0], pair[1])
	if err != nil {
		return false, errors.Wrap(err, "preinitialize new segment")
	}

	replacer := newSegmentReplacer(sg, pair[0], pair[1], newSegment)
	oldLeft, oldRight, err := replacer.switchOnDisk()
	if err != nil {
		return false, fmt.Errorf("replace compacted segments on disk: %w", err)
	}

	if err := replacer.switchInMemory(); err != nil {
		return false, fmt.Errorf("replace compacted segments (blocking): %w", err)
	}

	sg.addSegmentsToAwaitingDrop(oldLeft, oldRight)

	sg.metrics.DecSegmentTotalByStrategy(sg.strategy)
	sg.metrics.ObserveSegmentSize(sg.strategy, newSegment.Size())

	return true, nil
}

func (sg *SegmentGroup) preinitializeNewSegment(newPathTmp string, oldPos ...int) (*segment, error) {
	// WIP: we could add a random suffix to the tmp file to avoid conflicts

	// as a guardrail validate that the segment is considered a .tmp segment.
	// This way we can be sure that we're not accidentally operating on a live
	// segment as the segment group completely ignores .tmp segment files
	if !strings.HasSuffix(newPathTmp, ".tmp") {
		return nil, fmt.Errorf("pre computing a segment expects a .tmp segment path")
	}

	updatedCountNetAdditions := 0
	if len(oldPos) > 0 {
		func() {
			sg.maintenanceLock.RLock()
			defer sg.maintenanceLock.RUnlock()

			for i := range oldPos {
				updatedCountNetAdditions += sg.segments[oldPos[i]].getCountNetAdditions()
			}
		}()
	}

	seg, err := newSegment(newPathTmp, sg.logger, sg.metrics, nil,
		segmentConfig{
			mmapContents:                 sg.mmapContents,
			useBloomFilter:               sg.useBloomFilter,
			calcCountNetAdditions:        sg.calcCountNetAdditions,
			overwriteDerived:             true,
			enableChecksumValidation:     sg.enableChecksumValidation,
			MinMMapSize:                  sg.MinMMapSize,
			allocChecker:                 sg.allocChecker,
			precomputedCountNetAdditions: &updatedCountNetAdditions,
			fileList:                     make(map[string]int64), // empty to not check if bloom/cna files already exist
			writeMetadata:                sg.writeMetadata,
			deleteMarkerCounter:          sg.deleteMarkerCounter.Add(1),
		})
	if err != nil {
		return nil, fmt.Errorf("initialize new segment: %w", err)
	}

	return seg, nil
}

func (sg *SegmentGroup) waitForReferenceCountToReachZero(segments ...Segment) {
	if len(segments) == 0 {
		return
	}

	const (
		tickerInterval = 100 * time.Millisecond
		warnThreshold  = 10 * time.Second
		warnInterval   = 10 * time.Second
	)

	start := time.Now()
	var lastWarn time.Time
	t := time.NewTicker(tickerInterval)
	for {
		sg.segmentRefCounterLock.Lock()

		allZero := true
		var pos, count int
		for i, seg := range segments {
			if refs := seg.getRefs(); refs != 0 {
				allZero = false
				pos = i
				count = refs
				break
			}
		}

		sg.segmentRefCounterLock.Unlock()

		if allZero {
			return
		}

		now := time.Now()
		if sinceStart := now.Sub(start); sinceStart >= warnThreshold {
			if lastWarn.IsZero() || now.Sub(lastWarn) >= warnInterval {
				sg.logger.WithFields(logrus.Fields{
					"action":           "lsm_compaction_wait_refcount",
					"path":             sg.dir,
					"segment_position": pos,
					"refcount":         count,
					"total_wait":       sinceStart,
				}).Warnf("still waiting for segments to reach refcount=0 (waited %.2fs so far)", sinceStart.Seconds())
				lastWarn = now
			}
		}

		<-t.C
	}
}

// Compacted or cleaned up segments are pushed to the array to be dropped later.
// Immediate drop may not be possible due to files being in use by ongoing reads.
func (sg *SegmentGroup) addSegmentsToAwaitingDrop(segments ...Segment) {
	// as compaction / cleanup can not run in parallel, concurrent access to segmentsAwaitingDrop
	// is not possible, therefore no synchornization is used
	now := time.Now()
	for _, seg := range segments {
		sg.segmentsAwaitingDrop = append(sg.segmentsAwaitingDrop, struct {
			seg  Segment
			time time.Time
		}{seg: seg, time: now})
	}
}

// Compacted or cleaned up segments are closed and dropped only if there are
// no active references (segments are not read at the moment).
// Drop method should be called periodically to handle files that are no longer in use / read.
// As segments are already replaced by new ones, reference counter is expected only to decrease over time.
// Once it reaches 0, segment can be safely closed and its files removed from disk.
func (sg *SegmentGroup) dropSegmentsAwaiting() (dropped int, err error) {
	if len(sg.segmentsAwaitingDrop) == 0 {
		return 0, nil
	}

	warnThreshold := 10 * time.Second
	warnInterval := 10 * time.Second

	now := time.Now()
	skipWarning := sg.segmentsAwaitingLastWarn.Add(warnInterval).After(now)
	waitingCount := 0
	var maxWaitingDuration time.Duration
	var maxWaitingSegment Segment
	var maxWaitingRefs int

	toDrop := []Segment{}
	func() {
		sg.segmentRefCounterLock.Lock()
		defer sg.segmentRefCounterLock.Unlock()

		i := 0
		for _, st := range sg.segmentsAwaitingDrop {
			if refs := st.seg.getRefs(); refs == 0 {
				toDrop = append(toDrop, st.seg)
			} else {
				sg.segmentsAwaitingDrop[i] = st
				i++

				if !skipWarning {
					if d := now.Sub(st.time); d >= warnThreshold {
						waitingCount++
						if d > maxWaitingDuration {
							maxWaitingDuration = d
							maxWaitingSegment = st.seg
							maxWaitingRefs = refs
						}
					}
				}
			}
		}
		sg.segmentsAwaitingDrop = sg.segmentsAwaitingDrop[:i]
	}()

	if !skipWarning && maxWaitingDuration > 0 {
		sg.segmentsAwaitingLastWarn = now
		sg.logger.WithFields(logrus.Fields{
			"action":         "lsm_drop_segments_awaiting",
			"path":           sg.dir,
			"segment":        filepath.Base(maxWaitingSegment.getPath()),
			"refcount":       maxWaitingRefs,
			"wait":           maxWaitingDuration.String(),
			"total_segments": waitingCount,
		}).Warnf("skipping segments with refcounts (longest waiting %.2fs so far)", maxWaitingDuration.Seconds())
	}

	if len(toDrop) == 0 {
		return 0, nil
	}

	ec := errorcompounder.New()
	for _, seg := range toDrop {
		if err := seg.close(); err != nil {
			ec.Add(fmt.Errorf("close segment: %w", err))
			continue
		}
		if err := seg.dropMarked(); err != nil {
			ec.Add(fmt.Errorf("drop marked: %w", err))
			continue
		}
		dropped++
	}
	return dropped, ec.ToError()
}

func (sg *SegmentGroup) monitorSegments() {
	if sg.metrics == nil || sg.metrics.groupClasses {
		return
	}

	// Keeping metering to only the critical buckets helps
	// cut down on noise when monitoring
	if sg.metrics.criticalBucketsOnly {
		bucket := path.Base(sg.dir)
		if bucket != helpers.ObjectsBucketLSM &&
			!strings.HasPrefix(bucket, helpers.VectorsCompressedBucketLSM) {
			return
		}
		if bucket == helpers.ObjectsBucketLSM {
			sg.metrics.ObjectsBucketSegments.With(prometheus.Labels{
				"strategy": sg.strategy,
				"path":     sg.dir,
			}).Set(float64(sg.Len()))
		}
		if strings.HasPrefix(bucket, helpers.VectorsCompressedBucketLSM) {
			sg.metrics.CompressedVecsBucketSegments.With(prometheus.Labels{
				"strategy": sg.strategy,
				"path":     sg.dir,
			}).Set(float64(sg.Len()))
		}
		sg.reportSegmentStats()
		return
	}

	sg.metrics.ActiveSegments.With(prometheus.Labels{
		"strategy": sg.strategy,
		"path":     sg.dir,
	}).Set(float64(sg.Len()))
	sg.reportSegmentStats()
}

func (sg *SegmentGroup) reportSegmentStats() {
	stats := sg.segmentLevelStats()
	stats.fillMissingLevels()
	stats.report(sg.metrics, sg.strategy, sg.dir)
}

type segmentLevelStats struct {
	indexes  map[uint16]int
	payloads map[uint16]int
	count    map[uint16]int
	unloaded int
}

func newSegmentLevelStats() segmentLevelStats {
	return segmentLevelStats{
		indexes:  map[uint16]int{},
		payloads: map[uint16]int{},
		count:    map[uint16]int{},
		unloaded: 0,
	}
}

func (sg *SegmentGroup) segmentLevelStats() segmentLevelStats {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	stats := newSegmentLevelStats()

	for _, seg := range sg.segments {
		if !seg.isLoaded() {
			stats.unloaded++
			continue
		}
		level := seg.getLevel()
		stats.count[level]++

		cur := stats.indexes[level]
		cur += seg.indexSize()
		stats.indexes[level] = cur

		cur = stats.payloads[level]
		cur += seg.payloadSize()
		stats.payloads[level] = cur
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

	metrics.SegmentUnloaded.With(prometheus.Labels{
		"strategy": strategy,
		"path":     dir,
	}).Set(float64(s.unloaded))
}

func (sg *SegmentGroup) compactionFitsSizeLimit(left, right Segment) bool {
	if sg.maxSegmentSize == 0 {
		// no limit is set, always return true
		return true
	}

	totalSize := left.Size() + right.Size()
	return totalSize <= sg.maxSegmentSize
}
