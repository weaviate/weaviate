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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type SegmentGroup struct {
	segments []Segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
	dir             string

	cursorsLock      sync.RWMutex
	activeCursors    int
	enqueuedSegments []Segment

	// flushVsCompactLock is a simple synchronization mechanism between the
	// compaction and flush cycle. In general, those are independent, however,
	// there are parts of it that are not. See the comments of the routines
	// interacting with this lock for more details.
	flushVsCompactLock sync.Mutex

	strategy string

	compactionCallbackCtrl cyclemanager.CycleCallbackCtrl

	logger logrus.FieldLogger

	// for backward-compatibility with states where the disk state for maps was
	// not guaranteed to be sorted yet
	mapRequiresSorting bool

	status     storagestate.Status
	statusLock sync.Mutex
	metrics    *Metrics

	// all "replace" buckets support counting through net additions, but not all
	// produce a meaningful count. Typically, the only count we're interested in
	// is that of the bucket that holds objects
	monitorCount bool

	mmapContents             bool
	keepTombstones           bool // see bucket for more details
	useBloomFilter           bool // see bucket for more details
	calcCountNetAdditions    bool // see bucket for more details
	compactLeftOverSegments  bool // see bucket for more details
	enableChecksumValidation bool
	MinMMapSize              int64
	keepLevelCompaction      bool // see bucket for more details

	allocChecker   memwatch.AllocChecker
	maxSegmentSize int64

	segmentCleaner     segmentCleaner
	cleanupInterval    time.Duration
	lastCleanupCall    time.Time
	lastCompactionCall time.Time

	roaringSetRangeSegmentInMemory *roaringsetrange.SegmentInMemory
	bm25config                     *schema.BM25Config
}

type sgConfig struct {
	dir                      string
	strategy                 string
	mapRequiresSorting       bool
	monitorCount             bool
	mmapContents             bool
	keepTombstones           bool
	useBloomFilter           bool
	calcCountNetAdditions    bool
	forceCompaction          bool
	keepLevelCompaction      bool
	maxSegmentSize           int64
	cleanupInterval          time.Duration
	enableChecksumValidation bool
	keepSegmentsInMemory     bool
	MinMMapSize              int64
	bm25config               *models.BM25Config
}

func newSegmentGroup(logger logrus.FieldLogger, metrics *Metrics,
	compactionCallbacks cyclemanager.CycleCallbackGroup, cfg sgConfig,
	allocChecker memwatch.AllocChecker, lazySegmentLoading bool, files map[string]int64,
) (*SegmentGroup, error) {
	now := time.Now()
	sg := &SegmentGroup{
		segments:                 make([]Segment, len(files)),
		dir:                      cfg.dir,
		logger:                   logger,
		metrics:                  metrics,
		monitorCount:             cfg.monitorCount,
		mapRequiresSorting:       cfg.mapRequiresSorting,
		strategy:                 cfg.strategy,
		mmapContents:             cfg.mmapContents,
		keepTombstones:           cfg.keepTombstones,
		useBloomFilter:           cfg.useBloomFilter,
		calcCountNetAdditions:    cfg.calcCountNetAdditions,
		compactLeftOverSegments:  cfg.forceCompaction,
		maxSegmentSize:           cfg.maxSegmentSize,
		cleanupInterval:          cfg.cleanupInterval,
		enableChecksumValidation: cfg.enableChecksumValidation,
		allocChecker:             allocChecker,
		lastCompactionCall:       now,
		lastCleanupCall:          now,
		MinMMapSize:              cfg.MinMMapSize,
	}

	segmentIndex := 0

	segmentsAlreadyRecoveredFromCompaction := make(map[string]struct{})

	// Note: it's important to process first the compacted segments
	// TODO: a single iteration may be possible

	for entry := range files {
		if filepath.Ext(entry) != ".tmp" {
			continue
		}

		potentialCompactedSegmentFileName := strings.TrimSuffix(entry, ".tmp")

		if filepath.Ext(potentialCompactedSegmentFileName) != ".db" {
			// another kind of temporal file, ignore at this point but it may need to be deleted...
			continue
		}

		jointSegments := segmentID(potentialCompactedSegmentFileName)
		jointSegmentsIDs := strings.Split(jointSegments, "_")

		if len(jointSegmentsIDs) == 1 {
			// cleanup leftover, to be removed
			if err := os.Remove(filepath.Join(sg.dir, entry)); err != nil {
				return nil, fmt.Errorf("delete partially cleaned segment %q: %w", entry, err)
			}
			continue
		}

		if len(jointSegmentsIDs) != 2 {
			logger.WithField("action", "lsm_segment_init").
				WithField("path", filepath.Join(sg.dir, entry)).
				Warn("ignored (partially written) LSM compacted segment generated with a version older than v1.24.0")

			continue
		}

		leftSegmentFilename := fmt.Sprintf("segment-%s.db", jointSegmentsIDs[0])
		rightSegmentFilename := fmt.Sprintf("segment-%s.db", jointSegmentsIDs[1])

		leftSegmentPath := filepath.Join(sg.dir, leftSegmentFilename)
		rightSegmentPath := filepath.Join(sg.dir, rightSegmentFilename)

		leftSegmentFound, err := fileExists(leftSegmentPath)
		if err != nil {
			return nil, fmt.Errorf("check for presence of segment %s: %w", leftSegmentFilename, err)
		}

		rightSegmentFound, err := fileExists(rightSegmentPath)
		if err != nil {
			return nil, fmt.Errorf("check for presence of segment %s: %w", rightSegmentFilename, err)
		}

		if leftSegmentFound && rightSegmentFound {
			if err := os.Remove(filepath.Join(sg.dir, entry)); err != nil {
				return nil, fmt.Errorf("delete partially compacted segment %q: %w", entry, err)
			}
			continue
		}

		if leftSegmentFound && !rightSegmentFound {
			return nil, fmt.Errorf("missing right segment %q", rightSegmentFilename)
		}

		if !leftSegmentFound && rightSegmentFound {
			// segment is initialized just to be erased
			// there is no need of bloom filters nor net addition counter re-calculation
			rightSegment, err := newSegment(rightSegmentPath, logger,
				metrics, sg.makeExistsOn(nil),
				segmentConfig{
					mmapContents:             sg.mmapContents,
					useBloomFilter:           sg.useBloomFilter,
					calcCountNetAdditions:    sg.calcCountNetAdditions,
					overwriteDerived:         false,
					enableChecksumValidation: sg.enableChecksumValidation,
					MinMMapSize:              sg.MinMMapSize,
					allocChecker:             sg.allocChecker,
					fileList:                 make(map[string]int64), // empty to not check if bloom/cna files already exist
				})
			if err != nil {
				return nil, fmt.Errorf("init already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			err = rightSegment.close()
			if err != nil {
				return nil, fmt.Errorf("close already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			// https://github.com/weaviate/weaviate/pull/6128 introduces the ability
			// to drop segments delayed by renaming them first and then dropping them
			// later.
			//
			// The existing functionality (previously .drop) was renamed to
			// .dropImmediately. We are keeping the old behavior in this mainly for
			// backward compatbility, but also because the motivation behind the
			// delayed deletion does not apply here:
			//
			// The new behavior is meant to split the deletion into two steps, to
			// reduce the time that an expensive lock – which could block readers -
			// is held. In this scenario, the segment has not been initialized yet,
			// so there is no one we could be blocking.
			//
			// The total time is the same, so we can also just drop it immediately.
			err = rightSegment.dropImmediately()
			if err != nil {
				return nil, fmt.Errorf("delete already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			err = diskio.Fsync(sg.dir)
			if err != nil {
				return nil, fmt.Errorf("fsync segment directory %s: %w", sg.dir, err)
			}
		}

		if err := os.Rename(filepath.Join(sg.dir, entry), rightSegmentPath); err != nil {
			return nil, fmt.Errorf("rename compacted segment file %q as %q: %w", entry, rightSegmentFilename, err)
		}

		var segment Segment
		sgConf := segmentConfig{
			mmapContents:             sg.mmapContents,
			useBloomFilter:           sg.useBloomFilter,
			calcCountNetAdditions:    sg.calcCountNetAdditions,
			overwriteDerived:         true,
			enableChecksumValidation: sg.enableChecksumValidation,
			MinMMapSize:              sg.MinMMapSize,
			allocChecker:             sg.allocChecker,
			fileList:                 files,
		}
		if lazySegmentLoading {
			segment, err = newLazySegment(rightSegmentPath, logger,
				metrics, sg.makeExistsOn(sg.segments[:segmentIndex]), sgConf,
			)
			if err != nil {
				return nil, fmt.Errorf("init lazy segment %s: %w", rightSegmentFilename, err)
			}
		} else {
			segment, err = newSegment(rightSegmentPath, logger,
				metrics, sg.makeExistsOn(sg.segments[:segmentIndex]), sgConf,
			)
			if err != nil {
				return nil, fmt.Errorf("init segment %s: %w", rightSegmentFilename, err)
			}
		}

		sg.segments[segmentIndex] = segment
		segmentIndex++

		segmentsAlreadyRecoveredFromCompaction[rightSegmentFilename] = struct{}{}
	}

	for entry := range files {
		if filepath.Ext(entry) == DeleteMarkerSuffix {
			// marked for deletion, but never actually deleted. Delete now.
			if err := os.Remove(filepath.Join(sg.dir, entry)); err != nil {
				// don't abort if the delete fails, we can still continue (albeit
				// without freeing disk space that should have been freed)
				sg.logger.WithError(err).WithFields(logrus.Fields{
					"action": "lsm_segment_init_deleted_previously_marked_files",
					"file":   entry,
				}).Error("failed to delete file already marked for deletion")
			}
			continue

		}

		if filepath.Ext(entry) != ".db" {
			// skip, this could be commit log, etc.
			continue
		}

		_, alreadyRecoveredFromCompaction := segmentsAlreadyRecoveredFromCompaction[entry]
		if alreadyRecoveredFromCompaction {
			// the .db file was already removed and restored from a compacted segment
			continue
		}

		// before we can mount this file, we need to check if a WAL exists for it.
		// If yes, we must assume that the flush never finished, as otherwise the
		// WAL would have been lsmkv.Deleted. Thus we must remove it.
		walFileName := strings.TrimSuffix(entry, ".db") + ".wal"
		_, ok := files[walFileName]
		if ok {
			// the segment will be recovered from the WAL
			err := os.Remove(filepath.Join(sg.dir, entry))
			if err != nil {
				return nil, fmt.Errorf("delete partially written segment %s: %w", entry, err)
			}

			logger.WithField("action", "lsm_segment_init").
				WithField("path", filepath.Join(sg.dir, entry)).
				WithField("wal_path", walFileName).
				Info("discarded (partially written) LSM segment, because an active WAL for " +
					"the same segment was found. A recovery from the WAL will follow.")

			continue
		}

		var segment Segment
		segConf := segmentConfig{
			mmapContents:             sg.mmapContents,
			useBloomFilter:           sg.useBloomFilter,
			calcCountNetAdditions:    sg.calcCountNetAdditions,
			overwriteDerived:         false,
			enableChecksumValidation: sg.enableChecksumValidation,
			MinMMapSize:              sg.MinMMapSize,
			allocChecker:             sg.allocChecker,
			fileList:                 files,
		}
		var err error
		if lazySegmentLoading {
			segment, err = newLazySegment(filepath.Join(sg.dir, entry), logger,
				metrics, sg.makeExistsOn(sg.segments[:segmentIndex]), segConf,
			)
			if err != nil {
				return nil, fmt.Errorf("init lazy segment %s: %w", filepath.Join(sg.dir, entry), err)
			}
		} else {
			segment, err = newSegment(filepath.Join(sg.dir, entry), logger,
				metrics, sg.makeExistsOn(sg.segments[:segmentIndex]), segConf,
			)
			if err != nil {
				return nil, fmt.Errorf("init segment %s: %w", filepath.Join(sg.dir, entry), err)
			}
		}
		sg.segments[segmentIndex] = segment
		segmentIndex++
	}

	sg.segments = sg.segments[:segmentIndex]

	if sg.monitorCount {
		sg.metrics.ObjectCount(sg.count())
	}

	sc, err := newSegmentCleaner(sg)
	if err != nil {
		return nil, err
	}
	sg.segmentCleaner = sc

	// TODO AL: use separate cycle callback for cleanup?
	id := "segmentgroup/compaction/" + sg.dir
	sg.compactionCallbackCtrl = compactionCallbacks.Register(id, sg.compactOrCleanup)

	// if a segment exists of the map collection strategy, we need to
	// convert the inverted strategy to a map collection strategy
	// as it is done on the bucket level
	if sg.strategy == StrategyInverted && len(sg.segments) > 0 &&
		sg.segments[0].getStrategy() == segmentindex.StrategyMapCollection {
		sg.strategy = StrategyMapCollection
	}

	switch sg.strategy {
	case StrategyInverted:
		// start with last but one segment, as the last one doesn't need tombstones for now
		for i := len(sg.segments) - 2; i >= 0; i-- {
			// avoid crashing if segment has no tombstones
			tombstonesNext, err := sg.segments[i+1].ReadOnlyTombstones()
			if err != nil {
				return nil, fmt.Errorf("init segment %s: load tombstones %w", sg.segments[i+1].getPath(), err)
			}
			if _, err := sg.segments[i].MergeTombstones(tombstonesNext); err != nil {
				return nil, fmt.Errorf("init segment %s: merge tombstones %w", sg.segments[i].getPath(), err)
			}
		}

	case StrategyRoaringSetRange:
		if cfg.keepSegmentsInMemory {
			t := time.Now()
			sg.roaringSetRangeSegmentInMemory = roaringsetrange.NewSegmentInMemory()
			for _, seg := range sg.segments {
				cursor := seg.newRoaringSetRangeCursor()
				if err := sg.roaringSetRangeSegmentInMemory.MergeSegmentByCursor(cursor); err != nil {
					return nil, fmt.Errorf("build segment-in-memory of strategy '%s': %w", sg.strategy, err)
				}
			}
			logger.WithFields(logrus.Fields{
				"took":    time.Since(t).String(),
				"bucket":  filepath.Base(cfg.dir),
				"size_mb": fmt.Sprintf("%.3f", float64(sg.roaringSetRangeSegmentInMemory.Size())/1024/1024),
			}).Debug("rangeable segment-in-memory built")
		}
	}

	return sg, nil
}

func (sg *SegmentGroup) makeExistsOn(segments []Segment) existsOnLowerSegmentsFn {
	return func(key []byte) (bool, error) {
		if len(segments) == 0 {
			// this is already the lowest possible segment, we can guarantee that
			// any key in this segment is previously unseen.
			return false, nil
		}

		v, err := sg.getWithUpperSegmentBoundary(key, segments)
		if err != nil {
			return false, fmt.Errorf("check exists on segments: %w", err)
		}

		return v != nil, nil
	}
}

func (sg *SegmentGroup) add(path string) error {
	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	segment, err := newSegment(path, sg.logger,
		sg.metrics, sg.makeExistsOn(sg.segments),
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
		return fmt.Errorf("init segment %s: %w", path, err)
	}

	sg.segments = append(sg.segments, segment)
	return nil
}

func (sg *SegmentGroup) getAndLockSegments() (segments []Segment, release func()) {
	sg.cursorsLock.RLock()
	sg.maintenanceLock.RLock()

	if len(sg.enqueuedSegments) == 0 {
		return sg.segments, func() {
			sg.cursorsLock.RUnlock()
			sg.maintenanceLock.RUnlock()
		}
	}

	segments = make([]Segment, 0, len(sg.segments)+len(sg.enqueuedSegments))

	segments = append(segments, sg.segments...)
	segments = append(segments, sg.enqueuedSegments...)

	return segments, func() {
		sg.cursorsLock.RUnlock()
		sg.maintenanceLock.RUnlock()
	}
}

func (sg *SegmentGroup) addInitializedSegment(segment *segment) error {
	sg.cursorsLock.Lock()
	defer sg.cursorsLock.Unlock()

	if sg.activeCursors > 0 {
		sg.enqueuedSegments = append(sg.enqueuedSegments, segment)
		return nil
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	sg.segments = append(sg.segments, segment)
	return nil
}

func (sg *SegmentGroup) get(key []byte) ([]byte, error) {
	beforeMaintenanceLock := time.Now()
	segments, release := sg.getAndLockSegments()
	defer release()

	if time.Since(beforeMaintenanceLock) > 100*time.Millisecond {
		sg.logger.WithField("duration", time.Since(beforeMaintenanceLock)).
			WithField("action", "lsm_segment_group_get_obtain_maintenance_lock").
			Debug("waited over 100ms to obtain maintenance lock in segment group get()")
	}

	return sg.getWithUpperSegmentBoundary(key, segments)
}

// not thread-safe on its own, as the assumption is that this is called from a
// lockholder, e.g. within .get()
func (sg *SegmentGroup) getWithUpperSegmentBoundary(key []byte, segments []Segment) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(segments) - 1; i >= 0; i-- {
		beforeSegment := time.Now()
		v, err := segments[i].get(key)
		if time.Since(beforeSegment) > 100*time.Millisecond {
			sg.logger.WithField("duration", time.Since(beforeSegment)).
				WithField("action", "lsm_segment_group_get_individual_segment").
				WithError(err).
				WithField("segment_pos", i).
				Debug("waited over 100ms to get result from individual segment")
		}
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			if errors.Is(err, lsmkv.Deleted) {
				return nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, nil
	}

	return nil, nil
}

func (sg *SegmentGroup) getErrDeleted(key []byte) ([]byte, error) {
	segments, release := sg.getAndLockSegments()
	defer release()

	return sg.getWithUpperSegmentBoundaryErrDeleted(key, segments)
}

func (sg *SegmentGroup) getWithUpperSegmentBoundaryErrDeleted(key []byte, segments []Segment) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(segments) - 1; i >= 0; i-- {
		v, err := segments[i].get(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			if errors.Is(err, lsmkv.Deleted) {
				return nil, err
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, nil
	}

	return nil, lsmkv.NotFound
}

func (sg *SegmentGroup) getBySecondaryIntoMemory(pos int, key []byte, buffer []byte) ([]byte, []byte, []byte, error) {
	segments, release := sg.getAndLockSegments()
	defer release()

	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(segments) - 1; i >= 0; i-- {
		k, v, allocatedBuff, err := segments[i].getBySecondaryIntoMemory(pos, key, buffer)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			if errors.Is(err, lsmkv.Deleted) {
				return nil, nil, nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return k, v, allocatedBuff, nil
	}

	return nil, nil, nil, nil
}

func (sg *SegmentGroup) getCollection(key []byte) ([]value, error) {
	segments, release := sg.getAndLockSegments()
	defer release()

	var out []value

	// start with first and do not exit
	for _, segment := range segments {
		v, err := segment.getCollection(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			return nil, err
		}

		if len(out) == 0 {
			out = v
		} else {
			out = append(out, v...)
		}
	}

	return out, nil
}

func (sg *SegmentGroup) getCollectionAndSegments(key []byte) ([][]value, []Segment, func(), error) {
	segments, release := sg.getAndLockSegments()

	out := make([][]value, len(segments))
	outSegments := make([]Segment, len(segments))

	i := 0
	// start with first and do not exit
	for _, segment := range segments {
		v, err := segment.getCollection(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				release()
				return nil, nil, func() {}, err
			}
			// inverted segments need to be loaded anyway, even if they don't have
			// the key, as we need to know if they have tombstones
			if segment.getStrategy() != segmentindex.StrategyInverted {
				continue
			}
		}

		out[i] = v
		outSegments[i] = segment
		i++
	}

	return out[:i], outSegments[:i], release, nil
}

func (sg *SegmentGroup) roaringSetGet(key []byte) (roaringset.BitmapLayers, error) {
	segments, release := sg.getAndLockSegments()
	defer release()

	var out roaringset.BitmapLayers

	// start with first and do not exit
	for _, segment := range segments {
		layer, err := segment.roaringSetGet(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			return nil, err
		}

		out = append(out, layer)
	}

	return out, nil
}

func (sg *SegmentGroup) count() int {
	segments, release := sg.getAndLockSegments()
	defer release()

	count := 0
	for _, seg := range segments {
		count += seg.getCountNetAdditions()
	}

	return count
}

func (sg *SegmentGroup) shutdown(ctx context.Context) error {
	if err := sg.compactionCallbackCtrl.Unregister(ctx); err != nil {
		return fmt.Errorf("long-running compaction in progress: %w", ctx.Err())
	}
	if err := sg.segmentCleaner.close(); err != nil {
		return err
	}

	sg.cursorsLock.Lock()
	defer sg.cursorsLock.Unlock()

	for _, seg := range sg.enqueuedSegments {
		seg.close()
	}

	// Lock acquirement placed after compaction cycle stop request, due to occasional deadlock,
	// because compaction logic used in cycle also requires maintenance lock.
	//
	// If lock is grabbed by shutdown method and compaction in cycle loop starts right after,
	// it is blocked waiting for the same lock, eventually blocking entire cycle loop and preventing to read stop signal.
	// If stop signal can not be read, shutdown will not receive stop result and will not proceed with further execution.
	// Maintenance lock will then never be released.
	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	for _, seg := range sg.segments {
		if err := seg.close(); err != nil {
			return err
		}
	}

	// make sure the segment list itself is set to nil. In case a memtable will
	// still flush after closing, it might try to read from a disk segment list
	// otherwise and run into nil-pointer problems.
	sg.segments = nil

	return nil
}

func (sg *SegmentGroup) UpdateStatus(status storagestate.Status) {
	sg.statusLock.Lock()
	defer sg.statusLock.Unlock()

	sg.status = status
}

func (sg *SegmentGroup) isReadyOnly() bool {
	sg.statusLock.Lock()
	defer sg.statusLock.Unlock()

	return sg.status == storagestate.StatusReadOnly
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	return false, err
}

func (sg *SegmentGroup) compactOrCleanup(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	sg.monitorSegments()

	compact := func() bool {
		sg.lastCompactionCall = time.Now()
		compacted, err := sg.compactOnce()
		if err != nil {
			sg.logger.WithField("action", "lsm_compaction").
				WithField("path", sg.dir).
				WithError(err).
				Errorf("compaction failed")
		} else if !compacted {
			sg.logger.WithField("action", "lsm_compaction").
				WithField("path", sg.dir).
				Trace("no segments eligible for compaction")
		}
		return compacted
	}
	cleanup := func() bool {
		sg.lastCleanupCall = time.Now()
		cleaned, err := sg.segmentCleaner.cleanupOnce(shouldAbort)
		if err != nil {
			sg.logger.WithField("action", "lsm_cleanup").
				WithField("path", sg.dir).
				WithError(err).
				Errorf("cleanup failed")
		}
		return cleaned
	}

	// alternatively run compaction or cleanup first
	// if 1st one called succeeds, 2nd one is skipped, otherwise 2nd one is called as well
	//
	// compaction has the precedence over cleanup, however if cleanup
	// was not called for over [forceCleanupInterval], force at least one execution
	// in between compactions.
	// (ignore if compaction was not called within that time either)
	forceCleanupInterval := time.Hour * 12

	if time.Since(sg.lastCleanupCall) > forceCleanupInterval && sg.lastCleanupCall.Before(sg.lastCompactionCall) {
		return cleanup() || compact()
	}
	return compact() || cleanup()
}

func (sg *SegmentGroup) Len() int {
	segments, release := sg.getAndLockSegments()
	defer release()

	return len(segments)
}

func (sg *SegmentGroup) GetAveragePropertyLength() (float64, uint64) {
	segments, release := sg.getAndLockSegments()
	defer release()

	if len(segments) == 0 {
		return 0, 0
	}

	totalDocCount := uint64(0)
	for _, segment := range segments {
		invertedData := segment.getInvertedData()
		totalDocCount += invertedData.avgPropertyLengthsCount
	}

	if totalDocCount == 0 {
		return defaultAveragePropLength, 0
	}

	weightedAverage := 0.0
	for _, segment := range segments {
		invertedData := segment.getInvertedData()
		weightedAverage += float64(invertedData.avgPropertyLengthsCount) / float64(totalDocCount) * invertedData.avgPropertyLengthsAvg
	}

	return weightedAverage, totalDocCount
}
