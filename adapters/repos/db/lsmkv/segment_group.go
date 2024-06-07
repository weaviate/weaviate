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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type SegmentGroup struct {
	segments []*segment

	// Lock() for changing the currently active segments, RLock() for normal
	// operation
	maintenanceLock sync.RWMutex
	dir             string

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

	mmapContents            bool
	keepTombstones          bool // see bucket for more datails
	useBloomFilter          bool // see bucket for more datails
	calcCountNetAdditions   bool // see bucket for more datails
	compactLeftOverSegments bool // see bucket for more datails

	allocChecker   memwatch.AllocChecker
	maxSegmentSize int64
}

type sgConfig struct {
	dir                   string
	strategy              string
	mapRequiresSorting    bool
	monitorCount          bool
	mmapContents          bool
	keepTombstones        bool
	useBloomFilter        bool
	calcCountNetAdditions bool
	forceCompaction       bool
	maxSegmentSize        int64
}

func newSegmentGroup(logger logrus.FieldLogger, metrics *Metrics,
	compactionCallbacks cyclemanager.CycleCallbackGroup, cfg sgConfig,
	allocChecker memwatch.AllocChecker,
) (*SegmentGroup, error) {
	list, err := os.ReadDir(cfg.dir)
	if err != nil {
		return nil, err
	}

	sg := &SegmentGroup{
		segments:                make([]*segment, len(list)),
		dir:                     cfg.dir,
		logger:                  logger,
		metrics:                 metrics,
		monitorCount:            cfg.monitorCount,
		mapRequiresSorting:      cfg.mapRequiresSorting,
		strategy:                cfg.strategy,
		mmapContents:            cfg.mmapContents,
		keepTombstones:          cfg.keepTombstones,
		useBloomFilter:          cfg.useBloomFilter,
		calcCountNetAdditions:   cfg.calcCountNetAdditions,
		compactLeftOverSegments: cfg.forceCompaction,
		maxSegmentSize:          cfg.maxSegmentSize,
		allocChecker:            allocChecker,
	}

	segmentIndex := 0

	segmentsAlreadyRecoveredFromCompaction := make(map[string]struct{})

	// Note: it's important to process first the compacted segments
	// TODO: a single iteration may be possible

	for _, entry := range list {
		if filepath.Ext(entry.Name()) != ".tmp" {
			continue
		}

		potentialCompactedSegmentFileName := strings.TrimSuffix(entry.Name(), ".tmp")

		if filepath.Ext(potentialCompactedSegmentFileName) != ".db" {
			// another kind of temporal file, ignore at this point but it may need to be deleted...
			continue
		}

		jointSegments := segmentID(potentialCompactedSegmentFileName)
		jointSegmentsIDs := strings.Split(jointSegments, "_")

		if len(jointSegmentsIDs) != 2 {
			logger.WithField("action", "lsm_segment_init").
				WithField("path", filepath.Join(sg.dir, entry.Name())).
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
			if err := os.Remove(filepath.Join(sg.dir, entry.Name())); err != nil {
				return nil, fmt.Errorf("delete partially compacted segment %q: %w", entry.Name(), err)
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
				metrics, sg.makeExistsOnLower(segmentIndex),
				sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
			if err != nil {
				return nil, fmt.Errorf("init already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			err = rightSegment.close()
			if err != nil {
				return nil, fmt.Errorf("close already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			err = rightSegment.drop()
			if err != nil {
				return nil, fmt.Errorf("delete already compacted right segment %s: %w", rightSegmentFilename, err)
			}

			err = fsync(sg.dir)
			if err != nil {
				return nil, fmt.Errorf("fsync segment directory %s: %w", sg.dir, err)
			}
		}

		if err := os.Rename(filepath.Join(sg.dir, entry.Name()), rightSegmentPath); err != nil {
			return nil, fmt.Errorf("rename compacted segment file %q as %q: %w", entry.Name(), rightSegmentFilename, err)
		}

		segment, err := newSegment(rightSegmentPath, logger,
			metrics, sg.makeExistsOnLower(segmentIndex),
			sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, true)
		if err != nil {
			return nil, fmt.Errorf("init segment %s: %w", rightSegmentFilename, err)
		}

		sg.segments[segmentIndex] = segment
		segmentIndex++

		segmentsAlreadyRecoveredFromCompaction[rightSegmentFilename] = struct{}{}
	}

	for _, entry := range list {
		if filepath.Ext(entry.Name()) != ".db" {
			// skip, this could be commit log, etc.
			continue
		}

		_, alreadyRecoveredFromCompaction := segmentsAlreadyRecoveredFromCompaction[entry.Name()]
		if alreadyRecoveredFromCompaction {
			// the .db file was already removed and restored from a compacted segment
			continue
		}

		// before we can mount this file, we need to check if a WAL exists for it.
		// If yes, we must assume that the flush never finished, as otherwise the
		// WAL would have been lsmkv.Deleted. Thus we must remove it.
		walFileName := strings.TrimSuffix(entry.Name(), ".db") + ".wal"
		ok, err := fileExists(filepath.Join(sg.dir, walFileName))
		if err != nil {
			return nil, fmt.Errorf("check for presence of wals for segment %s: %w",
				entry.Name(), err)
		}
		if ok {
			// the segment will be recovered from the WAL
			err := os.Remove(filepath.Join(sg.dir, entry.Name()))
			if err != nil {
				return nil, fmt.Errorf("delete partially written segment %s: %w", entry.Name(), err)
			}

			logger.WithField("action", "lsm_segment_init").
				WithField("path", filepath.Join(sg.dir, entry.Name())).
				WithField("wal_path", walFileName).
				Info("discarded (partially written) LSM segment, because an active WAL for " +
					"the same segment was found. A recovery from the WAL will follow.")

			continue
		}

		segment, err := newSegment(filepath.Join(sg.dir, entry.Name()), logger,
			metrics, sg.makeExistsOnLower(segmentIndex),
			sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
		if err != nil {
			return nil, fmt.Errorf("init segment %s: %w", entry.Name(), err)
		}

		sg.segments[segmentIndex] = segment
		segmentIndex++
	}

	sg.segments = sg.segments[:segmentIndex]

	if sg.monitorCount {
		sg.metrics.ObjectCount(sg.count())
	}

	id := "segmentgroup/compaction/" + sg.dir
	sg.compactionCallbackCtrl = compactionCallbacks.Register(id, sg.compactIfLevelsMatch)

	return sg, nil
}

func (sg *SegmentGroup) makeExistsOnLower(nextSegmentIndex int) existsOnLowerSegmentsFn {
	return func(key []byte) (bool, error) {
		if nextSegmentIndex == 0 {
			// this is already the lowest possible segment, we can guarantee that
			// any key in this segment is previously unseen.
			return false, nil
		}

		v, err := sg.getWithUpperSegmentBoundary(key, nextSegmentIndex-1)
		if err != nil {
			return false, fmt.Errorf("check exists on segments lower than %d: %w",
				nextSegmentIndex, err)
		}

		return v != nil, nil
	}
}

func (sg *SegmentGroup) add(path string) error {
	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	newSegmentIndex := len(sg.segments)
	segment, err := newSegment(path, sg.logger,
		sg.metrics, sg.makeExistsOnLower(newSegmentIndex),
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, true)
	if err != nil {
		return fmt.Errorf("init segment %s: %w", path, err)
	}

	sg.segments = append(sg.segments, segment)
	return nil
}

func (sg *SegmentGroup) get(key []byte) ([]byte, error) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return sg.getWithUpperSegmentBoundary(key, len(sg.segments)-1)
}

// not thread-safe on its own, as the assumption is that this is called from a
// lockholder, e.g. within .get()
func (sg *SegmentGroup) getWithUpperSegmentBoundary(key []byte, topMostSegment int) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := topMostSegment; i >= 0; i-- {
		v, err := sg.segments[i].get(key)
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
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	return sg.getWithUpperSegmentBoundaryErrDeleted(key, len(sg.segments)-1)
}

func (sg *SegmentGroup) getWithUpperSegmentBoundaryErrDeleted(key []byte, topMostSegment int) ([]byte, error) {
	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := topMostSegment; i >= 0; i-- {
		v, err := sg.segments[i].get(key)
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

	return nil, nil
}

func (sg *SegmentGroup) getBySecondaryIntoMemory(pos int, key []byte, buffer []byte) ([]byte, []byte, error) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	// assumes "replace" strategy

	// start with latest and exit as soon as something is found, thus making sure
	// the latest takes presence
	for i := len(sg.segments) - 1; i >= 0; i-- {
		v, err, allocatedBuff := sg.segments[i].getBySecondaryIntoMemory(pos, key, buffer)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			if errors.Is(err, lsmkv.Deleted) {
				return nil, nil, nil
			}

			panic(fmt.Sprintf("unsupported error in segmentGroup.get(): %v", err))
		}

		return v, allocatedBuff, nil
	}

	return nil, nil, nil
}

func (sg *SegmentGroup) getCollection(key []byte) ([]value, error) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	var out []value

	// start with first and do not exit
	for _, segment := range sg.segments {
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

func (sg *SegmentGroup) getCollectionBySegments(key []byte) ([][]value, error) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	out := make([][]value, len(sg.segments))

	i := 0
	// start with first and do not exit
	for _, segment := range sg.segments {
		v, err := segment.getCollection(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			return nil, err
		}

		out[i] = v
		i++
	}

	return out[:i], nil
}

func (sg *SegmentGroup) roaringSetGet(key []byte) (roaringset.BitmapLayers, error) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	var out roaringset.BitmapLayers

	// start with first and do not exit
	for _, segment := range sg.segments {
		rs, err := segment.roaringSetGet(key)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				continue
			}

			return nil, err
		}

		out = append(out, rs)
	}

	return out, nil
}

func (sg *SegmentGroup) count() int {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	count := 0
	for _, seg := range sg.segments {
		count += seg.countNetAdditions
	}

	return count
}

func (sg *SegmentGroup) shutdown(ctx context.Context) error {
	if err := sg.compactionCallbackCtrl.Unregister(ctx); err != nil {
		return fmt.Errorf("long-running compaction in progress: %w", ctx.Err())
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

	for i, seg := range sg.segments {
		if err := seg.close(); err != nil {
			return err
		}

		sg.segments[i] = nil
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
