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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (sg *SegmentGroup) findInvertedConversionCandidates() (*segment, *sroar.Bitmap, int, error) {
	// if true, the parent shard has indicated that it has
	// entered an immutable state. During this time, the
	// SegmentGroup should refrain from flushing until its
	// shard indicates otherwise
	if sg.isReadyOnly() {
		return nil, nil, 0, fmt.Errorf("segment group is in read-only mode")
	}

	allTombstones := sroar.NewBitmap()

	// as newest segments are prioritized, loop in reverse order
	for id := len(sg.segments) - 1; id >= 0; id-- {
		if sg.segments[id].strategy == segmentindex.StrategyInverted {
			tombstones, err := sg.segments[id].GetTombstones()
			if err != nil {
				sg.logger.WithError(err).WithFields(logrus.Fields{
					"action": "lsm_conversion_mapcollection_inverted",
					"event":  "get_tombstones",
					"path":   sg.segments[id].path,
				}).Error("failed to get tombstones")
				return nil, nil, 0, err
			}

			allTombstones = allTombstones.Or(tombstones)
			continue
		}

		if sg.segments[id].strategy == segmentindex.StrategyMapCollection && strings.Contains(sg.segments[id].path, "_searchable") {
			return sg.segments[id], allTombstones, id, nil
		}

	}
	return nil, nil, 0, nil
}

func (sg *SegmentGroup) convertOnce(shouldAbort func() bool) (bool, bool, string, *convertedInvertedStats, error) {
	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()
	if len(sg.segments) == 0 {
		return false, true, "", nil, nil
	}

	segment, tombstones, index, err := sg.findInvertedConversionCandidates()
	if err != nil {
		return false, false, "", nil, err
	}

	if segment == nil {
		return false, true, "", nil, nil
	}

	path2 := sg.segments[0].path
	path2 = filepath.Dir(path2)
	pathSplit := strings.Split(path2, "/")
	// get parent dir
	propName := strings.Split(pathSplit[len(pathSplit)-1], "_")[1]
	propTokenization := "word"

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
				"action": "lsm_conversion_mapcollection_inverted",
				"event":  "conversion_skipped_oom",
				"path":   sg.dir,
			}).WithError(err).
				Warnf("skipping conversion due to memory pressure")

			return false, false, segment.path, nil, fmt.Errorf("skipping conversion due to memory pressure: %w", err)
		}
	}

	if shouldAbort() {
		sg.logger.WithFields(logrus.Fields{
			"action": "lsm_conversion_mapcollection_inverted",
			"path":   sg.dir,
		}).Warnf("skipping conversion due to shouldAbort")
		return false, false, "", nil, fmt.Errorf("skipping conversion due to shouldAbort")
	}

	path := filepath.Join(sg.dir, "segment-"+segmentID(segment.path)+".db.tmp")

	f, err := os.Create(path)
	if err != nil {
		return false, false, segment.path, nil, err
	}

	scratchSpacePath := segment.path + "compaction.scratch.d"

	secondaryIndices := segment.secondaryIndexCount
	cleanupTombstones := !sg.keepTombstones && index == 0

	pathLabel := "n/a"
	if sg.metrics != nil && !sg.metrics.groupClasses {
		pathLabel = sg.dir
	}
	// size := segment.size
	// fmt.Println("Converting segment: ", segment.path, size)
	sg.logger.WithFields(logrus.Fields{
		"action": "lsm_conversion_mapcollection_inverted",
		"event":  "conversion_started",
		"path":   path,
	}).Debug("conversion started")

	c := newConvertedInverted(f,
		segment.newMapCursor(),
		segment.level, secondaryIndices, scratchSpacePath, cleanupTombstones, tombstones, propName, propTokenization)

	if sg.metrics != nil {
		sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Inc()
		defer sg.metrics.CompactionMap.With(prometheus.Labels{"path": pathLabel}).Dec()
	}
	start := time.Now()
	if err := c.do(shouldAbort); err != nil {
		return false, false, segment.path, nil, err
	}

	if err := f.Sync(); err != nil {
		return false, false, segment.path, nil, errors.Wrap(err, "fsync converted segment file")
	}

	end := time.Now()

	if err := f.Close(); err != nil {
		return false, false, segment.path, nil, errors.Wrap(err, "close converted segment file")
	}

	sg.logger.WithFields(logrus.Fields{
		"action": "lsm_conversion_mapcollection_inverted",
		"event":  "conversion_done",
		"path":   path,
		"took":   end.Sub(start),
	}).Debug("Conversion completed successfully")

	// time
	// fmt.Println("Converted segment: ", segment.path, size, newSize.Size(), end.Sub(start).Seconds(), c.statsWrittenDocs, c.statsDeletedDocs, c.statsUpdatedDocs, c.statsWrittenKeys, c.statsDeletedKeys, c.statsUpdatedKeys, c.statsDeletedDocsLaterSegment, c.statsDeletedDocsObjectBucket, c.statsDeletedDocsIdBucket, c.statsDeletedDocsNoData, c.statsDeletedDocsNoProp, c.statsDeletedDocsNoText)

	if err := sg.replaceCompactedSegment(index, path); err != nil {
		return false, false, segment.path, nil, errors.Wrap(err, "replace converted segments")
	}

	return true, index == 0, segment.path, c.stats, nil
}

func (sg *SegmentGroup) replaceCompactedSegment(old int,
	newPathTmp string,
) error {
	// sg.maintenanceLock.RLock()
	updatedCountNetAdditions := sg.segments[old].countNetAdditions
	// sg.maintenanceLock.RUnlock()

	// WIP: we could add a random suffix to the tmp file to avoid conflicts
	precomputedFiles, err := preComputeSegmentMeta(newPathTmp,
		updatedCountNetAdditions, sg.logger,
		sg.useBloomFilter, sg.calcCountNetAdditions, false)
	if err != nil {
		return fmt.Errorf("precompute segment meta: %w", err)
	}

	s, err := sg.replaceCompactedSegmentBlocking(old, precomputedFiles)
	if err != nil {
		return fmt.Errorf("replace converted segments (blocking): %w", err)
	}

	mvMarkerSuffix := ".mapcollection"
	if err := os.Rename(s.bloomFilterPath()+DeleteMarkerSuffix, s.bloomFilterPath()+mvMarkerSuffix); err != nil {
		return fmt.Errorf("drop previously marked bloom filter: %w", err)
	}

	for i := 0; i < int(s.secondaryIndexCount); i++ {
		if err := os.Rename(s.bloomFilterSecondaryPath(i)+DeleteMarkerSuffix, s.bloomFilterSecondaryPath(i)+mvMarkerSuffix); err != nil {
			return fmt.Errorf("drop previously marked secondary bloom filter: %w", err)
		}
	}

	if err := os.Rename(s.path+DeleteMarkerSuffix, s.path+mvMarkerSuffix); err != nil {
		return fmt.Errorf("rename previously marked segment: %w", err)
	}

	return nil
}

func (sg *SegmentGroup) replaceCompactedSegmentBlocking(
	old int, precomputedFiles []string,
) (*segment, error) {
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
	sg.flushVsCompactLock.Lock()
	defer sg.flushVsCompactLock.Unlock()

	segment := sg.segments[old]

	if err := segment.close(); err != nil {
		return nil, errors.Wrap(err, "close disk segment")
	}

	if err := segment.markForDeletion(); err != nil {
		return nil, errors.Wrap(err, "drop disk segment")
	}

	err := diskio.Fsync(sg.dir)
	if err != nil {
		return nil, fmt.Errorf("fsync segment directory %s: %w", sg.dir, err)
	}

	sg.segments[old] = nil

	var newPath string
	// the old segments have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files which
	// carried the name of the second old segment
	for i, path := range precomputedFiles {
		updated, err := sg.stripTmpExtensionSingle(path)
		if err != nil {
			return nil, errors.Wrap(err, "strip .tmp extension of new segment")
		}

		if i == 0 {
			// the first element in the list is the segment itself
			newPath = updated
		}
	}

	seg, err := newSegment(newPath, sg.logger, sg.metrics, nil, segmentConfig{
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false, false,
	})
	if err != nil {
		return nil, errors.Wrap(err, "create new segment")
	}

	sg.segments[old] = seg

	return segment, nil
}

func (sg *SegmentGroup) stripTmpExtensionSingle(oldPath string) (string, error) {
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

func (b *Bucket) CompactAll() error {
	for {
		ok, err := b.disk.compactOnce()
		if err != nil {
			return fmt.Errorf("error during compaction: %w", err)
		}
		if !ok {
			break
		}
	}
	return nil
}
