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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	bolt "go.etcd.io/bbolt"
)

const (
	cleanupDbFileName = "cleanup.db.bolt"
	noIdx             = -1
)

var (
	cleanupSegmentsBucket       = []byte("segments")
	cleanupMetaBucket           = []byte("meta")
	cleanupMetaKeyNextAllowedTs = []byte("nextAllowedTs")
)

type segmentCleaner interface {
	close() error
	cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (cleaned bool, err error)
}

func newSegmentCleaner(sg *SegmentGroup) (segmentCleaner, error) {
	if sg.cleanupInterval <= 0 {
		return &segmentCleanerNoop{}, nil
	}

	switch sg.strategy {
	case StrategyReplace:
		cleaner := &segmentCleanerCommon{sg: sg}
		if err := cleaner.init(); err != nil {
			return nil, err
		}
		return cleaner, nil
	case StrategyMapCollection,
		StrategySetCollection,
		StrategyRoaringSet:
		// TODO AL: add roaring set range
		return &segmentCleanerNoop{}, nil
	default:
		return nil, fmt.Errorf("unrecognized strategy %q", sg.strategy)
	}
}

// ================================================================

type segmentCleanerNoop struct{}

func (c *segmentCleanerNoop) close() error {
	return nil
}

func (c *segmentCleanerNoop) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback) (bool, error) {
	return false, nil
}

// ================================================================

type segmentCleanerCommon struct {
	sg *SegmentGroup
	db *bolt.DB
}

func (c *segmentCleanerCommon) init() error {
	path := filepath.Join(c.sg.dir, cleanupDbFileName)
	var db *bolt.DB
	var err error

	if db, err = bolt.Open(path, 0o600, nil); err != nil {
		return fmt.Errorf("open cleanup bolt db %q: %w", path, err)
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(cleanupSegmentsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(cleanupMetaBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("create bucket cleanup bolt db %q: %w", path, err)
	}

	c.db = db
	return nil
}

func (c *segmentCleanerCommon) close() error {
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("close cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerCommon) findCandidate() (int, int, onCompletedFunc, error) {
	nowTs := time.Now().UnixNano()
	nextAllowedTs := nowTs - int64(c.sg.cleanupInterval)
	nextAllowedStoredTs := c.readNextAllowed()

	if nextAllowedStoredTs > nextAllowedTs {
		// too soon for next cleanup
		return noIdx, noIdx, nil, nil
	}

	ids, sizes, err := c.getSegmentIdsAndSizes()
	if err != nil {
		return noIdx, noIdx, nil, err
	}
	if count := len(ids); count <= 1 {
		// too few segments for cleanup, update next allowed timestamp for cleanup to now
		if err := c.storeNextAllowed(nowTs); err != nil {
			return noIdx, noIdx, nil, err
		}
		return noIdx, noIdx, nil, nil
	}

	// get idx and cleanup timestamp of earliest cleaned segment,
	// take the opportunity to find obsolete segment keys to be deleted later
	candidateIdx, startIdx, earliestCleanedTs, nonExistentSegmentKeys := c.readEarliestCleaned(ids, sizes, nowTs)

	if err := c.deleteSegmentMetas(nonExistentSegmentKeys); err != nil {
		return noIdx, noIdx, nil, err
	}

	if candidateIdx != noIdx && earliestCleanedTs <= nextAllowedTs {
		// candidate found
		id := ids[candidateIdx]
		lastProcessedId := ids[len(ids)-1]
		onCompleted := func(size int64) error {
			return c.storeSegmentMeta(id, lastProcessedId, size, nowTs)
		}
		return candidateIdx, startIdx, onCompleted, nil
	}

	// candidate not found, update next allowed timestamp for cleanup to earliest cleaned segment (or now)
	if err := c.storeNextAllowed(earliestCleanedTs); err != nil {
		return noIdx, noIdx, nil, err
	}

	return noIdx, noIdx, nil, nil
}

func (c *segmentCleanerCommon) getSegmentIdsAndSizes() ([]int64, []int64, error) {
	c.sg.maintenanceLock.RLock()
	defer c.sg.maintenanceLock.RUnlock()

	var ids []int64
	var sizes []int64
	if count := len(c.sg.segments); count > 1 {
		ids = make([]int64, count)
		sizes = make([]int64, count)

		for i, seg := range c.sg.segments {
			idStr := segmentID(seg.path)
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("parse segment id %q: %w", idStr, err)
			}
			ids[i] = id
			sizes[i] = seg.size
		}
	}

	return ids, sizes, nil
}

func (c *segmentCleanerCommon) readNextAllowed() int64 {
	ts := int64(0)
	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupMetaBucket)
		v := b.Get(cleanupMetaKeyNextAllowedTs)
		if v != nil {
			ts = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return ts
}

func (c *segmentCleanerCommon) storeNextAllowed(ts int64) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupMetaBucket)
		bufV := make([]byte, 8)

		binary.BigEndian.PutUint64(bufV, uint64(ts))
		return b.Put(cleanupMetaKeyNextAllowedTs, bufV)
	}); err != nil {
		return fmt.Errorf("updating cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerCommon) deleteSegmentMetas(segIds [][]byte) error {
	if len(segIds) > 0 {
		if err := c.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupSegmentsBucket)
			for _, k := range segIds {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("deleting from cleanup bolt db %q: %w", c.db.Path(), err)
		}
	}
	return nil
}

func (c *segmentCleanerCommon) readEarliestCleaned(ids, sizes []int64, nowTs int64,
) (int, int, int64, [][]byte) {
	earliestCleanedTs := nowTs
	candidateIdx := noIdx
	startIdx := noIdx

	count := len(ids)
	nonExistentSegmentKeys := [][]byte{}

	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupSegmentsBucket)
		c := b.Cursor()

		idx := 0
		ck, cv := c.First()

		// last segment does not need to be cleaned, therefore "count-1"
		for ck != nil && idx < count-1 {
			cid := int64(binary.BigEndian.Uint64(ck))
			id := ids[idx]

			if id > cid {
				// id no longer exists, to be removed from bolt db
				nonExistentSegmentKeys = append(nonExistentSegmentKeys, ck)
				ck, cv = c.Next()
			} else if id < cid {
				// id not yet present in bolt
				if earliestCleanedTs > 0 {
					earliestCleanedTs = 0
					candidateIdx = idx
					startIdx = idx + 1
				}
				idx++
			} else {
				// id present in bolt db
				cts := int64(binary.BigEndian.Uint64(cv[0:8]))
				// cleaned before current cleanup candidate
				// check if worth cleaning again
				if earliestCleanedTs > cts {
					lastId := ids[count-1]
					clastProcessedId := int64(binary.BigEndian.Uint64(cv[8:16]))
					if clastProcessedId < lastId {
						// current last segment's id is bigger than last processed one
						// (new segments must have been created since last cleanup)
						csize := int64(binary.BigEndian.Uint64(cv[16:24]))
						size := sizes[idx]

						// if size changed (compacted), start cleanup from next segment in case
						// left segment was not cleaned considering the same segments as right segment was
						// (could happen when new segments were added between left and right cleanup)
						tmpStartIdx := idx + 1
						if size == csize {
							// size not changed (not compacted), clean using only newly created segments,
							// skipping segments already processed in previous cleanup
							for i := idx + 1; i < count; i++ {
								tmpStartIdx = i
								if ids[i] > clastProcessedId {
									break
								}
							}
						}

						sumSize := int64(0)
						for i := tmpStartIdx; i < count; i++ {
							sumSize += sizes[i]
						}

						if sumSize*10 > size {
							// relevant segments bigger than 10% of cleaned segment,
							// clean again
							earliestCleanedTs = cts
							candidateIdx = idx
							startIdx = tmpStartIdx
						}
					}
				}
				ck, cv = c.Next()
				idx++
			}
		}
		// in case main loop finished due to idx reached count first
		for ; ck != nil; ck, _ = c.Next() {
			cid := int64(binary.BigEndian.Uint64(ck))
			if cid != ids[count-1] {
				nonExistentSegmentKeys = append(nonExistentSegmentKeys, ck)
			}
		}
		// in case main loop finished due to cursor reached end first
		for ; idx < count-1 && earliestCleanedTs > 0; idx++ {
			earliestCleanedTs = 0
			candidateIdx = idx
			startIdx = idx + 1
		}
		return nil
	})
	return candidateIdx, startIdx, earliestCleanedTs, nonExistentSegmentKeys
}

func (c *segmentCleanerCommon) storeSegmentMeta(id, lastProcessedId, size, cleanedTs int64) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupSegmentsBucket)
		bufK := make([]byte, 8)
		bufV := make([]byte, 24)

		binary.BigEndian.PutUint64(bufK, uint64(id))
		binary.BigEndian.PutUint64(bufV[0:8], uint64(cleanedTs))
		binary.BigEndian.PutUint64(bufV[8:16], uint64(lastProcessedId))
		binary.BigEndian.PutUint64(bufV[16:24], uint64(size))
		return b.Put(bufK, bufV)
	}); err != nil {
		return fmt.Errorf("updating cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerCommon) cleanupOnce(shouldAbort cyclemanager.ShouldAbortCallback,
) (bool, error) {
	if c.sg.isReadyOnly() {
		return false, nil
	}

	candidateIdx, startIdx, onCompleted, err := c.findCandidate()
	if err != nil {
		return false, err
	}
	if candidateIdx == noIdx {
		return false, nil
	}

	if c.sg.allocChecker != nil {
		// allocChecker is optional
		if err := c.sg.allocChecker.CheckAlloc(100 * 1024 * 1024); err != nil {
			// if we don't have at least 100MB to spare, don't start a cleanup. A
			// cleanup does not actually need a 100MB, but it will create garbage
			// that needs to be cleaned up. If we're so close to the memory limit, we
			// can increase stability by preventing anything that's not strictly
			// necessary. Cleanup can simply resume when the cluster has been
			// scaled.
			c.sg.logger.WithFields(logrus.Fields{
				"action": "lsm_cleanup",
				"event":  "cleanup_skipped_oom",
				"path":   c.sg.dir,
			}).WithError(err).
				Warnf("skipping cleanup due to memory pressure")

			return false, nil
		}
	}

	if shouldAbort() {
		c.sg.logger.WithFields(logrus.Fields{
			"action": "lsm_cleanup",
			"path":   c.sg.dir,
		}).Warnf("skipping cleanup due to shouldAbort")
		return false, nil
	}

	oldSegment := c.sg.segmentAtPos(candidateIdx)
	tmpSegmentPath := filepath.Join(c.sg.dir, "segment-"+segmentID(oldSegment.path)+".db.tmp")
	scratchSpacePath := oldSegment.path + "cleanup.scratch.d"

	file, err := os.Create(tmpSegmentPath)
	if err != nil {
		return false, err
	}

	switch c.sg.strategy {
	case StrategyReplace:
		c := newSegmentCleanerReplace(file, oldSegment.newCursor(),
			c.sg.makeKeyExistsOnUpperSegments(startIdx), oldSegment.level,
			oldSegment.secondaryIndexCount, scratchSpacePath)
		if err := c.do(shouldAbort); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unsported strategy %q", c.sg.strategy)
	}

	if err := file.Sync(); err != nil {
		return false, fmt.Errorf("fsync cleaned segment file: %w", err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("close cleaned segment file: %w", err)
	}

	segment, err := c.sg.replaceSegment(candidateIdx, tmpSegmentPath)
	if err != nil {
		return false, fmt.Errorf("replace compacted segments: %w", err)
	}
	if err := onCompleted(segment.size); err != nil {
		return false, fmt.Errorf("callback cleaned segment file: %w", err)
	}

	return true, nil
}

type onCompletedFunc func(size int64) error

// ================================================================

type keyExistsOnUpperSegmentsFunc func(key []byte) (bool, error)

func (sg *SegmentGroup) makeKeyExistsOnUpperSegments(startIdx int) keyExistsOnUpperSegmentsFunc {
	return func(key []byte) (bool, error) {
		// current len could be higher than the one stored in bolt db as last processed one.
		// that is acceptable and not considered an issue.

		i := startIdx
		segAtPos := func() *segment {
			sg.maintenanceLock.RLock()
			defer sg.maintenanceLock.RUnlock()

			if i < len(sg.segments) {
				i++
				return sg.segments[i-1]
			}
			return nil
		}

		for seg := segAtPos(); seg != nil; seg = segAtPos() {
			if exists, err := seg.exists(key); err != nil {
				return false, err
			} else if exists {
				return true, nil
			}
		}
		return false, nil
	}
}

func (sg *SegmentGroup) replaceSegment(segmentIdx int, tmpSegmentPath string,
) (*segment, error) {
	oldSegment := sg.segmentAtPos(segmentIdx)
	countNetAdditions := oldSegment.countNetAdditions

	precomputedFiles, err := preComputeSegmentMeta(tmpSegmentPath, countNetAdditions,
		sg.logger, sg.useBloomFilter, sg.calcCountNetAdditions)
	if err != nil {
		return nil, fmt.Errorf("precompute segment meta: %w", err)
	}

	sg.maintenanceLock.Lock()
	defer sg.maintenanceLock.Unlock()

	if err := oldSegment.close(); err != nil {
		return nil, fmt.Errorf("close disk segment %q: %w", oldSegment.path, err)
	}
	if err := oldSegment.drop(); err != nil {
		return nil, fmt.Errorf("drop disk segment %q: %w", oldSegment.path, err)
	}
	if err := fsync(sg.dir); err != nil {
		return nil, fmt.Errorf("fsync segment directory %q: %w", sg.dir, err)
	}

	segmentId := segmentID(oldSegment.path)
	var segmentPath string

	// the old segment have been deleted, we can now safely remove the .tmp
	// extension from the new segment itself and the pre-computed files
	for i, tmpPath := range precomputedFiles {
		path, err := sg.stripTmpExtension(tmpPath, segmentId, segmentId)
		if err != nil {
			return nil, fmt.Errorf("strip .tmp extension of new segment %q: %w", tmpPath, err)
		}
		if i == 0 {
			// the first element in the list is the segment itself
			segmentPath = path
		}
	}

	newSegment, err := newSegment(segmentPath, sg.logger, sg.metrics, nil,
		sg.mmapContents, sg.useBloomFilter, sg.calcCountNetAdditions, false)
	if err != nil {
		return nil, fmt.Errorf("create new segment %q: %w", newSegment.path, err)
	}

	sg.segments[segmentIdx] = newSegment
	return newSegment, nil
}
