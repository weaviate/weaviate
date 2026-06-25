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
	"encoding/binary"
	"errors"
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
	cleanupDbFileName     = "cleanup.db.bolt"
	emptyIdx              = -1
	minCleanupSizePercent = 10
	// cleanupAllocCheckBytes is the headroom required before starting a rewrite;
	// matches the heuristic cleanup path's guard.
	cleanupAllocCheckBytes = 100 * 1024 * 1024
)

// errCleanupPaused signals an edit-ops cleanup pass to stop without counting the
// segment as a failed attempt (a pause under memory pressure, not a
// quarantine-eligible error).
var errCleanupPaused = errors.New("cleanup paused due to memory pressure")

var (
	cleanupDbBucketSegments       = []byte("segments")
	cleanupDbBucketMeta           = []byte("meta")
	cleanupDbKeyMetaNextAllowedTs = []byte("nextAllowedTs")
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
		StrategyRoaringSet,
		StrategyRoaringSetRange,
		StrategyInverted:
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

// segmentCleanerCommon uses bolt db to persist data relevant to cleanup
// progress.
// db is stored in file named [cleanupDbFileName] in bucket directory, next to
// segment files.
//
// db uses 2 buckets:
// - [cleanupDbBucketMeta] to store global cleanup data
// - [cleanupDbBucketSegments] to store each segments cleanup data
//
// [cleanupDbBucketMeta] holds single key [cleanupDbKeyMetaNextAllowedTs] with value of
// timestamp of earliest of last segments' cleanups or last execution timestamp of findCandidate
// if no eligible cleanup candidate was found.
// [cleanupDbBucketSegments] holds multiple keys (being segment ids) with values being combined:
// - timestamp of current segment's cleanup
// - segmentId of last segment used in current segment's cleanup
// - size of current segment after cleanup
// Entries of segmentIds of segments that were removed (left segments after compaction)
// are regularly removed from cleanup db while next cleanup candidate is searched.
//
// cleanupInterval indicates minimal interval that have to pass for segment to be cleaned again.
// Each segment has stored its last cleanup timestamp in cleanup bolt db.
// Additionally "global" earliest cleanup timestamp is stored ([cleanupDbKeyMetaNextAllowedTs])
// or last execution timestamp of findCandiate method. This timeout is used to quickly exit
// findCandidate method without necessity to verify if interval passed for each segment.
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
		if _, err := tx.CreateBucketIfNotExists(cleanupDbBucketSegments); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(cleanupDbBucketMeta); err != nil {
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

// findCandidate returns index of segment that should be cleaned as next one,
// index of first newer segment to start cleanup from, index of last newer segment
// to finish cleanup on, callback to be executed after cleanup is successfully completed
// and error in case of issues occurred while finding candidate
func (c *segmentCleanerCommon) findCandidate(segments []Segment) (int, int, int, onCompletedFunc, error) {
	nowTs := time.Now().UnixNano()
	nextAllowedTs := nowTs - int64(c.sg.cleanupInterval)
	nextAllowedStoredTs := c.readNextAllowed()

	if nextAllowedStoredTs > nextAllowedTs {
		// too soon for next cleanup
		return emptyIdx, emptyIdx, emptyIdx, nil, nil
	}

	ids, sizes, err := c.getSegmentIdsAndSizes(segments)
	if err != nil {
		return emptyIdx, emptyIdx, emptyIdx, nil, err
	}
	if count := len(ids); count <= 1 {
		// too few segments for cleanup, update next allowed timestamp for cleanup to now
		if err := c.storeNextAllowed(nowTs); err != nil {
			return emptyIdx, emptyIdx, emptyIdx, nil, err
		}
		return emptyIdx, emptyIdx, emptyIdx, nil, nil
	}

	// get idx and cleanup timestamp of earliest cleaned segment,
	// take the opportunity to find obsolete segment keys to be deleted later from cleanup db
	candidateIdx, startIdx, lastIdx, earliestCleanedTs, nonExistentSegmentKeys := c.readEarliestCleaned(ids, sizes, nowTs)

	if err := c.deleteSegmentMetas(nonExistentSegmentKeys); err != nil {
		return emptyIdx, emptyIdx, emptyIdx, nil, err
	}

	if candidateIdx != emptyIdx && earliestCleanedTs <= nextAllowedTs {
		// candidate found and ready for cleanup
		id := ids[candidateIdx]
		lastProcessedId := ids[len(ids)-1]
		onCompleted := func(size int64) error {
			return c.storeSegmentMeta(id, lastProcessedId, size, nowTs)
		}
		return candidateIdx, startIdx, lastIdx, onCompleted, nil
	}

	// candidate not found or not ready for cleanup, update next allowed timestamp to earliest cleaned segment
	// (which is "now" if candidate was not found)
	if err := c.storeNextAllowed(earliestCleanedTs); err != nil {
		return emptyIdx, emptyIdx, emptyIdx, nil, err
	}

	return emptyIdx, emptyIdx, emptyIdx, nil, nil
}

func (c *segmentCleanerCommon) getSegmentIdsAndSizes(segments []Segment) ([]int64, []int64, error) {
	var ids []int64
	var sizes []int64
	if count := len(segments); count > 1 {
		ids = make([]int64, count)
		sizes = make([]int64, count)

		for i, seg := range segments {
			idStr := segmentID(seg.getPath())
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("parse segment id %q: %w", idStr, err)
			}
			ids[i] = id
			sizes[i] = seg.Size()
		}
	}

	return ids, sizes, nil
}

func (c *segmentCleanerCommon) readNextAllowed() int64 {
	ts := int64(0)
	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupDbBucketMeta)
		v := b.Get(cleanupDbKeyMetaNextAllowedTs)
		if v != nil {
			ts = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return ts
}

func (c *segmentCleanerCommon) storeNextAllowed(ts int64) error {
	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupDbBucketMeta)
		bufV := make([]byte, 8)

		binary.BigEndian.PutUint64(bufV, uint64(ts))
		return b.Put(cleanupDbKeyMetaNextAllowedTs, bufV)
	}); err != nil {
		return fmt.Errorf("updating cleanup bolt db %q: %w", c.db.Path(), err)
	}
	return nil
}

func (c *segmentCleanerCommon) deleteSegmentMetas(segIds [][]byte) error {
	if len(segIds) > 0 {
		if err := c.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(cleanupDbBucketSegments)
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

// based of data stored in cleanup bolt db and existing segments in filesystem
// method returns:
// - index of candidate segment best suitable for cleanup,
// - index of segment, cleanup of candidate should be started from,
// - index of segment, cleanup of candidate should be finished on,
// - time of previous candidate's cleanup,
// - list of segmentIds stored in cleanup bolt db that no longer exist in filesystem
// - error (if occurred).
//
// First candidate to be returned is segment that was not cleaned before (if multiple
// uncleaned segments exist - the oldest one is returned).
// If there is no unclean segment, segment that was cleaned as the earliest is returned.
// For segment already cleaned before to be returned, new segments must have been created
// after previous cleanup and sum of their sizes should be greater than [minCleanupSizePercent]
// percent of size of cleaned segment, to increase the chance of segment being actually cleaned,
// not just copied.
func (c *segmentCleanerCommon) readEarliestCleaned(ids, sizes []int64, nowTs int64,
) (int, int, int, int64, [][]byte) {
	earliestCleanedTs := nowTs
	candidateIdx := emptyIdx
	startIdx := emptyIdx
	lastIdx := emptyIdx

	count := len(ids)
	nonExistentSegmentKeys := [][]byte{}
	emptyId := int64(-1)

	c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupDbBucketSegments)
		cur := b.Cursor()

		// Loop through all segmentIds, the ones stored in cleanup db (cur)
		// and ones currently existing in filesystem (ids).
		// Note: both sets of segmentIds may have unique elements:
		// - cursor can contain segmentIds of segments already removed (by compaction)
		// - ids can contain segmentIds of newly created segments
		// Note: both sets are ordered, therefore in case of one element is missing
		// in set, only this set advances to next element
		idx := 0
		key, val := cur.First()
		for idx < count-1 || key != nil {
			id := emptyId
			storedId := emptyId

			if idx < count-1 {
				id = ids[idx]
			}
			if key != nil {
				storedId = int64(binary.BigEndian.Uint64(key))
			}

			// segment with segmentId stored in cleanup db (storedId) no longer exists,
			if id == emptyId || (storedId != emptyId && id > storedId) {
				// entry to be deleted
				nonExistentSegmentKeys = append(nonExistentSegmentKeys, key)
				// advance cursor
				key, val = cur.Next()
				continue
			}

			// segment with segmentId in filesystem (id) has no entry in cleanup db,
			if storedId == emptyId || (id != emptyId && id < storedId) {
				// as segment was not cleaned before (timestamp == 0), it becomes best
				// candidate for next cleanup.
				// (if there are more segments not yet cleaned, 1st one is selected)
				if earliestCleanedTs > 0 {
					earliestCleanedTs = 0
					candidateIdx = idx
					startIdx = idx + 1
					lastIdx = count - 1
				}
				// advance index
				idx++
				continue
			}

			// segmentId present in both sets, had to be cleaned before
			// id == cid

			storedCleanedTs := int64(binary.BigEndian.Uint64(val[0:8]))
			// check if cleaned before current candidate
			if earliestCleanedTs > storedCleanedTs {
				lastId := ids[count-1]
				storedLastId := int64(binary.BigEndian.Uint64(val[8:16]))
				// check if new segments created after last cleanup
				if storedLastId < lastId {
					// last segment's id in filesystem is higher than last id used for cleanup
					size := sizes[idx]
					storedSize := int64(binary.BigEndian.Uint64(val[16:24]))

					// In general segment could be cleaned considering only segments created
					// after its last cleanup. One exception is when segment was compacted
					// (previous and current sizes differ).
					// As after compaction cleanup db will contain only entry of right segment,
					// not the left one, it is unknown what was last segment used for cleanup of removed
					// left segment, therefore compacted segment will be cleaned again using all newer segments.
					possibleStartIdx := idx + 1
					// in case of using segments that were already used for cleanup, process them in reverse
					// order starting with newest ones, to maximize the chance of finding redundant entries
					// as soon as possible (leaving segments that were already used for cleanup as last ones)
					reverseOrder := true
					if size == storedSize {
						reverseOrder = false
						// size not changed (not compacted), clean using only newly created segments,
						// skipping segments already processed in previous cleanup
						for i := idx + 1; i < count; i++ {
							possibleStartIdx = i
							if ids[i] > storedLastId {
								break
							}
						}
					}

					// segment should be cleaned only if sum of sizes of segments to be cleaned
					// with exceeds [minCleanupSizePercent] of its current size, to increase
					// probability of redunand keys.
					sumSize := int64(0)
					for i := possibleStartIdx; i < count; i++ {
						sumSize += sizes[i]
					}
					if size*minCleanupSizePercent/100 <= sumSize {
						earliestCleanedTs = storedCleanedTs
						candidateIdx = idx
						startIdx = possibleStartIdx
						lastIdx = count - 1

						if reverseOrder {
							startIdx, lastIdx = lastIdx, startIdx
						}
					}
				}
			}
			// advance cursor and index
			key, val = cur.Next()
			idx++
		}
		return nil
	})
	return candidateIdx, startIdx, lastIdx, earliestCleanedTs, nonExistentSegmentKeys
}

func (c *segmentCleanerCommon) storeSegmentMeta(id, lastProcessedId, size, cleanedTs int64) error {
	bufK := make([]byte, 8)
	binary.BigEndian.PutUint64(bufK, uint64(id))

	bufV := make([]byte, 24)
	binary.BigEndian.PutUint64(bufV[0:8], uint64(cleanedTs))
	binary.BigEndian.PutUint64(bufV[8:16], uint64(lastProcessedId))
	binary.BigEndian.PutUint64(bufV[16:24], uint64(size))

	if err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cleanupDbBucketSegments)
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

	// While any edit operation has pending segments, cleanup is driven by the
	// pending set (every pending segment must be rewritten through the
	// transformer), bypassing the time/size heuristic and the cleanupInterval.
	// Normal cleanup resumes once there are no pending segments.
	if c.sg.editOps != nil && c.sg.strategy == StrategyReplace {
		cleaned, handled, err := c.cleanupOnceEditOps(shouldAbort)
		if handled || err != nil {
			return cleaned, err
		}
	}

	var candidateIdx, startIdx, lastIdx int
	var onCompleted onCompletedFunc
	var tmpSegmentPath string

	ok, err := func() (bool, error) {
		segments, release := c.sg.getConsistentViewOfSegments()
		defer release()

		var err error
		candidateIdx, startIdx, lastIdx, onCompleted, err = c.findCandidate(segments)
		if err != nil {
			return false, err
		}
		if candidateIdx == emptyIdx {
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

		oldSegment := segments[candidateIdx]
		segmentId := segmentID(oldSegment.getPath())
		var filename string
		if c.sg.writeSegmentInfoIntoFileName {
			filename = "segment-" + segmentId + segmentExtraInfo(oldSegment.getLevel(), oldSegment.getStrategy()) + ".db.tmp"
		} else {
			filename = "segment-" + segmentId + ".db.tmp"
		}
		tmpSegmentPath = filepath.Join(c.sg.dir, filename)
		start := time.Now()
		c.sg.logger.WithFields(logrus.Fields{
			"action":       "lsm_cleanup",
			"path":         c.sg.dir,
			"candidateIdx": candidateIdx,
			"startIdx":     startIdx,
			"lastIdx":      lastIdx,
			"segmentId":    segmentId,
		}).Info("cleanup started with candidate")
		defer func() {
			l := c.sg.logger.WithFields(logrus.Fields{
				"action":    "lsm_cleanup",
				"path":      c.sg.dir,
				"segmentId": segmentId,
				"took":      time.Since(start),
			})
			if err == nil {
				l.Info("clenaup finished")
			} else {
				l.WithError(err).Error("cleanup failed")
			}
		}()

		file, err := os.Create(tmpSegmentPath)
		if err != nil {
			return false, err
		}

		switch c.sg.strategy {
		case StrategyReplace:
			var transformer valueTransformer
			if c.sg.editOps != nil {
				// This is the heuristic (tombstone) cleanup path, which picks its
				// own candidate rather than draining the edit-ops pending set, so it
				// neither re-queues (no merge) nor marks anything done. That is safe
				// because a single-segment cleanup keeps the segment ID
				// (replaceSegment -> switchOnDisk strips .tmp back to the same
				// segment-<id>.db): any edit-ops pending row for this segment still
				// points at a live, now-stripped segment, so the edit-ops cleanup
				// driver re-cleans it idempotently and then marks it done. The ID
				// never changes here, so a pending row can never be orphaned onto a
				// missing segment.
				transformer, _, err = c.sg.editOps.BuildCurrentTransformer()
				if err != nil {
					return false, err
				}
			}
			c := newSegmentCleanerReplace(file, oldSegment.newCursor(),
				c.sg.makeKeyExistsOnUpperSegments(segments, startIdx, lastIdx), oldSegment.getLevel(),
				oldSegment.getSecondaryIndexCount(), c.sg.enableChecksumValidation, transformer)
			if err = c.do(shouldAbort); err != nil {
				return false, err
			}
		default:
			err = fmt.Errorf("unsported strategy %q", c.sg.strategy)
			return false, err
		}

		if err = file.Sync(); err != nil {
			err = fmt.Errorf("fsync cleaned segment file: %w", err)
			return false, err
		}
		if err = file.Close(); err != nil {
			err = fmt.Errorf("close cleaned segment file: %w", err)
			return false, err
		}
		return true, nil
	}()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	segment, err := c.sg.replaceSegment(candidateIdx, tmpSegmentPath)
	if err != nil {
		err = fmt.Errorf("replace compacted segments: %w", err)
		return false, err
	}
	if err = onCompleted(segment.size); err != nil {
		err = fmt.Errorf("callback cleaned segment file: %w", err)
		return false, err
	}

	return true, nil
}

// maxCleanupAttempts caps per-segment rewrite retries before quarantine (C6), so
// a permanently-failing segment can't retry forever.
const maxCleanupAttempts = 5

// cleanupOnceEditOps drives cleanup from the edit-ops pending set. It rewrites
// every pending segment through the per-pass transformer, marking each done on
// success and bumping/quarantining on failure. handled is false (so the caller
// falls back to the default heuristic) only when there are no pending segments.
//
// Marking an op done is sound: every op in the pending snapshot was registered
// before BuildCurrentTransformer read the live ops, so the transformer strips its
// target — we never clear a row whose data wasn't actually stripped. The
// rewrite -> replaceSegment -> markRowsDone sequence is not one transaction, but
// it is crash-safe: cleanup keeps the segment ID, so a crash after replaceSegment
// leaves a stripped segment under the same ID with stale pending rows that the
// next pass re-cleans idempotently (no S8-style orphaning onto a renamed output).
func (c *segmentCleanerCommon) cleanupOnceEditOps(shouldAbort cyclemanager.ShouldAbortCallback,
) (cleaned bool, handled bool, err error) {
	pending, err := c.sg.editOps.AllPending()
	if err != nil {
		return false, true, fmt.Errorf("load pending edit-op segments: %w", err)
	}
	if len(pending) == 0 {
		return false, false, nil
	}

	transformer, _, err := c.sg.editOps.BuildCurrentTransformer()
	if err != nil {
		return false, true, err
	}
	if transformer == nil {
		// Pending rows but nothing to strip (no live ops / no builder). Rewriting
		// would be a passthrough and markRowsDone would then silently complete a
		// drop without stripping. Leave the rows for Reconcile / a later pass.
		return false, true, nil
	}

	// Group by segment so each is rewritten once even when several ops have it
	// pending — the transformer already covers all active ops.
	bySegment := map[string][]PendingSegment{}
	order := make([]string, 0, len(pending))
	for _, p := range pending {
		if _, ok := bySegment[p.SegmentID]; !ok {
			order = append(order, p.SegmentID)
		}
		bySegment[p.SegmentID] = append(bySegment[p.SegmentID], p)
	}

	for _, segID := range order {
		if shouldAbort() {
			break
		}
		rows := bySegment[segID]

		idx, tmpPath, found, cerr := c.cleanPendingSegmentToTmp(segID, transformer, shouldAbort)
		if cerr != nil {
			if errors.Is(cerr, errCleanupPaused) {
				// Memory pressure: stop the pass without bumping attempts so a
				// transient low-memory window can't quarantine a healthy segment.
				c.sg.logger.WithField("action", "lsm_cleanup_editops").
					WithField("path", c.sg.dir).
					Warn("pausing edit-ops cleanup due to memory pressure")
				break
			}
			if e := c.bumpOrQuarantine(rows, cerr); e != nil {
				return cleaned, true, e
			}
			continue
		}
		if !found {
			// No longer in the live segment set (merged away by a concurrent
			// compaction); drop the stale rows.
			if e := c.markRowsDone(rows); e != nil {
				return cleaned, true, e
			}
			cleaned = true
			continue
		}
		if _, e := c.sg.replaceSegment(idx, tmpPath); e != nil {
			return cleaned, true, fmt.Errorf("replace cleaned segment: %w", e)
		}
		if e := c.markRowsDone(rows); e != nil {
			return cleaned, true, e
		}
		cleaned = true
	}

	return cleaned, true, nil
}

// cleanPendingSegmentToTmp locates the segment by id and rewrites it (minus keys
// shadowed by newer segments, transformer applied) into a .tmp file. found is
// false when the segment is no longer in the live segment set (merged away by a
// concurrent compaction); idx is consumed by replaceSegment. Returns
// errCleanupPaused if memory pressure should pause the pass before the rewrite.
func (c *segmentCleanerCommon) cleanPendingSegmentToTmp(segID string,
	transformer valueTransformer, shouldAbort cyclemanager.ShouldAbortCallback,
) (idx int, tmpPath string, found bool, err error) {
	segments, release := c.sg.getConsistentViewOfSegments()
	defer release()

	idx = emptyIdx
	for i, seg := range segments {
		if segmentID(seg.getPath()) == segID {
			idx = i
			break
		}
	}
	if idx == emptyIdx {
		return emptyIdx, "", false, nil
	}

	// Mirror the heuristic path's OOM guard: a rewrite produces a full copy of the
	// segment, so under memory pressure pause the pass (errCleanupPaused) rather
	// than push the system further. A pause must not count as a failed attempt.
	if c.sg.allocChecker != nil {
		if err := c.sg.allocChecker.CheckAlloc(cleanupAllocCheckBytes); err != nil {
			return emptyIdx, "", false, errCleanupPaused
		}
	}

	oldSegment := segments[idx]
	var filename string
	if c.sg.writeSegmentInfoIntoFileName {
		filename = "segment-" + segID + segmentExtraInfo(oldSegment.getLevel(), oldSegment.getStrategy()) + ".db.tmp"
	} else {
		filename = "segment-" + segID + ".db.tmp"
	}
	tmpPath = filepath.Join(c.sg.dir, filename)

	file, err := os.Create(tmpPath)
	if err != nil {
		return emptyIdx, "", false, err
	}

	// The last segment has no newer segments to shadow it; it is rewritten
	// purely to apply the transformer.
	var keyExists keyExistsOnUpperSegmentsFunc
	if idx >= len(segments)-1 {
		keyExists = func([]byte) (bool, error) { return false, nil }
	} else {
		keyExists = c.sg.makeKeyExistsOnUpperSegments(segments, idx+1, len(segments)-1)
	}

	cleaner := newSegmentCleanerReplace(file, oldSegment.newCursor(), keyExists,
		oldSegment.getLevel(), oldSegment.getSecondaryIndexCount(),
		c.sg.enableChecksumValidation, transformer)
	if err := cleaner.do(shouldAbort); err != nil {
		file.Close()
		return emptyIdx, "", false, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return emptyIdx, "", false, fmt.Errorf("fsync cleaned segment file: %w", err)
	}
	if err := file.Close(); err != nil {
		return emptyIdx, "", false, fmt.Errorf("close cleaned segment file: %w", err)
	}

	return idx, tmpPath, true, nil
}

func (c *segmentCleanerCommon) markRowsDone(rows []PendingSegment) error {
	for _, p := range rows {
		if err := c.sg.editOps.MarkSegmentDone(p.OpID, p.SegmentID); err != nil {
			return err
		}
	}
	return nil
}

// bumpOrQuarantine records a failed rewrite for each op that had the segment
// pending, quarantining it once it has failed maxCleanupAttempts times (C6).
func (c *segmentCleanerCommon) bumpOrQuarantine(rows []PendingSegment, cause error) error {
	for _, p := range rows {
		if err := c.sg.editOps.BumpAttempt(p.OpID, p.SegmentID, cause); err != nil {
			return err
		}
		if p.Attempts+1 >= maxCleanupAttempts {
			if err := c.sg.editOps.Quarantine(p.OpID, p.SegmentID); err != nil {
				return err
			}
		}
	}
	return nil
}

type onCompletedFunc func(size int64) error

// ================================================================

type keyExistsOnUpperSegmentsFunc func(key []byte) (bool, error)

func (sg *SegmentGroup) makeKeyExistsOnUpperSegments(segments []Segment, startIdx, lastIdx int) keyExistsOnUpperSegmentsFunc {
	var upperSegments []Segment
	if startIdx <= lastIdx {
		upperSegments = segments[startIdx : lastIdx+1]
	} else {
		upperSegments = make([]Segment, startIdx-lastIdx+1)
		for i := lastIdx; i <= startIdx; i++ {
			upperSegments[startIdx-i] = segments[i]
		}
	}

	return func(key []byte) (bool, error) {
		for i := range upperSegments {
			if exists, err := upperSegments[i].existsKey(key); err != nil {
				return false, err
			} else if exists {
				return true, nil
			}
		}
		return false, nil
	}
}

func (sg *SegmentGroup) replaceSegment(segmentPos int, tmpSegmentPath string,
) (*segment, error) {
	newSegment, err := sg.preinitializeNewSegment(tmpSegmentPath, segmentPos)
	if err != nil {
		return nil, fmt.Errorf("precompute segment meta: %w", err)
	}

	replacer := newSegmentReplacer(sg, segmentPos, segmentPos, newSegment)
	_, oldSegment, err := replacer.switchOnDisk()
	if err != nil {
		return nil, fmt.Errorf("replace cleaned segment on disk: %w", err)
	}

	if err := replacer.switchInMemory(); err != nil {
		return nil, fmt.Errorf("replace cleaned segment (blocking): %w", err)
	}

	sg.addSegmentsToAwaitingDrop(oldSegment)

	return newSegment, nil
}
