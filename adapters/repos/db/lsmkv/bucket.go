//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/config"

	entcfg "github.com/weaviate/weaviate/entities/config"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// since v1.34 StrategyRoaringSet is default strategy for dimensions bucket,
// StrategyMapCollection is left as backward compatibility for buckets created earlier
var DimensionsBucketPrioritizedStrategies = []string{
	StrategyRoaringSet,
	StrategyMapCollection,
}

const (
	FlushAfterDirtyDefault = 60 * time.Second
	unsetStrategy          = "UNSET"
	contextCheckInterval   = 50 // check context every 50 iterations, every iteration adds too much overhead
)

type BucketCreator interface {
	NewBucket(ctx context.Context, dir, rootDir string, logger logrus.FieldLogger,
		metrics *Metrics, compactionCallbacks, flushCallbacks cyclemanager.CycleCallbackGroup,
		opts ...BucketOption,
	) (*Bucket, error)
}

type Bucket struct {
	dir      string
	rootDir  string
	active   memtable
	flushing memtable
	disk     *SegmentGroup
	logger   logrus.FieldLogger

	// Lock() means a move from active to flushing is happening, RLock() is
	// normal operation
	flushLock        sync.RWMutex
	haltedFlushTimer *interval.BackoffTimer

	minWalThreshold   uint64
	walThreshold      uint64
	flushDirtyAfter   time.Duration
	memtableThreshold uint64
	minMMapSize       int64
	memtableResizer   *memtableSizeAdvisor
	strategy          string
	// Strategy inverted index is supposed to be created with, but existing
	// segment files were created with different one.
	// It can happen when new strategy were introduced to weaviate, but
	// files are already created using old implementation.
	// Example: RoaringSet strategy replaces CollectionSet strategy.
	// Field can be used for migration files of old strategy to newer one.
	desiredStrategy  string
	secondaryIndices uint16

	// Optional to avoid syscalls
	mmapContents  bool
	writeMetadata bool

	// for backward compatibility
	legacyMapSortingBeforeCompaction bool

	flushCallbackCtrl cyclemanager.CycleCallbackCtrl

	status     storagestate.Status
	statusLock sync.RWMutex

	metrics *Metrics

	// all "replace" buckets support counting through net additions, but not all
	// produce a meaningful count. Typically, the only count we're interested in
	// is that of the bucket that holds objects
	monitorCount bool

	pauseTimer *prometheus.Timer // Times the pause

	// Whether tombstones (set/map/replace types) or deletions (roaringset type)
	// should be kept in root segment during compaction process.
	// Since segments are immutable, deletions are added as new entries with
	// tombstones. Tombstones are by default copied to merged segment, as they
	// can refer to keys/values present in previous segments.
	// Those tombstones can be removed entirely when merging with root (1st) segment,
	// due to lack of previous segments, tombstones may relate to.
	// As info about key/value being deleted (based on tombstone presence) may be important
	// for some use cases (e.g. replication needs to know if object(ObjectsBucketLSM) was deleted)
	// keeping tombstones on compaction is optional
	keepTombstones bool

	// Init and use bloom filter for getting key from bucket segments.
	// As some buckets can be accessed only with cursor (see flat index),
	// where bloom filter is not applicable, it can be disabled.
	// ON by default
	useBloomFilter bool

	// Net additions keep track of number of elements stored in bucket (of type replace).
	// As some buckets don't have to provide Count info (see flat index),
	// tracking additions can be disabled.
	// ON by default
	calcCountNetAdditions bool

	forceCompaction    bool
	disableCompaction  bool
	lazySegmentLoading bool

	// if true, don't increase the segment level during compaction.
	// useful for migrations, as it allows to merge reindex and ingest buckets
	// without discontinuities in segment levels.
	keepLevelCompaction bool

	// optionally supplied to prevent starting memory-intensive
	// processes when memory pressure is high
	allocChecker memwatch.AllocChecker

	// optional segment size limit. If set, a compaction will skip segments that
	// sum to more than the specified value.
	maxSegmentSize int64

	// optional segments cleanup interval. If set, segments will be cleaned of
	// redundant obsolete data, that was deleted or updated in newer segments
	// (currently supported only in buckets of REPLACE strategy)
	segmentsCleanupInterval time.Duration

	// optional validation of segment file checksums. Enabling this option
	// introduces latency of segment availability, for the tradeoff of
	// ensuring segment files have integrity before reading them.
	enableChecksumValidation bool

	// keep segments in memory for more performant search
	// (currently used by roaringsetrange inverted indexes)
	keepSegmentsInMemory bool

	// pool of buffers for bitmaps merges
	// (currently used by roaringsetrange inverted indexes)
	bitmapBufPool roaringset.BitmapBufPool

	// add information like the level and the strategy into the filename so these things can be checked without loading
	// the segment
	writeSegmentInfoIntoFileName bool

	bm25Config *models.BM25Config

	// function to decide whether a key should be skipped
	// during compaction for the SetCollection strategy
	shouldSkipKey func(key []byte, ctx context.Context) (bool, error)
}

func NewBucketCreator() *Bucket { return &Bucket{} }

// NewBucket initializes a new bucket. It either loads the state from disk if
// it exists, or initializes new state.
//
// You do not need to ever call NewBucket() yourself, if you are using a
// [Store]. In this case the [Store] can manage buckets for you, using methods
// such as CreateOrLoadBucket().
func (*Bucket) NewBucket(ctx context.Context, dir, rootDir string, logger logrus.FieldLogger,
	metrics *Metrics, compactionCallbacks, flushCallbacks cyclemanager.CycleCallbackGroup,
	opts ...BucketOption,
) (b *Bucket, err error) {
	beforeAll := time.Now()

	defaultMemTableThreshold := uint64(10 * 1024 * 1024)
	defaultWalThreshold := uint64(1024 * 1024 * 1024)
	defaultFlushAfterDirty := FlushAfterDirtyDefault
	// this is not the nicest way of doing this check, but we will make strategy a required parameter in the future on
	// main
	defaultStrategy := unsetStrategy

	b = &Bucket{
		dir:                          dir,
		rootDir:                      rootDir,
		memtableThreshold:            defaultMemTableThreshold,
		walThreshold:                 defaultWalThreshold,
		flushDirtyAfter:              defaultFlushAfterDirty,
		strategy:                     defaultStrategy,
		mmapContents:                 true,
		logger:                       logger,
		metrics:                      metrics,
		useBloomFilter:               true,
		calcCountNetAdditions:        false,
		haltedFlushTimer:             interval.NewBackoffTimer(),
		writeSegmentInfoIntoFileName: false,
		minWalThreshold:              config.DefaultPersistenceMaxReuseWalSize,
	}

	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	if b.strategy == unsetStrategy {
		return nil, errors.New("strategy needs to be explicitly set for all buckets")
	}

	if b.memtableResizer != nil {
		b.memtableThreshold = uint64(b.memtableResizer.Initial())
	}

	if b.disableCompaction {
		compactionCallbacks = cyclemanager.NewCallbackGroupNoop()
	}

	b.desiredStrategy = b.strategy

	metrics.IncBucketInitCountByStrategy(b.strategy)
	metrics.IncBucketInitInProgressByStrategy(b.strategy)

	defer func(strategy string) {
		metrics.DecBucketInitInProgressByStrategy(strategy)

		if err != nil {
			metrics.IncBucketInitFailureCountByStrategy(strategy)
			return
		}

		metrics.ObserveBucketInitDurationByStrategy(strategy, time.Since(beforeAll))
	}(b.strategy)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	files, _, err := diskio.GetFileWithSizes(dir)
	if err != nil {
		return nil, err
	}

	sg, err := newSegmentGroup(ctx, logger, metrics,
		sgConfig{
			dir:                          dir,
			strategy:                     b.strategy,
			mapRequiresSorting:           b.legacyMapSortingBeforeCompaction,
			monitorCount:                 b.monitorCount,
			mmapContents:                 b.mmapContents,
			keepTombstones:               b.keepTombstones,
			forceCompaction:              b.forceCompaction,
			useBloomFilter:               b.useBloomFilter,
			calcCountNetAdditions:        b.calcCountNetAdditions,
			maxSegmentSize:               b.maxSegmentSize,
			cleanupInterval:              b.segmentsCleanupInterval,
			enableChecksumValidation:     b.enableChecksumValidation,
			keepSegmentsInMemory:         b.keepSegmentsInMemory,
			MinMMapSize:                  b.minMMapSize,
			bm25config:                   b.bm25Config,
			keepLevelCompaction:          b.keepLevelCompaction,
			writeSegmentInfoIntoFileName: b.writeSegmentInfoIntoFileName,
			writeMetadata:                b.writeMetadata,
			shouldSkipKey:                b.shouldSkipKey,
		}, compactionCallbacks, b, files)
	if err != nil {
		return nil, fmt.Errorf("init disk segments: %w", err)
	}

	b.disk = sg

	if b.active == nil {
		b.active, err = b.createNewActiveMemtable()
		if err != nil {
			return nil, err
		}
	}

	id := "bucket/flush/" + b.dir
	b.flushCallbackCtrl = flushCallbacks.Register(id, b.flushAndSwitchIfThresholdsMet)

	b.metrics.TrackStartupBucket(beforeAll)

	if err := GlobalBucketRegistry.TryAdd(dir); err != nil {
		// prevent accidentally trying to register the same bucket twice
		return nil, err
	}

	return b, nil
}

func (b *Bucket) GetDir() string {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()
	return b.dir
}

func (b *Bucket) GetRootDir() string {
	return b.rootDir
}

func (b *Bucket) GetStrategy() string {
	return b.strategy
}

func (b *Bucket) GetDesiredStrategy() string {
	return b.desiredStrategy
}

func (b *Bucket) GetSecondaryIndices() uint16 {
	return b.secondaryIndices
}

func (b *Bucket) GetStatus() storagestate.Status {
	b.statusLock.RLock()
	defer b.statusLock.RUnlock()

	return b.status
}

func (b *Bucket) GetMemtableThreshold() uint64 {
	return b.memtableThreshold
}

func (b *Bucket) GetWalThreshold() uint64 {
	return b.walThreshold
}

func (b *Bucket) GetFlushCallbackCtrl() cyclemanager.CycleCallbackCtrl {
	return b.flushCallbackCtrl
}

func (b *Bucket) IterateObjects(ctx context.Context, f func(object *storobj.Object) error) error {
	cursor := b.Cursor()
	defer cursor.Close()

	i := 0

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		obj, err := storobj.FromBinary(v)
		if err != nil {
			return fmt.Errorf("cannot unmarshal object %d, %w", i, err)
		}
		if err := f(obj); err != nil {
			return fmt.Errorf("callback on object '%d' failed: %w", obj.DocID, err)
		}

		i++
	}

	return nil
}

func (b *Bucket) pauseCompaction(ctx context.Context) error {
	return b.disk.pauseCompaction(ctx)
}

func (b *Bucket) resumeCompaction(ctx context.Context) error {
	return b.disk.resumeCompaction(ctx)
}

// ApplyToObjectDigests iterates over all objects in the bucket, both in memtable
// and on disk, and applies the given function to each object.
// The afterInMemCallback is called after the in-memory memtable has been processed.
// This allows the caller to perform actions that need to happen after the in-memory
// objects have been processed.
// The function f is called for each object, and if it returns an error, the
// processing is stopped and the error is returned.
// Note: this function pauses compaction while it is running, to ensure a consistent view of the data.
func (b *Bucket) ApplyToObjectDigests(ctx context.Context,
	afterInMemCallback func(), f func(object *storobj.Object) error,
) error {
	err := b.pauseCompaction(ctx)
	if err != nil {
		afterInMemCallback()
		return fmt.Errorf("pausing compaction: %w", err)
	}
	defer func() {
		ec := errorcompounder.New()

		if err != nil {
			ec.AddWrapf(err, "during ApplyToObjectDigests")
		}

		err = b.resumeCompaction(ctx)
		if err != nil {
			ec.AddWrapf(err, "resuming compaction after ApplyToObjectDigests")
		}

		err = ec.ToError()
	}()

	// note: it's important to first create the on disk cursor so to avoid potential double scanning over flushing memtable
	onDiskCursor := b.CursorOnDisk()
	defer onDiskCursor.Close()

	inmemProcessedDocIDs := make(map[uint64]struct{})

	// note: read-write access to active and flushing memtable will be blocked only during the scope of this inner function
	err = func() error {
		defer afterInMemCallback()

		inMemCursor := b.CursorInMem()
		defer inMemCursor.Close()

		for k, v := inMemCursor.First(); k != nil; k, v = inMemCursor.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				obj, err := storobj.FromBinaryUUIDOnly(v)
				if err != nil {
					return fmt.Errorf("cannot unmarshal object: %w", err)
				}
				if err := f(obj); err != nil {
					return fmt.Errorf("callback on object '%d' failed: %w", obj.DocID, err)
				}

				inmemProcessedDocIDs[obj.DocID] = struct{}{}
			}
		}

		return nil
	}()
	if err != nil {
		return err
	}

	for k, v := onDiskCursor.First(); k != nil; k, v = onDiskCursor.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			obj, err := storobj.FromBinaryUUIDOnly(v)
			if err != nil {
				return fmt.Errorf("cannot unmarshal object: %w", err)
			}

			if _, ok := inmemProcessedDocIDs[obj.DocID]; ok {
				continue
			}

			if err := f(obj); err != nil {
				return fmt.Errorf("callback on object '%d' failed: %w", obj.DocID, err)
			}
		}
	}

	return nil
}

func (b *Bucket) IterateMapObjects(ctx context.Context, f func([]byte, []byte, []byte, bool) error) error {
	cursor, err := b.MapCursor()
	if err != nil {
		return fmt.Errorf("create map cursor: %w", err)
	}
	defer cursor.Close()

	for kList, vList := cursor.First(ctx); kList != nil; kList, vList = cursor.Next(ctx) {
		for _, v := range vList {
			if err := f(kList, v.Key, v.Value, v.Tombstone); err != nil {
				return fmt.Errorf("callback on object '%v' failed: %w", v, err)
			}
		}
	}

	return nil
}

func (b *Bucket) SetMemtableThreshold(size uint64) {
	b.memtableThreshold = size
}

type BucketConsistentView struct {
	Active   memtable
	Flushing memtable
	Disk     []Segment
	release  func()
}

func (cv *BucketConsistentView) Release() {
	cv.release()
}

func (b *Bucket) getConsistentView() BucketConsistentView {
	beforeFlushLock := time.Now()
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	if duration := time.Since(beforeFlushLock); duration > 100*time.Millisecond {
		b.logger.WithFields(logrus.Fields{
			"duration": duration,
			"action":   "lsm_bucket_get_acquire_flush_lock",
		}).Debug("Waited more than 100ms to obtain a flush lock during get")
	}

	diskSegments, releaseDiskSegments := b.disk.getConsistentViewOfSegments()
	return BucketConsistentView{
		Active:   b.active,
		Flushing: b.flushing,
		Disk:     diskSegments,
		release:  releaseDiskSegments,
	}
}

// Get retrieves the single value for the given key.
//
// Get is specific to ReplaceStrategy and cannot be used with any of the other
// strategies. Use [Bucket.SetList] or [Bucket.MapList] instead.
//
// Get uses the regular or "primary" key for an object. If a bucket has
// secondary indexes, use [Bucket.GetBySecondary] to retrieve an object using
// its secondary key
func (b *Bucket) Get(key []byte) ([]byte, error) {
	v, err := b.get(key)
	if err != nil && lsmkv.IsDeletedOrNotFound(err) {
		return nil, nil
	}
	return v, err
}

func (b *Bucket) GetErrDeleted(key []byte) ([]byte, error) {
	return b.get(key)
}

func (b *Bucket) get(key []byte) ([]byte, error) {
	view := b.getConsistentView()
	defer view.release()

	return b.getWithConsistentView(key, view)
}

func (b *Bucket) getWithConsistentView(key []byte, view BucketConsistentView) ([]byte, error) {
	memtableNames := []string{"active_memtable", "flushing_memtable"}
	memtables := []memtable{view.Active}
	if view.Flushing != nil {
		memtables = append(memtables, view.Flushing)
	}

	for i := range memtables {
		v, err := b.getFromMemtable(key, memtables[i], memtableNames[i])
		if err == nil {
			// item found and no error, return and stop searching, since the strategy
			// is replace
			return v, nil
		}
		if errors.Is(err, lsmkv.Deleted) {
			// deleted in the mem-table (which is always the latest) means we don't
			// have to check the disk segments, return nil now
			return nil, err
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, fmt.Errorf("Bucket::get() %q: %w", memtableNames[i], err)
		}
	}

	return b.getFromSegmentGroup(key, view.Disk)
}

// GetBySecondary retrieves an object using one of its secondary keys. A bucket
// can have an infinite number of secondary keys. Specify the secondary key
// position as the first argument.
//
// A real-life example of secondary keys is the Weaviate object store. Objects
// are stored with the user-facing ID as their primary key and with the doc-id
// (an ever-increasing uint64) as the secondary key.
//
// Similar to [Bucket.Get], GetBySecondary is limited to ReplaceStrategy. No
// equivalent exists for Set and Map, as those do not support secondary
// indexes.
func (b *Bucket) GetBySecondary(ctx context.Context, pos int, key []byte) ([]byte, error) {
	bytes, _, err := b.GetBySecondaryWithBuffer(ctx, pos, key, nil)
	return bytes, err
}

// GetBySecondaryWithBuffer is like [Bucket.GetBySecondary], but also takes a
// buffer. It's in the response of the caller to pool the buffer, since the
// bucket does not know when the caller is done using it. The return bytes will
// likely point to the same memory that's part of the buffer. However, if the
// buffer is to small, a larger buffer may also be returned (second arg).
//
// Similar to [Bucket.Get], GetBySecondaryWithBuffer is limited to ReplaceStrategy. No
// equivalent exists for Set and Map, as those do not support secondary
// indexes.
func (b *Bucket) GetBySecondaryWithBuffer(ctx context.Context, pos int, seckey []byte, buffer []byte) ([]byte, []byte, error) {
	v, allocBuf, err := b.getBySecondary(ctx, pos, seckey, buffer)
	if err != nil && lsmkv.IsDeletedOrNotFound(err) {
		return nil, buffer, nil
	}
	return v, allocBuf, err
}

func (b *Bucket) getBySecondary(ctx context.Context, pos int, seckey []byte, buffer []byte) ([]byte, []byte, error) {
	if pos >= int(b.secondaryIndices) {
		return nil, nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	beforeAll := time.Now()
	view := b.getConsistentView()
	defer view.release()
	tookView := time.Since(beforeAll)

	memtableNames := []string{"active_memtable", "flushing_memtable"}
	memtables := []memtable{view.Active}
	if view.Flushing != nil {
		memtables = append(memtables, view.Flushing)
	}

	var memtablesTook []time.Duration
	for i := range memtables {
		beforeMemtable := time.Now()
		v, err := b.getBySecondaryFromMemtable(pos, seckey, memtables[i], memtableNames[i])
		memtablesTook = append(memtablesTook, time.Since(beforeMemtable))
		if err == nil {
			// item found and no error, return and stop searching, since the strategy
			// is replace
			return v, buffer, nil
		}
		if errors.Is(err, lsmkv.Deleted) {
			// deleted in the mem-table (which is always the latest) means we don't
			// have to check the disk segments, return nil now
			return nil, nil, err
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, nil, fmt.Errorf("Bucket::getBySecondary() %q: %w", memtableNames[i], err)
		}
	}

	beforeSegments := time.Now()
	k, v, allocBuf, err := b.getBySecondaryFromSegmentGroup(pos, seckey, buffer, view.Disk)
	if err != nil {
		return nil, nil, err
	}
	segmentsTook := time.Since(beforeSegments)

	// additional validation to ensure the primary key has not been marked as deleted
	beforeReCheck := time.Now()
	if _, err := b.getWithConsistentView(k, view); err != nil {
		return nil, nil, err
	}
	recheckTook := time.Since(beforeReCheck)

	activeMemtableTook := memtablesTook[0]
	var flushingMemtableTook time.Duration
	if len(memtablesTook) > 1 {
		flushingMemtableTook = memtablesTook[1]
	}

	helpers.AnnotateSlowQueryLogAppend(ctx, "lsm_get_by_secondary", BucketSlowLogEntry{
		View:             tookView,
		ActiveMemtable:   activeMemtableTook,
		FlushingMemtable: flushingMemtableTook,
		Segments:         segmentsTook,
		Recheck:          recheckTook,
		Total:            time.Since(beforeAll),
	})

	return v, allocBuf, nil
}

func (b *Bucket) getFromMemtable(key []byte, memtable memtable, component string) (v []byte, err error) {
	op := "get"

	start := time.Now()
	b.metrics.IncBucketReadOpCountByComponent(op, component)
	b.metrics.IncBucketReadOpOngoingByComponent(op, component)

	defer func() {
		if duration := time.Since(start); duration > 100*time.Millisecond {
			b.logger.WithFields(logrus.Fields{
				"duration": duration,
				"action":   fmt.Sprintf("lsm_bucket_get_%s", component),
			}).Debug("Waited more than 100ms to retrieve object from memtable")
		}

		b.metrics.DecBucketReadOpOngoingByComponent(op, component)
		if err != nil && !lsmkv.IsDeletedOrNotFound(err) {
			b.metrics.IncBucketReadOpFailureCountByComponent(op, component)
			return
		}
		b.metrics.ObserveBucketReadOpDurationByComponent(op, component, time.Since(start))
	}()

	return memtable.get(key)
}

func (b *Bucket) getBySecondaryFromMemtable(pos int, seckey []byte, memtable memtable, component string,
) (v []byte, err error) {
	op := "getbysecondary"

	start := time.Now()
	b.metrics.IncBucketReadOpCountByComponent(op, component)
	b.metrics.IncBucketReadOpOngoingByComponent(op, component)

	defer func() {
		if duration := time.Since(start); duration > 100*time.Millisecond {
			b.logger.WithFields(logrus.Fields{
				"duration": duration,
				"action":   fmt.Sprintf("lsm_bucket_getbysecondary_%s", component),
			}).Debug("Waited more than 100ms to retrieve object from memtable")
		}

		b.metrics.DecBucketReadOpOngoingByComponent(op, component)
		if err != nil && !lsmkv.IsDeletedOrNotFound(err) {
			b.metrics.IncBucketReadOpFailureCountByComponent(op, component)
			return
		}
		b.metrics.ObserveBucketReadOpDurationByComponent(op, component, time.Since(start))
	}()

	return memtable.getBySecondary(pos, seckey)
}

func (b *Bucket) getFromSegmentGroup(key []byte, segments []Segment) (v []byte, err error) {
	op := "get"
	component := "segment_group"

	start := time.Now()
	b.metrics.IncBucketReadOpCountByComponent(op, component)
	b.metrics.IncBucketReadOpOngoingByComponent(op, component)

	defer func() {
		b.metrics.DecBucketReadOpOngoingByComponent(op, component)
		if err != nil && !lsmkv.IsDeletedOrNotFound(err) {
			b.metrics.IncBucketReadOpFailureCountByComponent(op, component)
			return
		}
		b.metrics.ObserveBucketReadOpDurationByComponent(op, component, time.Since(start))
	}()

	return b.disk.getWithSegmentList(key, segments)
}

func (b *Bucket) getBySecondaryFromSegmentGroup(pos int, seckey []byte, buffer []byte, segments []Segment,
) (k, v []byte, buf []byte, err error) {
	op := "getbysecondary"
	component := "segment_group"

	start := time.Now()
	b.metrics.IncBucketReadOpCountByComponent(op, component)
	b.metrics.IncBucketReadOpOngoingByComponent(op, component)

	defer func() {
		b.metrics.DecBucketReadOpOngoingByComponent(op, component)
		if err != nil && !lsmkv.IsDeletedOrNotFound(err) {
			b.metrics.IncBucketReadOpFailureCountByComponent(op, component)
			return
		}
		b.metrics.ObserveBucketReadOpDurationByComponent(op, component, time.Since(start))
	}()

	return b.disk.getBySecondaryWithSegmentList(pos, seckey, buffer, segments)
}

// SetList returns all Set entries for a given key.
//
// SetList is specific to the Set Strategy, for Map use [Bucket.MapList], and
// for Replace use [Bucket.Get].
func (b *Bucket) SetList(key []byte) ([][]byte, error) {
	view := b.getConsistentView()
	defer view.Release()

	return b.setListFromConsistentView(view, key)
}

func (b *Bucket) setListFromConsistentView(view BucketConsistentView, key []byte) ([][]byte, error) {
	var out []value

	v, err := b.disk.getCollection(key, view.Disk)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	out = v

	if view.Flushing != nil {
		v, err = view.Flushing.getCollection(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
		out = append(out, v...)

	}

	v, err = view.Active.getCollection(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	if len(v) > 0 {
		// skip the expensive append operation if there was no memtable
		out = append(out, v...)
	}

	return newSetDecoder().Do(out), nil
}

// Put creates or replaces a single value for a given key.
//
//	err := bucket.Put([]byte("my_key"), []byte("my_value"))
//	 if err != nil {
//		/* do something */
//	}
//
// If a bucket has a secondary index configured, you can also specify one or
// more secondary keys, like so:
//
//	err := bucket.Put([]byte("my_key"), []byte("my_value"),
//		WithSecondaryKey(0, []byte("my_alternative_key")),
//	)
//	 if err != nil {
//		/* do something */
//	}
//
// Put is limited to ReplaceStrategy, use [Bucket.SetAdd] for Set or
// [Bucket.MapSet] and [Bucket.MapSetMulti].
func (b *Bucket) Put(key, value []byte, opts ...SecondaryKeyOption) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("put")
	b.metrics.IncBucketWriteOpOngoing("put")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("put")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("put")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("put", time.Since(start))
	}()

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.put(key, value, opts...)
}

// getActiveMemtableForWrite returns the active memtable and uses reference
// counting to avoid holding a lock during the entire duration of the write.
//
// It is safe to switch the memtable from active to flushing while a writer
// is ongoing, because the actual flush only happens once the writer count
// has dropped to zero. Essentially the switch just switches pointers, but we
// will always work on the same pointer for the duration of the write.
func (b *Bucket) getActiveMemtableForWrite() (active memtable, release func()) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	active = b.active
	active.incWriterCount()

	release = func() {
		active.decWriterCount()
	}

	return active, release
}

// SetAdd adds one or more Set-Entries to a Set for the given key. SetAdd is
// entirely agnostic of existing entries, it acts as append-only. This also
// makes it agnostic of whether the key already exists or not.
//
// Example to add two entries to a set:
//
//	err := bucket.SetAdd([]byte("my_key"), [][]byte{
//		[]byte("one-set-element"), []byte("another-set-element"),
//	})
//	if err != nil {
//		/* do something */
//	}
//
// SetAdd is specific to the Set strategy. For Replace, use [Bucket.Put], for
// Map use either [Bucket.MapSet] or [Bucket.MapSetMulti].
func (b *Bucket) SetAdd(key []byte, values [][]byte) error {
	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.append(key, newSetEncoder().Do(values))
}

// SetDeleteSingle removes one Set element from the given key. Note that LSM
// stores are append only, thus internally this action appends a tombstone. The
// entry will not be removed until a compaction has run, and even then a
// compaction does not guarantee the removal of the data right away. This is
// because an entry could have been created in an older segment than those
// present in the compaction. This can be seen as an implementation detail,
// unless the caller expects to free disk space by calling this method. Such
// freeing is not guaranteed.
//
// SetDeleteSingle is specific to the Set Strategy. For Replace, you can use
// [Bucket.Delete] to delete the entire row, for Maps use [Bucket.MapDeleteKey]
// to delete a single map entry.
func (b *Bucket) SetDeleteSingle(key []byte, valueToDelete []byte) error {
	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.append(key, []value{
		{
			value:     valueToDelete,
			tombstone: true,
		},
	})
}

// WasDeleted determines if an object used to exist in the LSM store
//
// There are 3 different locations that we need to check for the key
// in this order: active memtable, flushing memtable, and disk
// segment
func (b *Bucket) WasDeleted(key []byte) (bool, time.Time, error) {
	if !b.keepTombstones {
		return false, time.Time{}, fmt.Errorf("Bucket requires option `keepTombstones` set to check deleted keys")
	}

	if _, err := b.get(key); err != nil {
		if errors.Is(err, lsmkv.Deleted) {
			var errDeleted lsmkv.ErrDeleted
			if errors.As(err, &errDeleted) {
				return true, errDeleted.DeletionTime(), nil
			}
			return true, time.Time{}, nil
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return false, time.Time{}, fmt.Errorf("unsupported bucket error: %w", err)
		}
	}
	return false, time.Time{}, nil
}

type MapListOptionConfig struct {
	acceptDuplicates           bool
	legacyRequireManualSorting bool
}

type MapListOption func(c *MapListOptionConfig)

func MapListAcceptDuplicates() MapListOption {
	return func(c *MapListOptionConfig) {
		c.acceptDuplicates = true
	}
}

func MapListLegacySortingRequired() MapListOption {
	return func(c *MapListOptionConfig) {
		c.legacyRequireManualSorting = true
	}
}

// MapList returns all map entries for a given row key. The order of map pairs
// has no specific meaning. For efficient merge operations, pair entries are
// stored sorted on disk, however that is an implementation detail and not a
// caller-facing guarantee.
//
// MapList is specific to the Map strategy, for Sets use [Bucket.SetList], for
// Replace use [Bucket.Get].
func (b *Bucket) MapList(ctx context.Context, key []byte, cfgs ...MapListOption) ([]MapPair, error) {
	view := b.getConsistentView()
	defer view.Release()

	return b.mapListFromConsistentView(ctx, view, key, cfgs...)
}

func (b *Bucket) mapListFromConsistentView(ctx context.Context, view BucketConsistentView,
	key []byte, cfgs ...MapListOption,
) ([]MapPair, error) {
	c := MapListOptionConfig{}
	for _, cfg := range cfgs {
		cfg(&c)
	}

	entriesPerSegment := [][]MapPair{}
	// before := time.Now()
	plists, segmentsDisk, err := b.disk.getCollectionAndSegments(ctx, key, view.Disk)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	// segmentsDisk would be the same as view.Segments on StrategyInverted, but
	// differs on StrategyMapCollection where we would only contain segments that
	// have the key.

	allTombstones, err := b.loadAllTombstones(view)
	if err != nil {
		return nil, err
	}

	for i, plist := range plists {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		segmentStrategy := segmentsDisk[i].getStrategy()

		propLengths := make(map[uint64]uint32)
		if segmentStrategy == segmentindex.StrategyInverted {
			propLengths, err = segmentsDisk[i].getPropertyLengths()
			if err != nil {
				return nil, err
			}
		}

		segmentDecoded := make([]MapPair, len(plist))
		for j, v := range plist {
			// Inverted segments have a slightly different internal format
			// and separate property lengths that need to be read.
			if segmentStrategy == segmentindex.StrategyInverted {
				if err := segmentDecoded[j].FromBytesInverted(v.value, false); err != nil {
					return nil, err
				}
				docId := binary.BigEndian.Uint64(segmentDecoded[j].Key[:8])
				// check if there are any tombstones between the i and len(disk) segments
				for _, tombstones := range allTombstones[i+1:] {
					if tombstones != nil && tombstones.Contains(docId) {
						segmentDecoded[j].Tombstone = true
						break
					}
				}
				// put the property length in the value from the "external" property lengths
				binary.LittleEndian.PutUint32(segmentDecoded[j].Value[4:], math.Float32bits(float32(propLengths[docId])))

			} else {
				if err := segmentDecoded[j].FromBytes(v.value, false); err != nil {
					return nil, err
				}
				// Read "broken" tombstones with length 12 but a non-tombstone value
				// Related to Issue #4125
				// TODO: Remove the extra check, as it may interfere future in-disk format changes
				segmentDecoded[j].Tombstone = v.tombstone || len(v.value) == 12
			}
		}
		if len(segmentDecoded) > 0 {
			entriesPerSegment = append(entriesPerSegment, segmentDecoded)
		}
	}

	// fmt.Printf("--map-list: get all disk segments took %s\n", time.Since(before))

	// before = time.Now()
	// fmt.Printf("--map-list: append all disk segments took %s\n", time.Since(before))

	if view.Flushing != nil {
		v, err := view.Flushing.getMap(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
		if len(v) > 0 {
			entriesPerSegment = append(entriesPerSegment, v)
		}
	}

	// before = time.Now()
	v, err := view.Active.getMap(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	if len(v) > 0 {
		entriesPerSegment = append(entriesPerSegment, v)
	}
	// fmt.Printf("--map-list: get all active segments took %s\n", time.Since(before))

	// before = time.Now()
	// defer func() {
	// 	fmt.Printf("--map-list: run decoder took %s\n", time.Since(before))
	// }()

	if c.legacyRequireManualSorting {
		// Sort to support segments which were stored in an unsorted fashion
		for i := range entriesPerSegment {
			sort.Slice(entriesPerSegment[i], func(a, b int) bool {
				return bytes.Compare(entriesPerSegment[i][a].Key, entriesPerSegment[i][b].Key) == -1
			})
		}
	}

	return newSortedMapMerger().do(ctx, entriesPerSegment)
}

func (b *Bucket) loadAllTombstones(view BucketConsistentView) ([]*sroar.Bitmap, error) {
	hasTombstones := false
	allTombstones := make([]*sroar.Bitmap, len(view.Disk)+2)
	for i, segment := range view.Disk {
		if segment.getStrategy() == segmentindex.StrategyInverted {
			tombstones, err := segment.ReadOnlyTombstones()
			if err != nil {
				return nil, err
			}
			allTombstones[i] = tombstones
			hasTombstones = true
		}
	}
	if hasTombstones {
		if view.Flushing != nil {
			tombstones, err := view.Flushing.ReadOnlyTombstones()
			if err != nil {
				return nil, err
			}
			allTombstones[len(view.Disk)] = tombstones
		}

		tombstones, err := view.Active.ReadOnlyTombstones()
		if err != nil {
			return nil, err
		}
		allTombstones[len(view.Disk)+1] = tombstones
	}
	return allTombstones, nil
}

// MapSet writes one [MapPair] into the map for the given row key. It is
// agnostic of whether the row key already exists, as well as agnostic of
// whether the map key already exists. In both cases it will create the entry
// if it does not exist or override if it does.
//
// Example to add a new MapPair:
//
//	pair := MapPair{Key: []byte("Jane"), Value: []byte("Backend")}
//	err := bucket.MapSet([]byte("developers"), pair)
//	if err != nil {
//		/* do something */
//	}
//
// MapSet is specific to the Map Strategy, for Replace use [Bucket.Put], and for Set use [Bucket.SetAdd] instead.
func (b *Bucket) MapSet(rowKey []byte, kv MapPair) error {
	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.appendMapSorted(rowKey, kv)
}

// MapSetMulti is the same as [Bucket.MapSet], except that it takes in multiple
// [MapPair] objects at the same time.
func (b *Bucket) MapSetMulti(rowKey []byte, kvs []MapPair) error {
	active, release := b.getActiveMemtableForWrite()
	defer release()

	for _, kv := range kvs {
		if err := active.appendMapSorted(rowKey, kv); err != nil {
			return err
		}
	}

	return nil
}

// MapDeleteKey removes one key-value pair from the given map row. Note that
// LSM stores are append only, thus internally this action appends a tombstone.
// The entry will not be removed until a compaction has run, and even then a
// compaction does not guarantee the removal of the data right away. This is
// because an entry could have been created in an older segment than those
// present in the compaction. This can be seen as an implementation detail,
// unless the caller expects to free disk space by calling this method. Such
// freeing is not guaranteed.
//
// MapDeleteKey is specific to the Map Strategy. For Replace, you can use
// [Bucket.Delete] to delete the entire row, for Sets use [Bucket.SetDeleteSingle] to delete a single set element.
func (b *Bucket) MapDeleteKey(rowKey, mapKey []byte) error {
	active, release := b.getActiveMemtableForWrite()
	defer release()

	pair := MapPair{
		Key:       mapKey,
		Tombstone: true,
	}

	if active.getStrategy() == StrategyInverted {
		docID := binary.BigEndian.Uint64(mapKey)
		if err := active.SetTombstone(docID); err != nil {
			return err
		}
	}

	return active.appendMapSorted(rowKey, pair)
}

// Delete removes the given row. Note that LSM stores are append only, thus
// internally this action appends a tombstone.  The entry will not be removed
// until a compaction has run, and even then a compaction does not guarantee
// the removal of the data right away. This is because an entry could have been
// created in an older segment than those present in the compaction. This can
// be seen as an implementation detail, unless the caller expects to free disk
// space by calling this method. Such freeing is not guaranteed.
//
// Delete is specific to the Replace Strategy. For Maps, you can use
// [Bucket.MapDeleteKey] to delete a single key-value pair, for Sets use
// [Bucket.SetDeleteSingle] to delete a single set element.
func (b *Bucket) Delete(key []byte, opts ...SecondaryKeyOption) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("delete")
	b.metrics.IncBucketWriteOpOngoing("delete")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("delete")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("delete")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("delete", time.Since(start))
	}()

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.setTombstone(key, opts...)
}

func (b *Bucket) DeleteWith(key []byte, deletionTime time.Time, opts ...SecondaryKeyOption) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("delete")
	b.metrics.IncBucketWriteOpOngoing("delete")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("delete")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("delete")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("delete", time.Since(start))
	}()

	if !b.keepTombstones {
		return fmt.Errorf("bucket requires option `keepTombstones` set to delete keys at a given timestamp")
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.setTombstoneWith(key, deletionTime, opts...)
}

func (b *Bucket) createNewActiveMemtable() (memtable, error) {
	path := filepath.Join(b.dir, fmt.Sprintf("segment-%d", time.Now().UnixNano()))

	cl, err := newLazyCommitLogger(path, b.strategy)
	if err != nil {
		return nil, errors.Wrap(err, "init commit logger")
	}

	mt, err := newMemtable(path, b.strategy, b.secondaryIndices, cl,
		b.metrics, b.logger, b.enableChecksumValidation, b.bm25Config, b.writeSegmentInfoIntoFileName, b.allocChecker, b.shouldSkipKey)
	if err != nil {
		return nil, err
	}

	return mt, nil
}

func (b *Bucket) Count(ctx context.Context) (int, error) {
	view := b.getConsistentView()
	defer view.Release()

	return b.countFromCV(ctx, view)
}

func (b *Bucket) countFromCV(ctx context.Context, view BucketConsistentView) (int, error) {
	if err := CheckExpectedStrategy(b.strategy, StrategyReplace); err != nil {
		return 0, fmt.Errorf("Bucket::Count(): %w", err)
	}

	memtableCount := 0
	activeCountStats := view.Active.countStats()
	if view.Flushing == nil {
		count, err := b.memtableNetCount(ctx, activeCountStats, nil, view.Disk)
		if err != nil {
			return 0, err
		}
		memtableCount += count
	} else {
		flushingCountStats := view.Flushing.countStats()
		deltaActive, err := b.memtableNetCount(ctx, activeCountStats, flushingCountStats, view.Disk)
		if err != nil {
			return 0, err
		}
		deltaFlushing, err := b.memtableNetCount(ctx, flushingCountStats, nil, view.Disk)
		if err != nil {
			return 0, err
		}

		memtableCount = deltaActive + deltaFlushing
	}

	diskCount := b.disk.countWithSegmentList(view.Disk)

	if b.monitorCount {
		b.metrics.ObjectCount(memtableCount + diskCount)
	}
	return memtableCount + diskCount, nil
}

// CountAsync ignores the current memtable, that makes it async because it only
// reflects what has been already flushed. This in turn makes it very cheap to
// call, so it can be used for observability purposes where eventual
// consistency on the count is fine, but a large cost is not.
func (b *Bucket) CountAsync() int {
	return b.disk.count()
}

func (b *Bucket) memtableNetCount(ctx context.Context, stats *countStats, previousMemtable *countStats,
	segments []Segment,
) (int, error) {
	netCount := 0

	// TODO: this uses regular get, given that this may be called quite commonly,
	// we might consider building a pure Exists(), which skips reading the value
	// and only checks for tombstones, etc.
	for i, key := range stats.upsertKeys {
		if i%contextCheckInterval == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if !b.existsOnDiskAndPreviousMemtable(previousMemtable, key, segments) {
			netCount++
		}
	}

	for i, key := range stats.tombstonedKeys {
		if i%contextCheckInterval == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if b.existsOnDiskAndPreviousMemtable(previousMemtable, key, segments) {
			netCount--
		}
	}

	return netCount, nil
}

func (b *Bucket) existsOnDiskAndPreviousMemtable(previous *countStats, key []byte, segments []Segment) bool {
	// exists in the previous memtable
	if previous.hasUpsert(key) {
		return true
	}
	// deleted in the previous memtable
	if previous.hasTombstone(key) {
		return false
	}
	// check on disk
	if _, err := b.disk.getWithSegmentList(key, segments); err != nil {
		if errors.Is(err, lsmkv.Deleted) || errors.Is(err, lsmkv.NotFound) {
			return false
		}
		// assumes it does not exist in case of error
		return false
	}
	return true
}

func (b *Bucket) Shutdown(ctx context.Context) (err error) {
	defer GlobalBucketRegistry.Remove(b.GetDir())

	start := time.Now()

	b.metrics.IncBucketShutdownCountByStrategy(b.strategy)
	b.metrics.IncBucketShutdownInProgressByStrategy(b.strategy)

	defer func() {
		b.metrics.DecBucketShutdownInProgressByStrategy(b.strategy)

		if err != nil {
			b.metrics.IncBucketShutdownFailureCountByStrategy(b.strategy)
			return
		}

		b.metrics.ObserveBucketShutdownDurationByStrategy(b.strategy, time.Since(start))
	}()

	if err := b.disk.shutdown(ctx); err != nil {
		return err
	}

	if err := b.flushCallbackCtrl.Unregister(ctx); err != nil {
		return fmt.Errorf("long-running flush in progress: %w", ctx.Err())
	}

	b.flushLock.Lock()
	if b.active.getStrategy() == StrategyInverted {
		avgPropLength, propLengthCount := b.disk.GetAveragePropertyLength()
		b.active.setAveragePropertyLength(avgPropLength, propLengthCount)
	}
	if b.shouldReuseWAL() {
		if err := b.active.flushWAL(); err != nil {
			b.flushLock.Unlock()
			return err
		}
	} else {
		if _, err := b.active.flush(); err != nil {
			b.flushLock.Unlock()
			return err
		}
	}
	b.flushLock.Unlock()

	if b.flushing == nil {
		// active has flushing, no one else was currently flushing, it's safe to
		// exit
		return nil
	}

	// it seems we still need to wait for someone to finish flushing
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if b.flushing == nil {
				return nil
			}
		}
	}
}

func (b *Bucket) shouldReuseWAL() bool {
	return uint64(b.active.commitlogSize()) <= uint64(b.minWalThreshold)
}

// flushAndSwitchIfThresholdsMet is part of flush callbacks of the bucket.
func (b *Bucket) flushAndSwitchIfThresholdsMet(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	b.flushLock.RLock()
	commitLogSize := b.active.commitlogSize()
	memtableTooLarge := b.active.Size() >= b.memtableThreshold
	walTooLarge := uint64(commitLogSize) >= b.walThreshold
	dirtyTooLong := b.active.DirtyDuration() >= b.flushDirtyAfter
	shouldSwitch := memtableTooLarge || walTooLarge || dirtyTooLong

	// If true, the parent shard has indicated that it has
	// entered an immutable state. During this time, the
	// bucket should refrain from flushing until its shard
	// indicates otherwise
	if shouldSwitch && b.isReadOnly() {
		if b.haltedFlushTimer.IntervalElapsed() {
			b.logger.WithField("action", "lsm_memtable_flush").
				WithField("path", b.dir).
				Warn("flush halted due to shard READONLY status")
			b.haltedFlushTimer.IncreaseInterval()
		}

		b.flushLock.RUnlock()
		return false
	}

	if b.shouldReuseWAL() {
		defer b.flushLock.RUnlock()
		return b.getAndUpdateWritesSinceLastSync()
	}

	b.flushLock.RUnlock()
	if shouldSwitch {
		b.haltedFlushTimer.Reset()
		cycleLength := b.active.ActiveDuration()
		if err := b.FlushAndSwitch(); err != nil {
			b.logger.WithField("action", "lsm_memtable_flush").
				WithField("path", b.GetDir()).
				WithError(err).
				Errorf("flush and switch failed")
			return false
		}

		if b.memtableResizer != nil {
			next, ok := b.memtableResizer.NextTarget(int(b.memtableThreshold), cycleLength)
			if ok {
				b.memtableThreshold = uint64(next)
			}
		}
		return true
	}
	return false
}

func (b *Bucket) getAndUpdateWritesSinceLastSync() bool {
	return b.active.getAndUpdateWritesSinceLastSync(b.logger)
}

// UpdateStatus is used by the parent shard to communicate to the bucket
// when the shard has been set to readonly, or when it is ready for
// writes.
func (b *Bucket) UpdateStatus(status storagestate.Status) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	b.status = status
	b.disk.UpdateStatus(status)
}

func (b *Bucket) isReadOnly() bool {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	return b.status == storagestate.StatusReadOnly
}

// FlushAndSwitch is the main way to flush a memtable, replace it with a new
// one, and make sure that the flushed segment gets added to the segment group.
//
// Flushing and adding a segment can take considerable time, which is why the
// whole process is designed to be non-blocking.
//
// To achieve a non-blocking flush, the process is split into four parts:
//
//  1. atomicallySwitchMemtable: A new memtable is created, the previous
//     memtable is moved from "active" to "flushing". This switch is blocking
//     (holds b.flushLock.Lock()), but extremely fast, as we essentially just
//     switch a pointer.
//
//  2. flush: The previous memtable is flushed to disk. This may take
//     considerable time as we are I/O-bound. This is done "in the
//     background"meaning that it does not block any CRUD operations for the
//     user. It only blocks the flush process itself, meaning only one flush per
//     bucket can happen simultaneously. This is by design.
//
//  3. initAndPrecomputeNewSegment: (Newly added in
//     https://github.com/weaviate/weaviate/pull/5943, early October 2024). After
//     the previous flush step the segment can now be initialized. However, to
//     make it usable for real life, we still need to compute metadata, such as
//     bloom filters (all types) and net count additions (only Replace type).
//     Bloom filters can be calculated in isolation and are therefore fairly
//     trivial. Net count additions on the other hand are more complex, as they
//     depend on all previous segments. Calculating net count additions can take
//     a considerable amount of time, especially as the buckets grow larger. As a
//     result, we need to provide two guarantees: (1) the calculation is
//     non-blocking from a user's POV and (2) for the duration of the
//     calculation, the segment group is considered stable, i.e. no other
//     segments are added, removed, or merged. We can achieve this by holding a
//     `b.disk.maintenanceLock.RLock()` which prevents modification of the
//     segments array, but does not block user operation (which are themselves
//     RLock-holders on that same Lock).
//
//  4. atomicallyAddDiskSegmentAndRemoveFlushing: The previous method returned
//     a fully initialized segment that has not yet been added to the segment
//     group. This last step is the counter part to the first step and again
//     blocking, but fast. It adds the segment to the segment group  which at
//     this point is just a simple array append. At the same time it removes the
//     "flushing" memtable. It holds the `b.flushLock.Lock()` making this
//     operation atomic, but blocking.
//
// FlushAndSwitch is typically called periodically and does not require manual
// calling, but there are some situations where this might be intended, such as
// in test scenarios or when a force flush is desired.
func (b *Bucket) FlushAndSwitch() error {
	before := time.Now()
	var err error

	bucketPath := b.GetDir()

	b.logger.WithField("action", "lsm_memtable_flush_start").
		WithField("path", bucketPath).
		Trace("start flush and switch")

	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	if err != nil {
		b.logger.WithField("action", "lsm_memtable_flush_start").
			WithField("path", bucketPath).
			Error(err)
		return fmt.Errorf("flush and switch: %w", err)
	}
	if !switched {
		b.logger.WithField("action", "lsm_memtable_flush_start").
			WithField("path", bucketPath).
			Trace("flush and switch not needed")
		return nil
	}

	// Before we can start the actual flush, we need to make sure that all
	// ongoing writers have finished their write, otherwise we could lose the
	// write.
	//
	// The writer count is guaranteed to eventually reach zero, because we
	// switched the active memtable already, new writers will go into the
	// currently active memtable, and existing writers will eventually finish
	// their work
	b.waitForZeroWriters(b.flushing)

	if b.flushing.getStrategy() == StrategyInverted {
		avgPropLength, propLengthCount := b.disk.GetAveragePropertyLength()
		b.flushing.setAveragePropertyLength(avgPropLength, propLengthCount)
	}
	segmentPath, err := b.flushing.flush()
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	var tombstones *sroar.Bitmap
	if b.strategy == StrategyInverted {
		if tombstones, err = b.flushing.ReadOnlyTombstones(); err != nil {
			return fmt.Errorf("get tombstones: %w", err)
		}
	}

	segment, err := b.disk.initAndPrecomputeNewSegment(segmentPath)
	if err != nil {
		return fmt.Errorf("precompute metadata: %w", err)
	}

	if err := b.atomicallyAddDiskSegmentAndRemoveFlushing(segment); err != nil {
		return fmt.Errorf("add segment and remove flushing: %w", err)
	}

	switch b.strategy {
	case StrategyInverted:
		if !tombstones.IsEmpty() {
			if err = func() error {
				// As part of the discussions for
				// https://github.com/weaviate/weaviate/pull/9104 we disocvered that
				// there is a potential, non-critical bug in this logic. There can
				// essentially be a race:
				//
				//   1. Imagine two segments A+B.
				//   2. A compaction is started which merges A+B into AB
				//   3. A flush happens while the compaction is ongoing, we extend A+B
				//      with tombstones
				//   4. The compaction finishes, A+B are replaced with AB, which does
				//      not have the tombstones
				//
				// However, we deem the situation non-critical because any deleted
				// object would be filtered out at the end of the search, so we're
				// "only" wasting some CPU cycles on scoring objects that are already
				// deleted. In addition, on the next restart the tombstones would be
				// applied in a consistent fashion again, so this does not lead to
				// permanent data loss; only a temporary non-critical divergence of
				// in-memory vs on-disk state.
				//
				// As part of #9104, we have accepted this bug (as it is independent of
				// the work done in 9104) and may revisit this logic at a a later
				// point. #9104 simply changes from a maintenance RLock to a segment
				// view which does not alter the behavior, but does help reduce lock
				// contention.
				segments, release := b.disk.getConsistentViewOfSegments()
				defer release()

				// add flushing memtable tombstones to all segments
				for _, seg := range segments {
					if _, err := seg.MergeTombstones(tombstones); err != nil {
						return fmt.Errorf("merge tombstones: %w", err)
					}
				}
				return nil
			}(); err != nil {
				return fmt.Errorf("add tombstones: %w", err)
			}
		}
	}

	took := time.Since(before)
	b.logger.WithField("action", "lsm_memtable_flush_complete").
		WithField("path", bucketPath).
		Trace("finish flush and switch")

	b.logger.WithField("action", "lsm_memtable_flush_complete").
		WithField("path", bucketPath).
		WithField("took", took).
		Debugf("flush and switch took %s\n", took)

	return nil
}

// atomicallySwitchMemtable creates a new memtable and switches the active
// memtable to flushing while holding the flush lock. This makes the change atomic.
// This method is agnostic of how a new memtable is constructed, the caller can
// dependency-inject a function to do so.
func (b *Bucket) atomicallySwitchMemtable(createNewActiveMemtable func() (memtable, error)) (bool, error) {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if b.active.Size() == 0 {
		return false, nil
	}

	flushing := b.active

	mt, err := createNewActiveMemtable()
	if err != nil {
		return false, fmt.Errorf("switch active memtable: %w", err)
	}
	b.active = mt
	b.flushing = flushing

	return true, nil
}

func (b *Bucket) waitForZeroWriters(mt memtable) {
	const (
		tickerInterval = 100 * time.Millisecond
		warnThreshold  = 1 * time.Second
		warnInterval   = 1 * time.Second
	)

	start := time.Now()
	var lastWarn time.Time
	t := time.NewTicker(tickerInterval)
	for {
		writers := mt.getWriterCount()
		if writers == 0 {
			return
		}

		now := time.Now()
		if sinceStart := now.Sub(start); sinceStart >= warnThreshold {
			if lastWarn.IsZero() || now.Sub(lastWarn) >= warnInterval {
				b.logger.WithFields(logrus.Fields{
					"action":     "lsm_flush_wait_writers_refcount",
					"path":       b.dir,
					"refcount":   writers,
					"total_wait": sinceStart,
				}).Warnf("still delaying flush to wait for writers to finish (waited %.2fs so far)", sinceStart.Seconds())
				lastWarn = now
			}
		}

		<-t.C
	}
}

func (b *Bucket) atomicallyAddDiskSegmentAndRemoveFlushing(seg Segment) error {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if b.flushing.Size() == 0 {
		b.flushing = nil
		return nil
	}

	if err := b.disk.addInitializedSegment(seg); err != nil {
		return err
	}
	flushing := b.flushing
	b.flushing = nil

	switch b.strategy {
	case StrategyReplace:
		if b.monitorCount {
			// having just flushed the memtable we now have the most up2date count which
			// is a good place to update the metric
			b.metrics.ObjectCount(b.disk.count())
		}

	case StrategyRoaringSetRange:
		if b.keepSegmentsInMemory {
			b.disk.roaringSetRangeSegmentInMemory.MergeMemtableEventually(flushing.extractRoaringSetRange())
		}
	}

	return nil
}

func (b *Bucket) Strategy() string {
	return b.strategy
}

func (b *Bucket) DesiredStrategy() string {
	return b.desiredStrategy
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to avoid unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (b *Bucket) WriteWAL() error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.writeWAL()
}

func (b *Bucket) DocPointerWithScoreList(ctx context.Context, key []byte, propBoost float32, cfgs ...MapListOption) ([]terms.DocPointerWithScore, error) {
	view := b.getConsistentView()
	defer view.Release()

	return b.docPointerWithScoreListFromConsistentView(ctx, view, key, propBoost, cfgs...)
}

func (b *Bucket) docPointerWithScoreListFromConsistentView(ctx context.Context, view BucketConsistentView,
	key []byte, propBoost float32, cfgs ...MapListOption,
) ([]terms.DocPointerWithScore, error) {
	c := MapListOptionConfig{}
	for _, cfg := range cfgs {
		cfg(&c)
	}

	segments := [][]terms.DocPointerWithScore{}
	plists, segmentsDisk, err := b.disk.getCollectionAndSegments(ctx, key, view.Disk)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}

	allTombstones, err := b.loadAllTombstones(view)
	if err != nil {
		return nil, err
	}

	for i, plist := range plists {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("docPointerWithScoreList: %w", ctx.Err())
		}

		segmentStrategy := segmentsDisk[i].getStrategy()

		propLengths := make(map[uint64]uint32)
		if segmentStrategy == segmentindex.StrategyInverted {
			propLengths, err = segmentsDisk[i].getPropertyLengths()
			if err != nil {
				return nil, err
			}
		}

		segmentDecoded := make([]terms.DocPointerWithScore, len(plist))
		for j, v := range plist {
			if segmentStrategy == segmentindex.StrategyInverted {
				docId := binary.BigEndian.Uint64(v.value[:8])
				propLen := propLengths[docId]
				if err := segmentDecoded[j].FromBytesInverted(v.value, propBoost, float32(propLen)); err != nil {
					return nil, err
				}
				// check if there are any tombstones between the i and len(disk) segments
				for _, tombstones := range allTombstones[i+1:] {
					if tombstones != nil && tombstones.Contains(docId) {
						segmentDecoded[j].Frequency = 0
						break
					}
				}
			} else {
				if err := segmentDecoded[j].FromBytes(v.value, v.tombstone, propBoost); err != nil {
					return nil, err
				}
			}
		}
		segments = append(segments, segmentDecoded)
	}

	if view.Flushing != nil {
		mem, err := view.Flushing.getMap(key)
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
		docPointers := make([]terms.DocPointerWithScore, len(mem))
		for i, v := range mem {
			if err := docPointers[i].FromKeyVal(v.Key, v.Value, v.Tombstone, propBoost); err != nil {
				return nil, err
			}
		}
		segments = append(segments, docPointers)
	}

	mem, err := view.Active.getMap(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return nil, err
	}
	docPointers := make([]terms.DocPointerWithScore, len(mem))
	for i, v := range mem {
		if err := docPointers[i].FromKeyVal(v.Key, v.Value, v.Tombstone, propBoost); err != nil {
			return nil, err
		}
	}
	segments = append(segments, docPointers)

	if c.legacyRequireManualSorting {
		// Sort to support segments which were stored in an unsorted fashion
		for i := range segments {
			sort.Slice(segments[i], func(a, b int) bool {
				return segments[i][a].Id < segments[i][b].Id
			})
		}
	}

	return terms.NewSortedDocPointerWithScoreMerger().Do(ctx, segments)
}

func (b *Bucket) CreateDiskTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, config schema.BM25Config, ctx context.Context) ([][]*SegmentBlockMax, map[string]uint64, func(), error) {
	view := b.getConsistentView()
	return b.createDiskTermFromCV(ctx, view, N, filterDocIds, query, propName, propertyBoost, duplicateTextBoosts, config)
}

func (b *Bucket) createDiskTermFromCV(ctx context.Context, view BucketConsistentView, N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, config schema.BM25Config) ([][]*SegmentBlockMax, map[string]uint64, func(), error) {
	defer func() {
		if !entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
			if r := recover(); r != nil {
				b.logger.Errorf("Recovered from panic in CreateDiskTerm: %v", r)
				debug.PrintStack()
				view.Release()
			}
		}
	}()

	averagePropLength, err := b.GetAveragePropertyLength()
	if err != nil {
		view.Release()
		return nil, nil, func() {}, err
	}

	// Synchronization was reworked as part of
	// https://github.com/weaviate/weaviate/pull/9104.
	//
	// Originally we would hold a maintenanceLock.RLock and pass the unlock
	// function to the caller. This is because the actual BlockMax calcluation
	// happens outside of this function. The lock is no longer necessary now that
	// we support consistent views. We do still need to guarantee that the
	// memtables, and segments do not disappear until the caller has completed.
	// This can be done by passing the view.Release() method tot he caller.
	//
	// Panics at this level are caught and the view is released in the defer
	// function. The lock is released after the blockmax search is done, and
	// panics are also handled.

	output := make([][]*SegmentBlockMax, len(view.Disk)+2)
	idfs := make([]float64, len(query))
	idfCounts := make(map[string]uint64, len(query))
	// flusing memtable
	output[len(view.Disk)] = make([]*SegmentBlockMax, 0, len(query))

	// active memtable
	output[len(view.Disk)+1] = make([]*SegmentBlockMax, 0, len(query))

	memTombstones := sroar.NewBitmap()

	for i, queryTerm := range query {
		key := []byte(queryTerm)
		n := uint64(0)

		active := NewSegmentBlockMaxDecoded(key, i, propertyBoost, filterDocIds, averagePropLength, config)
		flushing := NewSegmentBlockMaxDecoded(key, i, propertyBoost, filterDocIds, averagePropLength, config)

		var activeTombstones *sroar.Bitmap
		if view.Active != nil {
			n2, _ := fillTerm(view.Active, key, active, filterDocIds)
			if active.Count() > 0 {
				output[len(view.Disk)+1] = append(output[len(view.Disk)+1], active)
			}
			n += n2

			var err error
			activeTombstones, err = view.Active.ReadOnlyTombstones()
			if err != nil {
				view.Release()
				return nil, nil, func() {}, fmt.Errorf("active tombstones: %w", err)
			}
			memTombstones.Or(activeTombstones)

			if !active.Exhausted() {
				active.advanceOnTombstoneOrFilter()
			}
		}

		if view.Flushing != nil {
			n2, _ := fillTerm(view.Flushing, key, flushing, filterDocIds)
			if flushing.Count() > 0 {
				output[len(view.Disk)] = append(output[len(view.Disk)], flushing)
			}
			n += n2

			tombstones, err := view.Flushing.ReadOnlyTombstones()
			if err != nil {
				view.Release()
				return nil, nil, func() {}, fmt.Errorf("flushing tombstones: %w", err)
			}
			memTombstones.Or(tombstones)

			if !flushing.Exhausted() {
				flushing.tombstones = activeTombstones
				flushing.advanceOnTombstoneOrFilter()
			}

		}

		for _, segment := range view.Disk {
			if segment.getStrategy() == segmentindex.StrategyInverted && segment.hasKey(key) {
				n += segment.getDocCount(key)
			}
		}

		// we can only know the full n after we have checked all segments and all memtables
		idfs[i] = math.Log(float64(1)+(N-float64(n)+0.5)/(float64(n)+0.5)) * float64(duplicateTextBoosts[i])

		active.idf = idfs[i]
		active.currentBlockImpact = float32(idfs[i])

		flushing.idf = idfs[i]
		flushing.currentBlockImpact = float32(idfs[i])

		idfCounts[queryTerm] = n
	}

	if ctx.Err() != nil {
		view.Release()
		return nil, nil, func() {}, fmt.Errorf("after memtable terms: %w", ctx.Err())
	}

	for j := len(view.Disk) - 1; j >= 0; j-- {
		segment := view.Disk[j]
		output[j] = make([]*SegmentBlockMax, 0, len(query))

		allTombstones := memTombstones.Clone()
		if j != len(view.Disk)-1 {
			segTombstones, err := view.Disk[j+1].ReadOnlyTombstones()
			if err != nil {
				view.Release()
				return nil, nil, func() {}, fmt.Errorf("read tombstones: %w", err)
			}
			allTombstones.Or(segTombstones)
		}

		for i, key := range query {
			term := segment.newSegmentBlockMax([]byte(key), i, idfs[i], propertyBoost, allTombstones, filterDocIds, averagePropLength, config)
			if term != nil {
				output[j] = append(output[j], term)
			}
		}
	}
	return output, idfCounts, view.Release, nil
}

func fillTerm(memtable memtable, key []byte, blockmax *SegmentBlockMax, filterDocIds helpers.AllowList) (uint64, error) {
	mapPairs, err := memtable.getMap(key)
	if err != nil && !errors.Is(err, lsmkv.NotFound) {
		return 0, err
	}
	if errors.Is(err, lsmkv.NotFound) {
		return 0, nil
	}
	n, err := addDataToTerm(mapPairs, filterDocIds, blockmax)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func addDataToTerm(mem []MapPair, filterDocIds helpers.AllowList, term *SegmentBlockMax) (uint64, error) {
	n := uint64(0)
	term.blockDataDecoded = &terms.BlockDataDecoded{
		DocIds: make([]uint64, 0, len(mem)),
		Tfs:    make([]uint64, 0, len(mem)),
	}
	term.propLengths = make(map[uint64]uint32)

	for _, v := range mem {
		if v.Tombstone {
			continue
		}
		n++
		if len(v.Value) < 8 {
			// b.logger.Warnf("Skipping pair in BM25: MapPair.Value should be 8 bytes long, but is %d.", len(v.Value))
			continue
		}
		d := terms.DocPointerWithScore{}
		if err := d.FromKeyVal(v.Key, v.Value, false, 1.0); err != nil {
			return 0, err
		}
		if filterDocIds != nil && !filterDocIds.Contains(d.Id) {
			continue
		}

		term.blockDataDecoded.DocIds = append(term.blockDataDecoded.DocIds, d.Id)
		term.blockDataDecoded.Tfs = append(term.blockDataDecoded.Tfs, uint64(d.Frequency))
		term.propLengths[d.Id] = uint32(d.PropLength)

	}
	if len(term.blockDataDecoded.DocIds) == 0 {
		return n, nil
	}
	term.exhausted = false
	term.blockEntries = make([]*terms.BlockEntry, 1)
	term.blockEntries[0] = &terms.BlockEntry{
		MaxId:  term.blockDataDecoded.DocIds[len(term.blockDataDecoded.DocIds)-1],
		Offset: 0,
	}

	term.currentBlockMaxId = term.blockDataDecoded.DocIds[len(term.blockDataDecoded.DocIds)-1]
	term.docCount = uint64(len(term.blockDataDecoded.DocIds))
	term.blockDataSize = len(term.blockDataDecoded.DocIds)
	term.idPointer = term.blockDataDecoded.DocIds[0]
	return n, nil
}

func (b *Bucket) GetAveragePropertyLength() (float64, error) {
	if b.strategy != StrategyInverted {
		return 0, fmt.Errorf("active memtable is not inverted")
	}

	var err error
	propLengthCount := uint64(0)
	propLengthSum := uint64(0)
	if b.flushing != nil {
		propLengthSum, propLengthCount, err = b.flushing.GetPropLengths()
		if err != nil {
			return 0, err
		}
	}
	// if the active memtable is inverted, we need to get the average property
	if b.active != nil {
		propLengthSum2, propLengthCount2, err := b.active.GetPropLengths()
		if err != nil {
			return 0, err
		}
		propLengthCount += propLengthCount2
		propLengthSum += propLengthSum2
	}

	// weighted average of m.averagePropLength and the average of the current flush
	// averaged by propLengthCount and m.propLengthCount
	segmentAveragePropLength, segmentPropCount := b.disk.GetAveragePropertyLength()

	if segmentPropCount != 0 {
		propLengthSum += uint64(segmentAveragePropLength * float64(segmentPropCount))
		propLengthCount += segmentPropCount
	}
	if propLengthCount == 0 {
		return 0, nil
	}
	return float64(propLengthSum) / float64(propLengthCount), nil
}

func DetermineUnloadedBucketStrategy(bucketPath string) (string, error) {
	return DetermineUnloadedBucketStrategyAmong(bucketPath, prioritizedAllStrategies)
}

func DetermineUnloadedBucketStrategyAmong(bucketPath string, prioritizedStrategies []string) (string, error) {
	if len(prioritizedStrategies) == 0 {
		return "", fmt.Errorf("no prioritizedStrategies given")
	}
	defaultStrategy := prioritizedStrategies[0]

	entries, err := os.ReadDir(bucketPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		return defaultStrategy, nil
	}
	if len(entries) == 0 {
		return defaultStrategy, nil
	}

	// check wals only after checking all segments
	var walEntries []os.DirEntry
	ec := errorcompounder.New()
	ecMismatchStrategy := errorcompounder.New()
	bufHeader := make([]byte, segmentindex.HeaderSize)

	for _, entry := range entries {
		switch filepath.Ext(entry.Name()) {
		case ".db":
			info, err := entry.Info()
			if err != nil {
				ec.Add(fmt.Errorf("%q:%w", entry.Name(), err))
				continue
			}
			if info.Size() <= 0 {
				continue
			}

			strategy, err := func() (string, error) {
				file, err := os.Open(filepath.Join(bucketPath, entry.Name()))
				if err != nil {
					return "", err
				}
				defer file.Close()

				_, err = io.ReadFull(file, bufHeader)
				if err != nil {
					return "", err
				}

				header, err := segmentindex.ParseHeader(bufHeader)
				if err != nil {
					return "", err
				}

				return header.Strategy.String(), nil
			}()
			if err != nil {
				ec.Add(fmt.Errorf("%q:%w", entry.Name(), err))
				continue
			}
			if !slices.Contains(prioritizedStrategies, strategy) {
				ecMismatchStrategy.Add(fmt.Errorf("%q:strategy %q does not match %v", entry.Name(), strategy, prioritizedStrategies))
				continue
			}
			return strategy, nil

		case ".wal":
			info, err := entry.Info()
			if err != nil {
				ec.Add(fmt.Errorf("%q:%w", entry.Name(), err))
				continue
			}
			if info.Size() <= 0 {
				continue
			}
			walEntries = append(walEntries, entry)

		default:
			continue
		}
	}

	if err := ecMismatchStrategy.ToError(); err != nil {
		return "", err
	}

	// strategy not yet determined. proceed with wal files
	for _, entry := range walEntries {
		strategy, err := func() (string, error) {
			file, err := os.Open(filepath.Join(bucketPath, entry.Name()))
			if err != nil {
				return "", err
			}
			defer file.Close()

			return guessCommitLogStrategyAmong(file, prioritizedStrategies)
		}()
		if err != nil {
			ec.Add(fmt.Errorf("%q:strategy %q does not match %v", entry.Name(), strategy, prioritizedStrategies))
			continue
		}
		return strategy, nil
	}

	if err := ec.ToError(); err != nil {
		return "", err
	}
	return defaultStrategy, nil
}
