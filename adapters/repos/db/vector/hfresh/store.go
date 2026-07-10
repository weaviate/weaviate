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

package hfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	postingStoreSchemaVersionV1 = 1
)

// defaultReadConcurrency bounds how many posting reads a single MultiGet
// issues in parallel. Overridable via HFRESH_READ_CONCURRENCY (1 = sequential).
const defaultReadConcurrency = 16

type PostingStore struct {
	store    *lsmkv.Store
	bucket   *lsmkv.Bucket
	locks    *common.ShardedRWLocks
	metrics  *Metrics
	versions *PostingVersionsStore
	logger   logrus.FieldLogger

	// readConcurrency is the max number of parallel posting reads per
	// MultiGet call.
	readConcurrency int
}

func NewPostingStore(store *lsmkv.Store, sharedBucket *lsmkv.Bucket, metrics *Metrics, logger logrus.FieldLogger, id string, cfg StoreConfig) (*PostingStore, error) {
	bName := postingsBucketName(id)

	versions := NewPostingVersionsStore(sharedBucket)

	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(
			lsmkv.StrategySetCollection,
			lsmkv.WithForceCompaction(true),
			lsmkv.WithShouldSkipKeyFunction(
				func(key []byte, ctx context.Context) (bool, error) {
					if len(key) != 10 {
						// don't skip on error
						return false, fmt.Errorf("invalid key length: %d", len(key))
					}
					postingID := binary.LittleEndian.Uint64(key[1:9])
					segmentPostingVersion := key[9]
					currentPostingVersion, err := versions.Get(ctx, postingID)
					if err != nil {
						return false, errors.Wrap(err, "get posting version during compaction")
					}
					skip := segmentPostingVersion != currentPostingVersion
					return skip, nil
				},
			),
		)...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	return &PostingStore{
		store:           store,
		bucket:          store.Bucket(bName),
		locks:           common.NewDefaultShardedRWLocks(),
		metrics:         metrics,
		versions:        versions,
		logger:          logger,
		readConcurrency: envIntOrDefault("HFRESH_READ_CONCURRENCY", defaultReadConcurrency),
	}, nil
}

// envIntOrDefault reads a positive integer from the environment, falling back
// to def when unset or invalid.
func envIntOrDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	return n
}

// envFloatOrDefault reads a non-negative float from the environment, falling
// back to def when unset or invalid.
func envFloatOrDefault(key string, def float32) float32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 32)
	if err != nil || f < 0 {
		return def
	}
	return float32(f)
}

// envBoolOrDefault reads a boolean from the environment, falling back to def
// when unset. "0", "false", "off" and "no" disable; everything else enables.
func envBoolOrDefault(key string, def bool) bool {
	switch os.Getenv(key) {
	case "":
		return def
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// schema of the key of the posting list:
// - 1 byte: schema version of the posting store
// - 8 bytes: posting ID (little endian uint64)
// - 1 byte: version of the posting list (incremented on each Put operation)
func (p *PostingStore) getKeyBytes(ctx context.Context, postingID uint64) ([]byte, error) {
	var buf [10]byte
	buf[0] = postingStoreSchemaVersionV1
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	version, err := p.versions.Get(ctx, postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "get posting version for id %d", postingID)
	}
	buf[9] = version
	return buf[:], nil
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	return p.getWithStats(ctx, postingID, nil)
}

// getWithStats reads one posting and, when st is non-nil, accounts the read in
// the per-query search stats (postings found/empty, disk segments touched,
// memtable hits, payload bytes).
func (p *PostingStore) getWithStats(ctx context.Context, postingID uint64, st *searchStats) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	p.locks.RLock(postingID)
	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		p.locks.RUnlock(postingID)
		return nil, err
	}
	list, stats, err := p.bucket.SetRawListWithStats(key)
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	return buildPosting(list, stats, st), nil
}

// getWithStatsFromView is like getWithStats but reads the posting bytes WITHOUT
// copying them out of the segment memory, under a caller-held consistent view.
//
// WARNING: the returned Posting aliases the view's segment/memtable memory (see
// (*Bucket).SetRawListWithStatsFromView). It is valid ONLY until the view is
// released; callers must extract everything they need (IDs, distances) during
// the scan and must not retain the Posting — or any Vector in it — past
// release.
func (p *PostingStore) getWithStatsFromView(ctx context.Context, view lsmkv.BucketConsistentView, postingID uint64, st *searchStats) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	p.locks.RLock(postingID)
	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		p.locks.RUnlock(postingID)
		return nil, err
	}
	list, stats, err := p.bucket.SetRawListWithStatsFromView(view, key)
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	return buildPosting(list, stats, st), nil
}

// buildPosting accounts the read in st (when non-nil) and wraps the raw values
// as a Posting. The Vectors alias the input list's backing memory.
func buildPosting(list [][]byte, stats lsmkv.SetRawListStats, st *searchStats) Posting {
	if st != nil {
		if len(list) > 0 {
			st.PostingsRead++
		} else {
			st.PostingsEmpty++
		}
		st.SegmentReads += uint32(stats.SegmentsHit)
		if stats.FlushingHit {
			st.MemtableReads++
		}
		if stats.MemtableHit {
			st.MemtableReads++
		}
		st.PostingBytes += uint64(stats.Bytes)
	}

	posting := Posting(make([]Vector, len(list)))
	for i, v := range list {
		posting[i] = Vector(v)
	}

	return posting
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	return p.MultiGetWithStats(ctx, postingIDs, nil)
}

// MultiGetWithStats behaves like MultiGet and, when st is non-nil, accounts
// every read in the per-query search stats. Reads are issued in parallel,
// bounded by the store's readConcurrency; results keep the order of
// postingIDs.
func (p *PostingStore) MultiGetWithStats(ctx context.Context, postingIDs []uint64, st *searchStats) ([]Posting, error) {
	concurrency := min(p.readConcurrency, len(postingIDs))

	if concurrency <= 1 {
		postings := make([]Posting, 0, len(postingIDs))
		for _, id := range postingIDs {
			posting, err := p.getWithStats(ctx, id, st)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get posting %d", id)
			}
			postings = append(postings, posting)
		}
		return postings, nil
	}

	postings := make([]Posting, len(postingIDs))
	// each worker accumulates into its own stats to avoid synchronizing the
	// hot counters; merged once below
	workerStats := make([]searchStats, concurrency)

	eg := enterrors.NewErrorGroupWrapper(p.logger)
	for w := range concurrency {
		eg.Go(func() error {
			var ws *searchStats
			if st != nil {
				ws = &workerStats[w]
			}
			for i := w; i < len(postingIDs); i += concurrency {
				posting, err := p.getWithStats(ctx, postingIDs[i], ws)
				if err != nil {
					return errors.Wrapf(err, "failed to get posting %d", postingIDs[i])
				}
				postings[i] = posting
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if st != nil {
		for i := range workerStats {
			st.add(&workerStats[i])
		}
	}

	return postings, nil
}

// PostingResult is one posting delivered by MultiGetStreamWithStats, tagged
// with its index in the requested ID slice.
type PostingResult struct {
	Index   int
	Posting Posting
}

// MultiGetStreamWithStats reads the postings with bounded parallelism and
// delivers each one on the returned channel as soon as it is available
// (arbitrary order), so the caller can overlap processing with the remaining
// reads. The channel is buffered for all results: readers never block on a
// slow consumer, and abandoning the channel early stalls nothing.
//
// Reads are ZERO-COPY: a single consistent bucket view is acquired up front and
// held for the whole stream; posting bytes are returned as sub-slices of the
// view's segment/memtable memory instead of being copied out. The view
// refcounts the disk segments, so their mmaps cannot be unmapped while it is
// held.
//
// WARNING: every delivered Posting aliases the view's memory and is valid ONLY
// until the returned done function releases the view. The consumer must extract
// what it needs (IDs, distances) during the scan and MUST NOT retain any
// Posting — or Vector bytes — past done(). Callers that need results to outlive
// the read must use MultiGet/MultiGetWithStats (which copy).
//
// The channel is closed once all reads finished. The returned done function is
// idempotent (safe to call multiple times): it waits for the workers, reports
// the first read error, merges the per-worker stats into st, and RELEASES the
// view. It MUST be called (e.g. via defer) — unlike the old wait function,
// abandoning the stream without calling done leaks the consistent view and
// blocks compactions.
//
// concurrency caps the number of parallel reads; the search path passes a
// load-adaptive value derived from the CPU budget and in-flight query count,
// rather than the fixed p.readConcurrency used by the non-search MultiGet
// paths.
func (p *PostingStore) MultiGetStreamWithStats(ctx context.Context, postingIDs []uint64, st *searchStats, concurrency int) (<-chan PostingResult, func() error) {
	concurrency = max(min(concurrency, len(postingIDs)), 1)

	ch := make(chan PostingResult, len(postingIDs))
	workerStats := make([]searchStats, concurrency)
	errCh := make(chan error, 1)

	// One consistent view for the whole stream. All workers read posting bytes
	// without copying, aliasing this view's segment memory; done() releases it.
	view := p.bucket.GetConsistentView()

	eg := enterrors.NewErrorGroupWrapper(p.logger)
	for w := range concurrency {
		eg.Go(func() error {
			var ws *searchStats
			if st != nil {
				ws = &workerStats[w]
			}
			for i := w; i < len(postingIDs); i += concurrency {
				posting, err := p.getWithStatsFromView(ctx, view, postingIDs[i], ws)
				if err != nil {
					return errors.Wrapf(err, "failed to get posting %d", postingIDs[i])
				}
				ch <- PostingResult{Index: i, Posting: posting}
			}
			return nil
		})
	}

	enterrors.GoWrapper(func() {
		errCh <- eg.Wait()
		close(ch)
	}, p.logger)

	// done is idempotent (sync.Once). The stats merge must happen in the
	// caller's goroutine: the caller mutates st while consuming the channel
	// (that's the point of the pipeline), so the closer goroutine must not
	// touch it. The receive on errCh orders the merge after every worker's last
	// write. The view is released only after all workers have finished, so no
	// worker can dereference unmapped memory.
	var (
		once    sync.Once
		doneErr error
	)
	done := func() error {
		once.Do(func() {
			doneErr = <-errCh
			if st != nil {
				for i := range workerStats {
					st.add(&workerStats[i])
				}
			}
			view.ReleaseView()
		})
		return doneErr
	}
	return ch, done
}

func (p *PostingStore) Put(ctx context.Context, postingID uint64, posting Posting) error {
	start := time.Now()
	defer p.metrics.StorePutDuration(start)

	if posting == nil {
		return errors.New("posting cannot be nil")
	}

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	set := make([][]byte, len(posting))
	for i, v := range posting {
		set[i] = v
	}

	currentVersion, err := p.versions.Get(ctx, postingID)
	if err != nil {
		return err
	}
	newVersion := currentVersion + 1

	var buf [10]byte
	buf[0] = postingStoreSchemaVersionV1
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	buf[9] = newVersion
	err = p.bucket.SetAdd(buf[:], set)
	if err != nil {
		return errors.Wrapf(err, "failed to put posting %d", postingID)
	}

	err = p.versions.Set(ctx, postingID, newVersion)
	if err != nil {
		return errors.Wrapf(err, "set new posting version for id %d", postingID)
	}

	return nil
}

func (p *PostingStore) Append(ctx context.Context, postingID uint64, vector Vector) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	p.locks.Lock(postingID)
	defer p.locks.Unlock(postingID)

	key, err := p.getKeyBytes(ctx, postingID)
	if err != nil {
		return err
	}

	return p.bucket.SetAdd(key, [][]byte{vector})
}

func postingsBucketName(id string) string {
	return fmt.Sprintf("hfresh_postings_%s", id)
}

// PostingVersions keeps track of the version of the posting list.
// Versions are incremented on each Put operation to the posting list,
// and allow for simpler cleanup of stale data during LSMKV compactions.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingVersionsStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix []byte
	cache     *otter.Cache[uint64, uint8]
}

func NewPostingVersionsStore(bucket *lsmkv.Bucket) *PostingVersionsStore {
	cache, _ := otter.New[uint64, uint8](nil)
	return &PostingVersionsStore{
		bucket:    bucket,
		keyPrefix: postingVersionBucketPrefix,
		cache:     cache,
	}
}

func (p *PostingVersionsStore) key(postingID uint64) []byte {
	buf := make([]byte, len(p.keyPrefix)+8)
	copy(buf, p.keyPrefix)
	binary.LittleEndian.PutUint64(buf[len(p.keyPrefix):], postingID)
	return buf
}

func (p *PostingVersionsStore) Get(ctx context.Context, postingID uint64) (uint8, error) {
	version, err := p.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint8](func(ctx context.Context, key uint64) (uint8, error) {
		k := p.key(postingID)
		v, err := p.bucket.Get(k[:])
		if err != nil {
			return 0, errors.Wrapf(err, "failed to get posting size for %d", postingID)
		}
		if len(v) == 0 {
			return 0, otter.ErrNotFound
		}

		return v[0], nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return 0, nil
	}

	return version, err
}

func (p *PostingVersionsStore) Set(ctx context.Context, postingID uint64, version uint8) error {
	key := p.key(postingID)
	err := p.bucket.Put(key[:], []byte{version})
	if err != nil {
		return err
	}

	p.cache.Set(postingID, version)
	return nil
}
