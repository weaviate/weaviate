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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	postingStoreSchemaVersionV1 = 1
)

// bucketRef names a bucket instead of holding it, so every operation resolves
// and pins it afresh.
//
// The stores in this package are built once and live as long as the index, but
// the buckets under them do not: a shard teardown deregisters a bucket and
// frees its mmap'd segments while requests are still in flight. A bucket
// pointer captured at construction therefore outlives the bucket, and reading
// through it is a use-after-free, not merely a stale read.
//
// Resolving by name is necessary but not sufficient. [lsmkv.Bucket.Shutdown]
// waits only for pins taken through [lsmkv.Store.AcquireBucketForRead] before
// it frees segments, so an unpinned pointer — however freshly resolved — can
// still be freed mid-operation. Every access goes through [bucketRef.acquire],
// which holds that pin for the caller's whole operation, cursor iteration
// included. A bucket already gone at resolve time reports
// [lsmkv.ErrBucketNotFound].
// errUninitializedBucketRef reports a zero bucketRef — one that was never
// given a store. It is deliberately not [lsmkv.ErrBucketNotFound]: that
// sentinel means "the store no longer holds this bucket", which callers
// legitimately tolerate (the compaction callback swallows it), whereas a zero
// ref is a wiring bug that must surface.
var errUninitializedBucketRef = errors.New("bucket ref used before initialization")

type bucketRef struct {
	store *lsmkv.Store
	name  string
}

func newBucketRef(store *lsmkv.Store, name string) bucketRef {
	return bucketRef{store: store, name: name}
}

// acquire resolves the bucket and pins it against teardown, or reports that
// the store no longer holds it ([lsmkv.ErrBucketNotFound]) or that the ref was
// never initialized ([errUninitializedBucketRef]). The pin blocks a concurrent
// bucket shutdown, so callers MUST call the returned release exactly once —
// deferring it at the call site — and MUST NOT retain the bucket beyond it.
func (r bucketRef) acquire() (*lsmkv.Bucket, func(), error) {
	if r.store == nil {
		return nil, nil, errUninitializedBucketRef
	}

	bucket, release := r.store.AcquireBucketForRead(r.name)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", r.name)
	}
	return bucket, release, nil
}

type PostingStore struct {
	store    *lsmkv.Store
	bucket   bucketRef
	locks    *common.ShardedRWLocks
	metrics  *Metrics
	versions *PostingVersionsStore
}

func NewPostingStore(store *lsmkv.Store, sharedBucket bucketRef, metrics *Metrics, id string, cfg StoreConfig) (*PostingStore, error) {
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
						if errors.Is(err, lsmkv.ErrBucketNotFound) {
							// This runs during teardown too, and a teardown
							// deregisters the shared bucket the versions live in
							// before flushing this one. Keeping the key is always
							// safe — skipping is an optimization, and a later
							// compaction drops it once the version is readable.
							//
							// Deregistration happens before the drain, so the
							// pin this lookup takes cannot park behind the
							// teardown that made it fail: it reports the bucket
							// missing instead of blocking compaction.
							return false, nil
						}
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
		store:    store,
		bucket:   newBucketRef(store, bName),
		locks:    common.NewDefaultShardedRWLocks(),
		metrics:  metrics,
		versions: versions,
	}, nil
}

// schema of the key of the posting list:
// - 1 byte: schema version of the posting store
// - 8 bytes: posting ID (little endian uint64)
// - 1 byte: version of the posting list (incremented on each Put operation)
func (p *PostingStore) getKeyBytes(ctx context.Context, versionsBucket *lsmkv.Bucket, postingID uint64) ([]byte, error) {
	var buf [10]byte
	buf[0] = postingStoreSchemaVersionV1
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	version, err := p.versions.getWithBucket(ctx, versionsBucket, postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "get posting version for id %d", postingID)
	}
	buf[9] = version
	return buf[:], nil
}

// acquireBuckets pins both buckets a posting operation touches: the postings
// bucket and the shared bucket the versions live in. Pinning is against a
// store-wide lock, so operations that fan out take it once here rather than
// once per posting.
//
// PIN ORDER: postings bucket BEFORE shared bucket, and every overlapping pin
// in this package follows it — Put releases its version read before pinning
// the postings bucket and only re-pins the shared bucket underneath it; the
// shared-bucket stores never reach back into the postings bucket. A caller
// that pinned the shared bucket first and then wanted the postings bucket
// could deadlock against two concurrent bucket teardowns, each parked
// draining the pin the other holds.
func (p *PostingStore) acquireBuckets() (postings, versions *lsmkv.Bucket, release func(), err error) {
	postings, releasePostings, err := p.bucket.acquire()
	if err != nil {
		return nil, nil, nil, err
	}

	versions, releaseVersions, err := p.versions.bucket.acquire()
	if err != nil {
		releasePostings()
		return nil, nil, nil, err
	}

	return postings, versions, func() {
		releaseVersions()
		releasePostings()
	}, nil
}

func (p *PostingStore) Get(ctx context.Context, postingID uint64) (Posting, error) {
	postingsBucket, versionsBucket, release, err := p.acquireBuckets()
	if err != nil {
		return nil, err
	}
	defer release()

	return p.getWithBuckets(ctx, postingsBucket, versionsBucket, postingID)
}

// getWithBuckets reads one posting through buckets the caller already pinned.
func (p *PostingStore) getWithBuckets(ctx context.Context, postingsBucket, versionsBucket *lsmkv.Bucket, postingID uint64) (Posting, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	p.locks.RLock(postingID)
	key, err := p.getKeyBytes(ctx, versionsBucket, postingID)
	if err != nil {
		p.locks.RUnlock(postingID)
		return nil, err
	}

	list, err := postingsBucket.SetRawList(key)
	p.locks.RUnlock(postingID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}

	posting := Posting(make([]Vector, len(list)))

	for i, v := range list {
		posting[i] = Vector(v)
	}

	return posting, nil
}

func (p *PostingStore) MultiGet(ctx context.Context, postingIDs []uint64) ([]Posting, error) {
	postings := make([]Posting, 0, len(postingIDs))

	// One pin for the whole fan-out; a search reaches this with every selected
	// centroid at once.
	postingsBucket, versionsBucket, release, err := p.acquireBuckets()
	if err != nil {
		return nil, err
	}
	defer release()

	for _, id := range postingIDs {
		posting, err := p.getWithBuckets(ctx, postingsBucket, versionsBucket, id)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get posting %d", id)
		}
		postings = append(postings, posting)
	}

	return postings, nil
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
	bucket, release, err := p.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	err = bucket.SetAdd(buf[:], set)
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

	postingsBucket, versionsBucket, release, err := p.acquireBuckets()
	if err != nil {
		return err
	}
	defer release()

	key, err := p.getKeyBytes(ctx, versionsBucket, postingID)
	if err != nil {
		return err
	}

	return postingsBucket.SetAdd(key, [][]byte{vector})
}

func postingsBucketName(id string) string {
	return helpers.HFreshPostingsBucketName(id)
}

// PostingVersions keeps track of the version of the posting list.
// Versions are incremented on each Put operation to the posting list,
// and allow for simpler cleanup of stale data during LSMKV compactions.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingVersionsStore struct {
	bucket    bucketRef
	keyPrefix []byte
	cache     *otter.Cache[uint64, uint8]
}

func NewPostingVersionsStore(bucket bucketRef) *PostingVersionsStore {
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
	// Acquire before consulting the cache, not inside the loader: a cache hit
	// would otherwise skip the check entirely and report a version for a
	// bucket the store no longer holds, making the same call succeed or fail
	// on nothing but cache state. Holding the pin across cache.Get also covers
	// the loader's read.
	bucket, release, err := p.bucket.acquire()
	if err != nil {
		return 0, err
	}
	defer release()

	return p.getWithBucket(ctx, bucket, postingID)
}

// getWithBucket reads a version through a bucket the caller already pinned.
func (p *PostingVersionsStore) getWithBucket(ctx context.Context, bucket *lsmkv.Bucket, postingID uint64) (uint8, error) {
	version, err := p.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint8](func(ctx context.Context, key uint64) (uint8, error) {
		k := p.key(postingID)
		v, err := bucket.Get(k[:])
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
	bucket, release, err := p.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	err = bucket.Put(key[:], []byte{version})
	if err != nil {
		return err
	}

	p.cache.Set(postingID, version)
	return nil
}
