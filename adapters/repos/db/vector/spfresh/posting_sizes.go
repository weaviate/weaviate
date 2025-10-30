package spfresh

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingSizes keeps track of the number of vectors in each posting.
// It uses a combination of an LSMKV store for persistence and an in-memory
// cache for fast access.
type PostingSizes struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, uint32]
	store   *PostingSizeStore
}

func NewPostingSizes(store *lsmkv.Store, metrics *Metrics, id string, cfg StoreConfig) (*PostingSizes, error) {
	pStore, err := NewPostingSizeStore(store, metrics, postingSizeBucketName(id), cfg)
	if err != nil {
		return nil, err
	}

	cache, err := otter.New(&otter.Options[uint64, uint32]{
		MaximumSize: 10_000,
	})
	if err != nil {
		return nil, err
	}

	return &PostingSizes{
		cache:   cache,
		metrics: metrics,
		store:   pStore,
	}, nil
}

// Get returns the size of the posting with the given ID.
func (v *PostingSizes) Get(ctx context.Context, postingID uint64) (uint32, error) {
	size, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint32](func(ctx context.Context, key uint64) (uint32, error) {
		size, err := v.store.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return 0, otter.ErrNotFound
			}

			return 0, err
		}

		return size, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return 0, ErrPostingNotFound
	}

	return size, err
}

// Sets the size of the posting to newSize.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingSizes) Set(ctx context.Context, postingID uint64, newSize uint32) error {
	err := v.store.Set(ctx, postingID, newSize)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, newSize)
	v.metrics.ObservePostingSize(float64(newSize))
	return nil
}

// Incr increments the size of the posting by delta and returns the new size.
// This method assumes the posting has been locked for writing by the caller.
func (v *PostingSizes) Inc(ctx context.Context, postingID uint64, delta uint32) (uint32, error) {
	old, err := v.Get(ctx, postingID)
	if err != nil {
		if !errors.Is(err, ErrPostingNotFound) {
			return 0, err
		}
	}

	newSize := old + delta
	err = v.store.Set(ctx, postingID, newSize)
	if err != nil {
		return 0, err
	}

	v.cache.Set(postingID, newSize)
	v.metrics.ObservePostingSize(float64(newSize))
	return newSize, nil
}

// PostingSizeStore is a persistent store for posting sizes.
// It stores the sizes in an LSMKV bucket.
type PostingSizeStore struct {
	store  *lsmkv.Store
	bucket *lsmkv.Bucket
}

func NewPostingSizeStore(store *lsmkv.Store, metrics *Metrics, bucketName string, cfg StoreConfig) (*PostingSizeStore, error) {
	err := store.CreateOrLoadBucket(context.Background(),
		bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithAllocChecker(cfg.AllocChecker),
		lsmkv.WithMinMMapSize(cfg.MinMMapSize),
		lsmkv.WithMinWalThreshold(cfg.MaxReuseWalSize),
		lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments),
		lsmkv.WithWriteSegmentInfoIntoFileName(cfg.WriteSegmentInfoIntoFileName),
		lsmkv.WithWriteMetadata(cfg.WriteMetadataFilesEnabled),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bucketName)
	}

	return &PostingSizeStore{
		store:  store,
		bucket: store.Bucket(bucketName),
	}, nil
}

func (p *PostingSizeStore) Get(ctx context.Context, postingID uint64) (uint32, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)
	v, err := p.bucket.Get(buf[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return 0, ErrPostingNotFound
	}

	return binary.LittleEndian.Uint32(v), nil
}

func (p *PostingSizeStore) Set(ctx context.Context, postingID uint64, size uint32) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], postingID)

	return p.bucket.Put(buf[:], binary.LittleEndian.AppendUint32(nil, size))
}

func postingSizeBucketName(id string) string {
	return fmt.Sprintf("spfresh_posting_sizes_%s", id)
}
