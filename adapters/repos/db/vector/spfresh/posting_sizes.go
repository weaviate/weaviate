package spfresh

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
)

// PostingSizes keeps track of the number of vectors in each posting.
type PostingSizes struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, uint32]
	store   *PostingSizeStore
}

func NewPostingSizes(store *lsmkv.Store, id string, metrics *Metrics, cfg StoreConfig) (*PostingSizes, error) {
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

func (v *PostingSizes) Get(ctx context.Context, postingID uint64) (uint32, error) {
	return v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, uint32](func(ctx context.Context, key uint64) (uint32, error) {
		size, err := v.store.Get(ctx, postingID)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				return 0, otter.ErrNotFound
			}

			return 0, err
		}

		return size, nil
	}))
}

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
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			return 0, errors.WithStack(ErrPostingNotFound)
		}
		return 0, errors.Wrapf(err, "failed to get posting size for %d", postingID)
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
