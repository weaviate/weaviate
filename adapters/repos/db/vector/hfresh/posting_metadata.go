package hfresh

import (
	"context"
	"encoding/binary"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingMetadata holds various information about a posting.
type PostingMetadata struct {
	// Vectors holds the IDs of the vectors in the posting.
	Vectors []uint64
	// Version keeps track of the version of the posting list.
	// Versions are incremented on each Put operation to the posting list,
	// and allow for simpler cleanup of stale data during LSMKV compactions.
	// It uses a combination of an LSMKV store for persistence and an in-memory
	// cache for fast access.
	Version uint32
}

// PostingMetadata manages various information about postings.
type PostingMetadataStore struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, *PostingMetadata]
	bucket  *PostingMetadataBucket
}

func NewPostingMetadataStore(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMetadataStore {
	b := NewPostingMetadataBucket(bucket, postingMetadataBucketPrefix)

	cache, _ := otter.New[uint64, *PostingMetadata](nil)

	return &PostingMetadataStore{
		cache:   cache,
		metrics: metrics,
		bucket:  b,
	}
}

// Get returns the size of the posting with the given ID.
func (v *PostingMetadataStore) Get(ctx context.Context, postingID uint64) (*PostingMetadata, error) {
	m, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, *PostingMetadata](func(ctx context.Context, key uint64) (*PostingMetadata, error) {
		m, err := v.bucket.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return nil, otter.ErrNotFound
			}

			return nil, err
		}

		return m, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return nil, ErrPostingNotFound
	}

	return m, err
}

// SetVectorIDs sets the vector IDs for the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read concurrently.
func (v *PostingMetadataStore) SetVectorIDs(ctx context.Context, postingID uint64, posting Posting) error {
	old, err := v.Get(ctx, postingID)
	if err != nil && err != ErrPostingNotFound {
		return err
	}

	metadata := PostingMetadata{
		Vectors: make([]uint64, len(posting)),
	}
	for i, vector := range posting {
		metadata.Vectors[i] = vector.ID()
	}
	if old != nil {
		metadata.Version = old.Version
	}

	err = v.bucket.Set(ctx, postingID, &metadata)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, &metadata)

	return nil
}

// PostingMetadataBucket is a persistent store for posting metadata.
type PostingMetadataBucket struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
}

func NewPostingMetadataBucket(bucket *lsmkv.Bucket, keyPrefix byte) *PostingMetadataBucket {
	return &PostingMetadataBucket{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingMetadataBucket) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

// Get retrieves the posting metadata for the given posting ID.
func (p *PostingMetadataBucket) Get(ctx context.Context, postingID uint64) (*PostingMetadata, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	var metadata PostingMetadata
	err = msgpack.Unmarshal(v, &metadata)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal posting metadata for %d", postingID)
	}

	return &metadata, nil
}

// Set adds or replaces the posting metadata for the given posting ID.
func (p *PostingMetadataBucket) Set(ctx context.Context, postingID uint64, metadata *PostingMetadata) error {
	key := p.key(postingID)

	data, err := msgpack.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal posting metadata for %d", postingID)
	}

	return p.bucket.Put(key[:], data)
}
