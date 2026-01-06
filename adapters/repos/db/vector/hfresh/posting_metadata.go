package hfresh

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// postingMetadata holds various information about a posting.
type postingMetadata struct {
	// Vectors holds the IDs of the vectors in the posting.
	Vectors []uint64
	// Version keeps track of the version of the posting list.
	// Versions are incremented on each Put operation to the posting list,
	// and allow for simpler cleanup of stale data during LSMKV compactions.
	// It uses a combination of an LSMKV store for persistence and an in-memory
	// cache for fast access.
	Version uint32
}

type PostingMetadataStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
}

func NewPostingMetadataStore(bucket *lsmkv.Bucket, keyPrefix byte) *PostingMetadataStore {
	return &PostingMetadataStore{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingMetadataStore) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

func (p *PostingMetadataStore) Get(ctx context.Context, postingID uint64) (*postingMetadata, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	var metadata postingMetadata
	err = msgpack.Unmarshal(v, &metadata)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal posting metadata for %d", postingID)
	}

	return &metadata, nil
}

func (p *PostingMetadataStore) Set(ctx context.Context, postingID uint64, metadata *postingMetadata) error {
	key := p.key(postingID)

	data, err := msgpack.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal posting metadata for %d", postingID)
	}

	return p.bucket.Put(key[:], data)
}
