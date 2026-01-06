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
func (p *PostingMetadataBucket) Get(ctx context.Context, postingID uint64) (*postingMetadata, error) {
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

// Set adds or replaces the posting metadata for the given posting ID.
func (p *PostingMetadataBucket) Set(ctx context.Context, postingID uint64, metadata *postingMetadata) error {
	key := p.key(postingID)

	data, err := msgpack.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal posting metadata for %d", postingID)
	}

	return p.bucket.Put(key[:], data)
}
