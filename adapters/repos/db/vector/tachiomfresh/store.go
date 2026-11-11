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

package tachiomfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
)

// postingStore persists the postings: one Set key per posting, one
// 6-byte element per entry, 7 for an entry above version 1. It is the
// durable copy only; the read path is postingTable (posting.go).
//
// The index does not use hfresh's PostingStore, for two measured reasons:
//
//   - hfresh.PostingStore can only replace a whole posting (Put), so
//     removing one token after a reassign rewrote its posting. hfresh
//     removes no entry (its reassign bumps the vector's version); this
//     index does remove entries. The rewrites held a large memtable
//     after one search sweep and made the saved index several times
//     larger.
//   - Put writes under a new key whose last byte is a version that wraps
//     after 256 puts. The Set under the reused key still holds what was
//     put there 256 puts earlier, so a reopened index read back tokens
//     that had long moved to another posting.
//
// Here a key is the posting id alone, and a change writes only the
// elements that change: SetAdd for a new entry, a Set tombstone
// (SetDeleteSingle) for a removed one. There are no versions, so nothing
// wraps.
//
// A posting whose centroid a split or a merge deleted is dropped, and not
// emptied. Nothing is written, the id joins dead, and the bucket leaves a
// dead key out at its next flush or compaction. hfresh's version filter
// does the same for the keys of old versions. Measured, one tombstone
// per entry made a split write much more to disk. Posting ids are never
// reused, so a dead key is never read again, unless a crash loses the
// centroid delete that made it dead (see writeWALs in tachiomfresh.go).
//
// Concurrency is the caller's: every method is called with the posting's
// lock held (or on a posting no other caller can reach yet), as the
// methods of postingTable are.
type postingStore struct {
	store   *lsmkv.Store
	bucket  string
	metrics *hfresh.Metrics

	// fault, set by tests only, makes Append, Remove or Put fail when it
	// returns an error for the operation's name.
	fault func(op string) error

	// deadMu guards dead, one bit per posting id. The bucket's flush and
	// compaction read it; Drop writes it.
	deadMu sync.RWMutex
	dead   []uint64
}

// newPostingStore opens the store's own bucket, as the type table has its
// own: hfresh's bucket accessors are unexported, and nothing under hfresh
// changes for this index. The options are those of
// hfresh.NewPostingStore. The key filter drops dead postings, where
// hfresh's drops old versions.
func newPostingStore(store *lsmkv.Store, metrics *hfresh.Metrics, id string, cfg hfresh.StoreConfig) (*postingStore, error) {
	p := &postingStore{
		store:   store,
		bucket:  helpers.TachiomFreshPostingsBucketName(id),
		metrics: metrics,
	}
	err := store.CreateOrLoadBucket(context.Background(), p.bucket,
		cfg.MakeBucketOptions(
			lsmkv.StrategySetCollection,
			lsmkv.WithForceCompaction(true),
			lsmkv.WithShouldSkipKeyFunction(
				func(key []byte, ctx context.Context) (bool, error) {
					if len(key) != 8 {
						// don't skip on error
						return false, fmt.Errorf("invalid key length: %d", len(key))
					}
					return p.isDead(binary.LittleEndian.Uint64(key)), nil
				},
			),
		)...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", p.bucket)
	}
	return p, nil
}

// Drop gives up posting postingID, whose centroid is deleted. Its key is
// left out of the disk at the next flush or compaction. Nothing may be
// written to the posting afterwards.
func (p *postingStore) Drop(postingID uint64) {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	word := postingID / 64
	if uint64(len(p.dead)) <= word {
		p.dead = append(p.dead, make([]uint64, word+1-uint64(len(p.dead)))...)
	}
	p.dead[word] |= 1 << (postingID % 64)
}

func (p *postingStore) isDead(postingID uint64) bool {
	p.deadMu.RLock()
	defer p.deadMu.RUnlock()
	word := postingID / 64
	return word < uint64(len(p.dead)) && p.dead[word]&(1<<(postingID%64)) != 0
}

// acquire pins the bucket for one operation, as hfresh's bucketRef.acquire.
func (p *postingStore) acquire() (*lsmkv.Bucket, func(), error) {
	bucket, release := p.store.AcquireBucketForRead(p.bucket)
	if bucket == nil {
		release()
		return nil, nil, errors.Wrapf(lsmkv.ErrBucketNotFound, "bucket %s", p.bucket)
	}
	return bucket, release, nil
}

func postingKey(postingID uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, postingID)
	return key
}

// storedRefSize is the size of an entry on disk: (docID u32, tokenPos u16).
// An entry above version 1 adds its version byte (versionedRefSize), so a
// document never re-inserted costs no more than before (see packedPosting).
const (
	storedRefSize    = 6
	versionedRefSize = 7
)

func encodeRef(ref tokenRef) []byte {
	size := storedRefSize
	if ref.Version > 1 {
		size = versionedRefSize
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf, ref.DocID)
	binary.LittleEndian.PutUint16(buf[4:], ref.Pos)
	if ref.Version > 1 {
		buf[6] = ref.Version
	}
	return buf
}

// Get returns the entries of posting postingID, in no particular order.
func (p *postingStore) Get(postingID uint64) ([]tokenRef, error) {
	start := time.Now()
	defer p.metrics.StoreGetDuration(start)

	bucket, release, err := p.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	list, err := bucket.SetList(postingKey(postingID))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting %d", postingID)
	}
	refs := make([]tokenRef, len(list))
	for i, v := range list {
		var version uint8
		switch len(v) {
		case storedRefSize:
		case versionedRefSize:
			version = v[6]
		default:
			return nil, errors.Errorf("posting %d: entry of %d bytes, expected %d or %d", postingID, len(v), storedRefSize, versionedRefSize)
		}
		refs[i] = newTokenRef(binary.LittleEndian.Uint32(v), binary.LittleEndian.Uint16(v[4:]), version)
	}
	return refs, nil
}

// Append adds ref to posting postingID.
func (p *postingStore) Append(postingID uint64, ref tokenRef) error {
	start := time.Now()
	defer p.metrics.StoreAppendDuration(start)

	if p.fault != nil {
		if err := p.fault("append"); err != nil {
			return err
		}
	}
	bucket, release, err := p.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.SetAdd(postingKey(postingID), [][]byte{encodeRef(ref)})
}

// Remove drops ref from posting postingID.
func (p *postingStore) Remove(postingID uint64, ref tokenRef) error {
	start := time.Now()
	defer p.metrics.StoreRemoveDuration(start)

	if p.fault != nil {
		if err := p.fault("remove"); err != nil {
			return err
		}
	}
	bucket, release, err := p.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.SetDeleteSingle(postingKey(postingID), encodeRef(ref))
}

// Put makes posting postingID hold next, given that it holds prev. The
// entries only in prev are removed; the entries only in next are added. A
// split's new posting has no prev and is written whole. A merge adds the
// small posting's entries to the large one's and writes nothing else. A
// posting that is given up is not emptied this way: see Drop.
func (p *postingStore) Put(postingID uint64, prev, next []tokenRef) error {
	start := time.Now()
	defer p.metrics.StorePutDuration(start)

	if p.fault != nil {
		if err := p.fault("put"); err != nil {
			return err
		}
	}
	bucket, release, err := p.acquire()
	if err != nil {
		return err
	}
	defer release()

	key := postingKey(postingID)
	kept := make(map[tokenRef]struct{}, len(next))
	for _, ref := range next {
		kept[ref] = struct{}{}
	}
	had := make(map[tokenRef]struct{}, len(prev))
	for _, ref := range prev {
		had[ref] = struct{}{}
		if _, ok := kept[ref]; !ok {
			if err := bucket.SetDeleteSingle(key, encodeRef(ref)); err != nil {
				return errors.Wrapf(err, "failed to remove an entry of posting %d", postingID)
			}
		}
	}
	var added [][]byte
	for _, ref := range next {
		if _, ok := had[ref]; !ok {
			added = append(added, encodeRef(ref))
		}
	}
	if len(added) == 0 {
		return nil
	}
	if err := bucket.SetAdd(key, added); err != nil {
		return errors.Wrapf(err, "failed to put posting %d", postingID)
	}
	return nil
}
