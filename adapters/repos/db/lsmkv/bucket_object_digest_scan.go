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
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/storobj"
)

// ObjectDigestScan is a point-in-time view of a Replace bucket for the hashtree init scan: take it with Bucket.NewObjectDigestScan, then Apply or Close it. Not safe for concurrent use.
type ObjectDigestScan struct {
	inMem  *CursorReplace // holds the flush lock and both memtable read locks until released
	onDisk *CursorReplace // pins the segment set by refcount until released

	inMemReleased  bool
	onDiskReleased bool
}

// NewObjectDigestScan snapshots memtables and segments at one consistent point: the disk view is taken under the memtable view's flush lock, so no flush can move data between them.
// The memtable view blocks every writer of this bucket until Apply ends its in-memory pass (or Close runs); taking the disk view loads lazy segments.
func (b *Bucket) NewObjectDigestScan() *ObjectDigestScan {
	inMem := b.CursorInMemWithTombstones()
	// A panic taking the disk view must not leave the flush and memtable locks held.
	defer func() {
		if r := recover(); r != nil {
			inMem.Close()
			panic(r)
		}
	}()
	// Digest mode: Apply reads only the header, so the value copy is skipped.
	onDisk := b.CursorOnDiskDigest(storobj.MarshallerV1HeaderLen)
	return &ObjectDigestScan{inMem: inMem, onDisk: onDisk}
}

// Apply applies f once per live UUID, stopping on the first error: the memtable pass first, then the disk pass skipping every UUID the memtable pass recorded (live or tombstone), so a memtable-only tombstone suppresses its stale on-disk value.
// Dedup is keyed by UUID, not docID (a vector-changed update reuses the UUID under a new docID). The snapshot is released on every return.
// afterInMemCallback fires exactly once, after the memtable locks are released, even when the memtable pass fails.
func (s *ObjectDigestScan) Apply(ctx context.Context,
	afterInMemCallback func(), f func(uuidBytes []byte, updateTime int64) error,
) error {
	defer s.Close()

	inmemProcessedUUIDs := make(map[[16]byte]struct{})

	err := func() error {
		defer afterInMemCallback()
		defer s.releaseInMem()

		for k, v := s.inMem.First(); k != nil; k, v = s.inMem.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if len(k) != 16 {
				return fmt.Errorf("invalid object uuid '%x': expected 16 bytes, got %d", k, len(k))
			}

			inmemProcessedUUIDs[[16]byte(k)] = struct{}{}

			if v == nil {
				continue // tombstone: recorded, not folded
			}

			_, updateTime, err := storobj.DocIDAndTimeFromBinary(v)
			if err != nil {
				return fmt.Errorf("cannot unmarshal object '%x': %w", k, err)
			}
			if err := f(k, updateTime); err != nil {
				return fmt.Errorf("callback on object '%x' failed: %w", k, err)
			}
		}

		return nil
	}()
	if err != nil {
		return err
	}

	for k, v := s.onDisk.First(); k != nil; k, v = s.onDisk.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(k) != 16 {
			return fmt.Errorf("invalid object uuid '%x': expected 16 bytes, got %d", k, len(k))
		}

		if _, ok := inmemProcessedUUIDs[[16]byte(k)]; ok {
			continue
		}

		_, updateTime, err := storobj.DocIDAndTimeFromBinary(v)
		if err != nil {
			return fmt.Errorf("cannot unmarshal object '%x': %w", k, err)
		}
		if err := f(k, updateTime); err != nil {
			return fmt.Errorf("callback on object '%x' failed: %w", k, err)
		}
	}

	return nil
}

// Close releases whatever the scan still holds; idempotent, so an abandoned scan never pins memtables or segments.
func (s *ObjectDigestScan) Close() {
	s.releaseInMem()
	if !s.onDiskReleased {
		s.onDiskReleased = true
		s.onDisk.Close()
	}
}

func (s *ObjectDigestScan) releaseInMem() {
	if !s.inMemReleased {
		s.inMemReleased = true
		s.inMem.Close()
	}
}
