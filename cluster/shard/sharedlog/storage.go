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

package sharedlog

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// groupStorage mirrors etcd's MemoryStorage semantics over bbolt:
//
//   - Empty group: FirstIndex=1, LastIndex=0, Term(0)=0.
//   - Term(snapshot.Index) returns snapshot.Term even though the entry
//     itself has been compacted out.
//   - Out of range: ErrCompacted below FirstIndex, ErrUnavailable above
//     LastIndex.
type groupStorage struct {
	store   *Store
	groupID uint64
}

func (g *groupStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	var hs raftpb.HardState
	var cs raftpb.ConfState
	err := g.store.db.View(func(tx *bbolt.Tx) error {
		key := encodeGroupKey(g.groupID)
		if v := tx.Bucket([]byte(bucketState)).Get(key); v != nil {
			if e := hs.Unmarshal(v); e != nil {
				return fmt.Errorf("unmarshal hardstate: %w", e)
			}
		}
		if v := tx.Bucket([]byte(bucketConfState)).Get(key); v != nil {
			if e := cs.Unmarshal(v); e != nil {
				return fmt.Errorf("unmarshal confstate: %w", e)
			}
		}
		return nil
	})
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, fmt.Errorf("sharedlog: InitialState: %w", err)
	}
	return hs, cs, nil
}

func (g *groupStorage) FirstIndex() (uint64, error) {
	snap, err := g.Snapshot()
	if err != nil {
		return 0, err
	}
	return snap.Metadata.Index + 1, nil
}

func (g *groupStorage) LastIndex() (uint64, error) {
	var last uint64
	err := g.store.db.View(func(tx *bbolt.Tx) error {
		if v := tx.Bucket([]byte(bucketSnapMeta)).Get(encodeGroupKey(g.groupID)); v != nil {
			var snap raftpb.Snapshot
			if e := snap.Unmarshal(v); e != nil {
				return fmt.Errorf("unmarshal snapshot: %w", e)
			}
			last = snap.Metadata.Index
		}
		// Seek past the group's prefix and step back, giving an
		// O(log N) max-index lookup rather than a full scan.
		prefix := encodeGroupKey(g.groupID)
		c := tx.Bucket([]byte(bucketEntries)).Cursor()
		var ourMax []byte
		if g.groupID == ^uint64(0) {
			k, _ := c.Last()
			if k != nil && bytes.HasPrefix(k, prefix) {
				ourMax = k
			}
		} else {
			nextPrefix := encodeGroupKey(g.groupID + 1)
			k, _ := c.Seek(nextPrefix)
			if k == nil {
				k, _ = c.Last()
				if k != nil && bytes.HasPrefix(k, prefix) {
					ourMax = k
				}
			} else {
				k, _ = c.Prev()
				if k != nil && bytes.HasPrefix(k, prefix) {
					ourMax = k
				}
			}
		}
		if ourMax != nil {
			if idx := decodeEntryIndex(ourMax); idx > last {
				last = idx
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("sharedlog: LastIndex: %w", err)
	}
	return last, nil
}

func (g *groupStorage) Term(i uint64) (uint64, error) {
	snap, err := g.Snapshot()
	if err != nil {
		return 0, err
	}
	if i == snap.Metadata.Index {
		return snap.Metadata.Term, nil
	}
	first := snap.Metadata.Index + 1
	if i < first {
		return 0, raft.ErrCompacted
	}
	last, err := g.LastIndex()
	if err != nil {
		return 0, err
	}
	if i > last {
		return 0, raft.ErrUnavailable
	}

	var term uint64
	err = g.store.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket([]byte(bucketEntries)).Get(encodeEntryKey(g.groupID, i))
		if v == nil {
			return raft.ErrUnavailable
		}
		var ent raftpb.Entry
		if e := ent.Unmarshal(v); e != nil {
			return fmt.Errorf("unmarshal entry at index %d: %w", i, e)
		}
		term = ent.Term
		return nil
	})
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (g *groupStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	snap, err := g.Snapshot()
	if err != nil {
		return nil, err
	}
	first := snap.Metadata.Index + 1
	if lo < first {
		return nil, raft.ErrCompacted
	}
	last, err := g.LastIndex()
	if err != nil {
		return nil, err
	}
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}

	var ents []raftpb.Entry
	var size uint64
	err = g.store.db.View(func(tx *bbolt.Tx) error {
		startKey := encodeEntryKey(g.groupID, lo)
		endKey := encodeEntryKey(g.groupID, hi)
		c := tx.Bucket([]byte(bucketEntries)).Cursor()
		for k, v := c.Seek(startKey); k != nil && bytes.Compare(k, endKey) < 0; k, v = c.Next() {
			var ent raftpb.Entry
			if e := ent.Unmarshal(v); e != nil {
				return fmt.Errorf("unmarshal entry at index %d: %w", decodeEntryIndex(k), e)
			}
			entSize := uint64(ent.Size())
			// etcd's contract: always return at least one entry even
			// if it exceeds maxSize.
			if len(ents) > 0 && size+entSize > maxSize {
				break
			}
			ents = append(ents, ent)
			size += entSize
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sharedlog: Entries: %w", err)
	}
	return ents, nil
}

func (g *groupStorage) Snapshot() (raftpb.Snapshot, error) {
	var snap raftpb.Snapshot
	err := g.store.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket([]byte(bucketSnapMeta)).Get(encodeGroupKey(g.groupID))
		if v == nil {
			return nil
		}
		return snap.Unmarshal(v)
	})
	if err != nil {
		return raftpb.Snapshot{}, fmt.Errorf("sharedlog: Snapshot: %w", err)
	}
	return snap, nil
}
