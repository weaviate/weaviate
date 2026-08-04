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

// Benchmarks comparing the segmented WAL against the retired bbolt engine on
// the two operations that motivated the migration: the group-commit flush
// (append + fsync) and per-group compaction. The bbolt side replicates the
// removed engine verbatim — same bucket schema, truncate-then-append via
// collect-then-delete range deletion, one Update transaction per batch — so
// the comparison runs both engines on the same machine and workload. Both
// sides are driven below the batcher (identical for both engines).
//
// Run manually, e.g.:
//
//	go test -mod=readonly -run - -bench BenchmarkSharedLog -benchtime 100x ./cluster/shard/sharedlog/

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

type benchEngine interface {
	writeBatch(writes []*GroupWrite) error
	compactTo(groupID, upTo uint64) error
	purgeGroup(groupID uint64) error
}

// --- WAL side -------------------------------------------------------------

type benchWAL struct{ w *wal }

func newBenchWAL(b *testing.B, segMax int64) benchWAL {
	b.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	w, err := openWAL(filepath.Join(b.TempDir(), "wal"), segMax, log)
	require.NoError(b, err)
	b.Cleanup(func() { _ = w.close() })
	return benchWAL{w}
}

func (bw benchWAL) writeBatch(writes []*GroupWrite) error { return bw.w.writeBatch(writes) }

func (bw benchWAL) compactTo(groupID, upTo uint64) error {
	bw.w.compact(groupID, upTo)
	return nil
}

func (bw benchWAL) purgeGroup(groupID uint64) error { return bw.w.deleteGroup(groupID) }

// --- bbolt replica of the retired engine ----------------------------------

type benchBbolt struct{ db *bbolt.DB }

func newBenchBbolt(b *testing.B) *benchBbolt {
	b.Helper()
	db, err := bbolt.Open(filepath.Join(b.TempDir(), "shared.db"), 0o600, &bbolt.Options{
		Timeout:         time.Second,
		InitialMmapSize: 16 * 1024 * 1024,
	})
	require.NoError(b, err)
	require.NoError(b, db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{"entries", "state", "confstate", "snapmeta"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	}))
	b.Cleanup(func() { _ = db.Close() })
	return &benchBbolt{db}
}

func bboltGroupKey(groupID uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, groupID)
	return k
}

func bboltEntryKey(groupID, index uint64) []byte {
	k := make([]byte, 16)
	binary.BigEndian.PutUint64(k[:8], groupID)
	binary.BigEndian.PutUint64(k[8:], index)
	return k
}

// bboltDeleteRange mirrors the retired engine's collect-then-delete shape (a
// bbolt cursor Delete+Next over a same-tx-modified node skips entries).
func bboltDeleteRange(bk *bbolt.Bucket, start, end, prefix []byte) error {
	var keys [][]byte
	c := bk.Cursor()
	for k, _ := c.Seek(start); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		keys = append(keys, append([]byte(nil), k...))
	}
	for _, k := range keys {
		if err := bk.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func bboltApplyWrite(tx *bbolt.Tx, w *GroupWrite) error {
	key := bboltGroupKey(w.GroupID)
	if len(w.Entries) > 0 {
		eb := tx.Bucket([]byte("entries"))
		if err := bboltDeleteRange(eb, bboltEntryKey(w.GroupID, w.Entries[0].Index), nil, key); err != nil {
			return err
		}
		for i := range w.Entries {
			data, err := w.Entries[i].Marshal()
			if err != nil {
				return err
			}
			if err := eb.Put(bboltEntryKey(w.GroupID, w.Entries[i].Index), data); err != nil {
				return err
			}
		}
	}
	if w.HardState != nil {
		data, err := w.HardState.Marshal()
		if err != nil {
			return err
		}
		if err := tx.Bucket([]byte("state")).Put(key, data); err != nil {
			return err
		}
	}
	return nil
}

func (bb *benchBbolt) writeBatch(writes []*GroupWrite) error {
	return bb.db.Update(func(tx *bbolt.Tx) error {
		for _, w := range writes {
			if err := bboltApplyWrite(tx, w); err != nil {
				return err
			}
		}
		return nil
	})
}

func (bb *benchBbolt) compactTo(groupID, upTo uint64) error {
	return bb.db.Update(func(tx *bbolt.Tx) error {
		prefix := bboltGroupKey(groupID)
		return bboltDeleteRange(tx.Bucket([]byte("entries")), prefix, bboltEntryKey(groupID, upTo), prefix)
	})
}

func (bb *benchBbolt) purgeGroup(groupID uint64) error {
	return bb.db.Update(func(tx *bbolt.Tx) error {
		key := bboltGroupKey(groupID)
		if err := bboltDeleteRange(tx.Bucket([]byte("entries")), key, nil, key); err != nil {
			return err
		}
		for _, name := range []string{"state", "confstate", "snapmeta"} {
			if err := tx.Bucket([]byte(name)).Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- workloads ------------------------------------------------------------

// benchBatchMaker produces successive group-commit batches: `groups` writes
// per batch, each with `entriesPerWrite` entries of `entrySize` payload plus
// a HardState — the shape one flush persists.
type benchBatchMaker struct {
	next    []uint64
	payload []byte
	groups  int
	perWr   int
}

func newBenchBatchMaker(groups, entriesPerWrite, entrySize int) *benchBatchMaker {
	next := make([]uint64, groups+1)
	for i := range next {
		next[i] = 1
	}
	return &benchBatchMaker{
		next:    next,
		payload: bytes.Repeat([]byte{0xAB}, entrySize),
		groups:  groups,
		perWr:   entriesPerWrite,
	}
}

func (m *benchBatchMaker) batch() []*GroupWrite {
	writes := make([]*GroupWrite, 0, m.groups)
	for g := 1; g <= m.groups; g++ {
		ents := make([]raftpb.Entry, m.perWr)
		for j := range ents {
			ents[j] = raftpb.Entry{Index: m.next[g], Term: 1, Data: m.payload}
			m.next[g]++
		}
		writes = append(writes, &GroupWrite{
			GroupID:   uint64(g),
			Entries:   ents,
			HardState: &raftpb.HardState{Term: 1, Commit: m.next[g] - 1},
		})
	}
	return writes
}

func (m *benchBatchMaker) bytesPerBatch() int64 {
	return int64(m.groups * m.perWr * len(m.payload))
}

func benchFlush(b *testing.B, eng benchEngine, preloadBytes int64) {
	const groups, entriesPerWrite, entrySize = 8, 4, 4096
	m := newBenchBatchMaker(groups, entriesPerWrite, entrySize)

	// Preload with large batches (64 entries per write) so aging setup costs
	// few commits; the measured shape below is unaffected.
	pre := newBenchBatchMaker(groups, 64, entrySize)
	for written := int64(0); written < preloadBytes; written += pre.bytesPerBatch() {
		require.NoError(b, eng.writeBatch(pre.batch()))
	}
	for g := 1; g <= groups; g++ {
		m.next[g] = pre.next[g]
	}

	b.SetBytes(m.bytesPerBatch())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.writeBatch(m.batch()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSharedLogFlush(b *testing.B) {
	for _, aged := range []struct {
		name    string
		preload int64
	}{
		{"flat", 0},
		{"aged256MiB", 256 << 20},
	} {
		b.Run("wal/"+aged.name, func(b *testing.B) {
			benchFlush(b, newBenchWAL(b, defaultSegmentMaxBytes), aged.preload)
		})
		b.Run("bbolt/"+aged.name, func(b *testing.B) {
			benchFlush(b, newBenchBbolt(b), aged.preload)
		})
	}
}

func benchCompact(b *testing.B, eng benchEngine) {
	const gid = 1
	const total = 20000
	const window = 500
	const entrySize = 1024

	preload := func() {
		m := newBenchBatchMaker(1, 250, entrySize)
		for m.next[gid] <= total {
			require.NoError(b, eng.writeBatch(m.batch()))
		}
	}
	preload()

	b.ResetTimer()
	next := uint64(0)
	for i := 0; i < b.N; i++ {
		next += window
		if next > total {
			b.StopTimer()
			require.NoError(b, eng.purgeGroup(gid))
			preload()
			next = window
			b.StartTimer()
		}
		if err := eng.compactTo(gid, next); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSharedLogCompact(b *testing.B) {
	b.Run("wal", func(b *testing.B) {
		// A small segment cap so compaction includes its real I/O cost:
		// segment reclamation.
		benchCompact(b, newBenchWAL(b, 4<<20))
	})
	b.Run("bbolt", func(b *testing.B) {
		benchCompact(b, newBenchBbolt(b))
	})
}
