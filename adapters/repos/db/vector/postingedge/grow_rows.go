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

package postingedge

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// The consolidated edges of v5.1 and v5.3 (bench-report.md, "v5.1: aristas
// consolidadas" and "v5.3: la fila de aristas en un valor, junto a la de
// miembros"): the edges of a posting as one row, written whole, with the
// changes since it was written waiting in memory. It is the other way of
// keeping the edges behind growEdges, next to a value per list (growLists,
// v5.0), and the one the index a shard opens is written with: the row in the
// bucket of the postings, next to the members (v5.3), which is what a search
// reads least from disk for.
//
// What waits in memory is in the index itself (growIndex.pending), which is
// where the benchmarks read it. Where the index is asked to (growOpen.
// keepPending) every change that waits is in a bucket too, from the moment it
// is made until the row that holds it is written: what a sync leaves on disk
// is then the whole index, and an index that opens finds what waited when it
// closed, or when it stopped. No search reads that bucket.

// growRows keeps the edges of every posting in one row (growEdges).
type growRows struct {
	g *growIndex
}

func (r growRows) buckets() []*lsmkv.Bucket {
	var buckets []*lsmkv.Bucket
	if r.g.edges != nil {
		buckets = append(buckets, r.g.edges)
	}
	if r.g.waiting != nil {
		buckets = append(buckets, r.g.waiting)
	}
	return buckets
}

// growPendingValue is a change that waits, as its bucket holds it: a byte
// that tells the format, the list, and the distance of every edge of it.
func growPendingValue(list []uint32, dists []float32) []byte {
	value := make([]byte, 1+4*len(list)+4*len(dists))
	value[0] = growPendingFormat
	for i, endpoint := range list {
		binary.LittleEndian.PutUint32(value[1+4*i:], endpoint)
	}
	at := 1 + 4*len(list)
	for i, dist := range dists {
		putFloat32(value[at+4*i:], dist)
	}
	return value
}

func growPendingDecode(value []byte) (list []uint32, dists []float32, err error) {
	if len(value) < 1 || value[0] != growPendingFormat || (len(value)-1)%8 != 0 {
		return nil, nil, errors.Errorf("a change to the edges that waits takes %d bytes, or is of another format", len(value))
	}
	n := (len(value) - 1) / 8
	list, dists = make([]uint32, n), make([]float32, n)
	for i := range n {
		list[i] = binary.LittleEndian.Uint32(value[1+4*i:])
		dists[i] = getFloat32(value[1+4*n+4*i:])
	}
	return list, dists, nil
}

// loadPending reads what waited when the index was last synced back into
// memory, by the posting the map has every member in now. The changes of a
// posting are counted as one per member that waits: how many times each list
// changed is not kept.
func (g *growIndex) loadPending() error {
	if g.waiting == nil {
		return nil
	}
	side := g.st.side
	err := growScan(g.waiting, func(key, value []byte) error {
		if len(key) != 4 {
			return errors.Errorf("a change to the edges that waits with a key of %d bytes", len(key))
		}
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= len(g.live) || !g.live[docID] {
			return errors.Errorf("a change to the edges of doc %d waits, and the doc is not in the map", docID)
		}
		list, dists, err := growPendingDecode(value)
		if err != nil {
			return errors.Wrapf(err, "doc %d", docID)
		}
		g.keepPending(side.postingOf[docID], uint64(docID), list, dists)
		return nil
	})
	// No insert has changed anything yet.
	g.touched = g.touched[:0]
	return errors.Wrap(err, "read the changes to the edges that waited")
}

// writePosting writes the row of a posting that has none. From a split it is
// counted as a row written by one.
func (r growRows) writePosting(p uint32, docIDs []uint64, lists [][]uint32, split bool) error {
	block, err := growEdgesBlock(docIDs, lists)
	if err != nil {
		return err
	}
	if err := r.g.putRow(p, nil, block); err != nil {
		return errors.Wrapf(err, "write the row of edges of posting %d", p)
	}
	if split {
		r.g.stats.RowWrites.add(1, len(block))
		r.g.stats.RowWritesSplit++
	}
	return nil
}

// fetch reads the row of a posting: one value with the whole row, the last
// one written.
func (r growRows) fetch(p uint32, dp *diskPosting) error {
	block, err := r.g.getRow(p)
	if err != nil {
		return errors.Wrapf(err, "read the row of edges of posting %d", p)
	}
	dp.lists, dp.block = nil, block
	dp.bytes[diskBlockEdges] = len(block)
	return nil
}

func (r growRows) join(p uint32, dp *diskPosting) error {
	return r.g.joinRows(p, dp)
}

// add leaves the list of a new member, and its distances, waiting with the
// other changes of its posting. It is the first change of an insert.
func (r growRows) add(p uint32, docID uint64, edges []uint32, dists []float32) error {
	r.g.touched = r.g.touched[:0]
	return r.g.setPending(p, docID, edges, dists)
}

// replace leaves the new list waiting in memory, with its distances, until
// the row of its posting is written.
func (r growRows) replace(p uint32, docID uint64, _, updated []uint32, dists []float32) error {
	return r.g.setPending(p, docID, updated, dists)
}

// distsOf returns the distances of the edges of a member of posting p: the
// ones that wait with its list, or the ones in their bucket. read is the
// bytes it read from the bucket, 0 when they were waiting.
func (r growRows) distsOf(p uint32, docID uint64) (dists []float32, read int, err error) {
	if pend := r.g.pending[p]; pend != nil {
		if dists, waiting := pend.dists[docID]; waiting {
			return dists, 0, nil
		}
	}
	return r.g.storedDists(docID)
}

// retire forgets what waited of a posting that was split, with its distances
// written first, and deletes its row: the rows of its halves hold its lists.
func (r growRows) retire(p uint32) error {
	if err := r.g.dropPending(p, true); err != nil {
		return err
	}
	if err := r.g.dropRow(p); err != nil {
		return errors.Wrapf(err, "delete the row of edges of posting %d", p)
	}
	r.g.stats.RowDeletes++
	return nil
}

// settle writes the rows of edges of the postings the insert changed, once
// they have their share of changes. A posting that split has none left.
func (r growRows) settle() error {
	g := r.g
	for _, q := range g.touched {
		if pend := g.pending[q]; pend != nil && pend.changes >= g.meta.Consolidate {
			if err := g.writeRow(q, &g.stats.RowWritesFull); err != nil {
				return err
			}
		}
	}
	return nil
}

// flush writes the row of edges of every posting that has changes waiting.
// After it nothing waits: what the index is, is in its store.
func (r growRows) flush() error {
	g := r.g
	postings := make([]uint32, 0, len(g.pending))
	for p := range g.pending {
		postings = append(postings, p)
	}
	slices.Sort(postings)
	for _, p := range postings {
		if err := g.writeRow(p, &g.stats.RowWritesClose); err != nil {
			return err
		}
	}
	return nil
}

// growPostingKey is the key of a posting in the bucket of edges.
func growPostingKey(p uint32) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, p)
	return key
}

// growEdgesBlock is the row of edges of a posting as one value: how many
// members, the doc id and the degree of each, in the order of the row of
// members, and then their edges, member after member.
func growEdgesBlock(ids []uint64, lists [][]uint32) ([]byte, error) {
	total := 0
	for _, list := range lists {
		if len(list) > math.MaxUint8 {
			return nil, errors.Errorf("a list of %d edges, a row holds up to %d a member", len(list), math.MaxUint8)
		}
		total += len(list)
	}
	block := make([]byte, 4+5*len(ids)+4*total)
	binary.LittleEndian.PutUint32(block, uint32(len(ids)))
	at := 4 + 5*len(ids)
	for i, id := range ids {
		binary.LittleEndian.PutUint32(block[4+5*i:], uint32(id))
		block[4+5*i+4] = uint8(len(lists[i]))
		for _, endpoint := range lists[i] {
			binary.LittleEndian.PutUint32(block[at:], endpoint)
			at += 4
		}
	}
	return block, nil
}

func (b *growBlock) decode(block []byte) error {
	if len(block) < 4 {
		return errors.Errorf("a row of edges of %d bytes", len(block))
	}
	n := int(binary.LittleEndian.Uint32(block))
	if len(block) < 4+5*n {
		return errors.Errorf("a row of edges of %d bytes for %d members", len(block), n)
	}
	b.ids, b.start = resized(b.ids, n), resized(b.start, n+1)
	b.start[0] = 0
	for i := range n {
		b.ids[i] = binary.LittleEndian.Uint32(block[4+5*i:])
		b.start[i+1] = b.start[i] + uint32(block[4+5*i+4])
	}
	rest := block[4+5*n:]
	if len(rest) != 4*int(b.start[n]) {
		return errors.Errorf("a row of edges with %d bytes of edges, its degrees add up to %d edges", len(rest), b.start[n])
	}
	b.edges = resized(b.edges, int(b.start[n]))
	for i := range b.edges {
		b.edges[i] = binary.LittleEndian.Uint32(rest[4*i:])
	}
	return nil
}

// getRow reads the row of edges of posting p of an index with consolidated
// edges: nil if it has none. Next to the members it is the one value of its
// key, and the store holding more than one stops whatever asked.
func (g *growIndex) getRow(p uint32) ([]byte, error) {
	if !g.meta.RowsWithMembers {
		return g.edges.Get(growPostingKey(p))
	}
	values, err := g.postings.SetList(diskKey(p, diskBlockEdges))
	if err != nil {
		return nil, err
	}
	switch len(values) {
	case 0:
		return nil, nil
	case 1:
		return values[0], nil
	}
	return nil, errors.Errorf("STOP: the store holds %d rows of edges for posting %d, not one", len(values), p)
}

// putRow writes the row of edges of posting p, whose row so far is old, nil
// if it has none. In a bucket of its own the new row replaces it. Next to the
// members there is no replacing: the old row is taken out, with a tombstone
// that carries it, and the new one is added.
func (g *growIndex) putRow(p uint32, old, block []byte) error {
	if !g.meta.RowsWithMembers {
		return g.edges.Put(growPostingKey(p), block)
	}
	if old != nil {
		if bytes.Equal(old, block) {
			return nil
		}
		if err := g.postings.SetDeleteSingle(diskKey(p, diskBlockEdges), slices.Clone(old)); err != nil {
			return err
		}
		g.stats.RowTombstones.add(1, len(old))
	}
	return g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{block})
}

// dropRow takes out the row of edges of posting p, which is retired. Next to
// the members there is nothing to write: the keys of a retired posting are
// the ones the store is told to skip (skipKey), its row of edges with its
// row of members, as in v5.0. A tombstone for it would be skipped too, when
// its memtable is flushed, and the row it takes out may be in an older
// segment: until a compaction gets to it the store still holds that row,
// which nothing reads, because the posting is never loaded again.
func (g *growIndex) dropRow(p uint32) error {
	if !g.meta.RowsWithMembers {
		return g.edges.Delete(growPostingKey(p))
	}
	return nil
}

// joinRows lays the edges of a fetched posting out by member, in the order
// of its members, from its row with what waits in memory over it.
func (g *growIndex) joinRows(p uint32, dp *diskPosting) error {
	row := &dp.row
	if dp.block == nil {
		row.ids, row.start, row.edges = row.ids[:0], append(row.start[:0], 0), row.edges[:0]
	} else if err := row.decode(dp.block); err != nil {
		return errors.Wrapf(err, "posting %d", p)
	}
	pend := g.pending[p]
	n := len(dp.docIDs)
	dp.edgeStart, dp.edges = resized(dp.edgeStart, n+1), dp.edges[:0]
	dp.edgeStart[0] = 0
	var byDoc map[uint32]int
	for i, docID := range dp.docIDs {
		var list []uint32
		found := false
		if pend != nil {
			list, found = pend.lists[docID]
		}
		if !found {
			// The row is in the order of the members: the same place is
			// looked at first.
			at := -1
			if i < len(row.ids) && uint64(row.ids[i]) == docID {
				at = i
			} else {
				if byDoc == nil {
					byDoc = make(map[uint32]int, len(row.ids))
					for j, id := range row.ids {
						byDoc[id] = j
					}
				}
				if j, ok := byDoc[uint32(docID)]; ok {
					at = j
				}
			}
			if at < 0 {
				return errors.Errorf("doc %d, a member of posting %d, has no list of edges: neither in its row nor waiting", docID, p)
			}
			list = row.edges[row.start[at]:row.start[at+1]]
		}
		dp.edges = append(dp.edges, list...)
		dp.edgeStart[i+1] = uint32(len(dp.edges))
	}
	return nil
}

// setPending leaves the list of a member of posting p, and the distances of
// its edges, waiting in memory: a change of the posting. Where what waits is
// kept in a bucket, it is written there first.
func (g *growIndex) setPending(p uint32, docID uint64, list []uint32, dists []float32) error {
	if g.waiting != nil {
		value := growPendingValue(list, dists)
		if err := g.waiting.Put(growDocKey(docID), value); err != nil {
			return errors.Wrapf(err, "write the change to the edges of doc %d", docID)
		}
		g.stats.PendingLog.add(1, len(value))
	}
	g.keepPending(p, docID, list, dists)
	return nil
}

// keepPending is setPending in memory alone.
func (g *growIndex) keepPending(p uint32, docID uint64, list []uint32, dists []float32) {
	pend := g.pending[p]
	if pend == nil {
		pend = &growPending{lists: make(map[uint64][]uint32), dists: make(map[uint64][]float32)}
		g.pending[p] = pend
	}
	if old, waiting := pend.lists[docID]; waiting {
		pend.bytes -= int64(4*len(old) + 4*len(pend.dists[docID]))
		g.pendingBytes -= int64(4*len(old) + 4*len(pend.dists[docID]))
	} else {
		pend.order = append(pend.order, docID)
		g.pendingMembers++
	}
	pend.lists[docID], pend.dists[docID] = slices.Clone(list), slices.Clone(dists)
	pend.bytes += int64(4*len(list) + 4*len(dists))
	g.pendingBytes += int64(4*len(list) + 4*len(dists))
	pend.changes++
	if g.pendingBytes > g.stats.PendingPeakBytes {
		g.stats.PendingPeakBytes, g.stats.PendingPeak, g.stats.PendingPeakPosts = g.pendingBytes, g.pendingMembers, len(g.pending)
	}
	if !slices.Contains(g.touched, p) {
		g.touched = append(g.touched, p)
	}
}

// dropPending forgets what waits of posting p. With its distances written
// first, which is what a split and the writing of a row want: the lists then
// are in a row on disk. Only then do they leave the bucket of what waits,
// where they are kept in one: a change is in it, in a row, or in both.
func (g *growIndex) dropPending(p uint32, writeDists bool) error {
	pend := g.pending[p]
	if pend == nil {
		return nil
	}
	if writeDists {
		for _, docID := range pend.order {
			value := growDistsValue(pend.dists[docID])
			if err := g.dists.Put(growDocKey(docID), value); err != nil {
				return errors.Wrapf(err, "write the distances of the edges of doc %d", docID)
			}
			g.stats.RowDists.add(1, len(value))
		}
	}
	if g.waiting != nil {
		// The row that holds these changes and their distances are handed to
		// the system before the changes leave the bucket of what waits: every
		// bucket has a log of its own, written when it fills, and a process
		// that died with the row still in one and the deletes already out of
		// the other would leave members without edges.
		rows := g.postings
		if g.edges != nil {
			rows = g.edges
		}
		if err := rows.WriteWAL(); err != nil {
			return errors.Wrap(err, "write the log of the rows of edges")
		}
		if err := g.dists.WriteWAL(); err != nil {
			return errors.Wrap(err, "write the log of the distances of the edges")
		}
		for _, docID := range pend.order {
			if err := g.waiting.Delete(growDocKey(docID)); err != nil {
				return errors.Wrapf(err, "take the change to the edges of doc %d out of those that wait", docID)
			}
		}
	}
	g.pendingMembers -= int64(len(pend.order))
	g.pendingBytes -= pend.bytes
	delete(g.pending, p)
	return nil
}

// writeRow writes the row of edges of posting p whole, as a new version: the
// one on disk with what waits in memory over it. The members it has no list
// of yet, the ones that came since it was written, go after the others, in
// the order they came. why counts what had it written.
func (g *growIndex) writeRow(p uint32, why *int) error {
	pend := g.pending[p]
	if pend == nil {
		return nil
	}
	old, err := g.getRow(p)
	if err != nil {
		return errors.Wrapf(err, "read the row of edges of posting %d", p)
	}
	g.stats.RowReads.add(1, len(old))
	var row growBlock
	if old != nil {
		if err := row.decode(old); err != nil {
			return errors.Wrapf(err, "posting %d", p)
		}
	}
	ids := make([]uint64, 0, len(row.ids)+len(pend.order))
	lists := make([][]uint32, 0, len(row.ids)+len(pend.order))
	inRow := make(map[uint64]bool, len(row.ids))
	for i, id := range row.ids {
		docID := uint64(id)
		inRow[docID] = true
		list, waiting := pend.lists[docID]
		if !waiting {
			list = row.edges[row.start[i]:row.start[i+1]]
		}
		ids, lists = append(ids, docID), append(lists, list)
	}
	for _, docID := range pend.order {
		if !inRow[docID] {
			ids, lists = append(ids, docID), append(lists, pend.lists[docID])
		}
	}
	block, err := growEdgesBlock(ids, lists)
	if err != nil {
		return err
	}
	if err := g.putRow(p, old, block); err != nil {
		return errors.Wrapf(err, "write the row of edges of posting %d", p)
	}
	g.stats.RowWrites.add(1, len(block))
	*why++
	return g.dropPending(p, true)
}
