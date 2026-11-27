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
	"encoding/binary"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// growEdges is where an index that grows on disk keeps the edges of its
// postings: the list of every member, by posting, and how a list is read,
// added and changed. The format of the edges on disk is not settled, so the
// index reaches them only through here.
//
// Two ways of keeping them were measured. growLists, below, is the one of
// v5.0: every list is a value of its own in the bucket of postings, and a
// change is a new value and a tombstone of the old one, handed to the store
// at once. The other consolidates the lists of a posting into one row that is
// written whole, with the changes since then waiting in memory (v5.1 and
// v5.3, growRows in grow_rows.go). The meta of an index says which it was
// written with (growMeta.Consolidate), and it is opened with that one.
//
// The distances of the edges of every member, which only the inserts read,
// are in a bucket of the index by doc id in both (growIndex.dists). A store
// of edges says when they are written.
type growEdges interface {
	// buckets returns the buckets it keeps beyond those of the index.
	buckets() []*lsmkv.Bucket
	// writePosting writes the lists of all the members of a posting that
	// has none written: one of the base, or, with split, a half of a
	// posting that was split. The distances are not its to write: those of
	// the base are written by whoever writes it, and a split changes none.
	writePosting(p uint32, docIDs []uint64, lists [][]uint32, split bool) error
	// fetch reads what the store holds of the edges of posting p into dp.
	// It may run next to other reads, and next to the read of the members
	// of the same posting.
	fetch(p uint32, dp *diskPosting) error
	// join lays out by member what fetch read, in the order of the members
	// of dp: dp.edgeStart and dp.edges.
	join(p uint32, dp *diskPosting) error
	// add gives a new member of posting p its list, with the distance of
	// every edge. It is the first thing an insert changes of the edges.
	add(p uint32, docID uint64, edges []uint32, dists []float32) error
	// replace changes the list of a member of posting p from old to updated,
	// whose distances are dists.
	replace(p uint32, docID uint64, old, updated []uint32, dists []float32) error
	// distsOf returns the distances of the edges of a member of posting p,
	// and the bytes it read from the store for them.
	distsOf(p uint32, docID uint64) (dists []float32, read int, err error)
	// retire tells that posting p was split, once the lists of its halves
	// are written.
	retire(p uint32) error
	// settle runs once the writes of an insert are done.
	settle() error
	// flush writes whatever waits in memory.
	flush() error
}

// growLists keeps every list of edges as a value of its own in the row of
// edges of its posting, in the bucket of postings (v5.0): the doc id of the
// member and those its edges lead to. A list changes without its row being
// read or written again: the new list goes in, and a tombstone of the old
// one. Nothing waits in memory.
type growLists struct {
	g *growIndex
}

func (l growLists) buckets() []*lsmkv.Bucket { return nil }

func (l growLists) writePosting(p uint32, docIDs []uint64, lists [][]uint32, split bool) error {
	values := make([][]byte, len(docIDs))
	bytes := 0
	for i, docID := range docIDs {
		values[i] = growEdgesValue(docID, lists[i])
		bytes += len(values[i])
	}
	if err := l.g.postings.SetAdd(diskKey(p, diskBlockEdges), values); err != nil {
		return errors.Wrapf(err, "write the edges of posting %d", p)
	}
	if split {
		l.g.stats.SplitRowsW.add(len(values), bytes)
	}
	return nil
}

func (l growLists) fetch(p uint32, dp *diskPosting) error {
	values, err := l.g.row(p, diskBlockEdges)
	if err != nil {
		return err
	}
	dp.lists = values
	bytes := 0
	for _, value := range values {
		if len(value) < 4 || len(value)%4 != 0 {
			return errors.Errorf("a list of edges of posting %d takes %d bytes", p, len(value))
		}
		bytes += len(value)
	}
	dp.bytes[diskBlockEdges] = bytes
	return nil
}

func (l growLists) join(p uint32, dp *diskPosting) error {
	return dp.joinEdges(p)
}

func (l growLists) add(p uint32, docID uint64, edges []uint32, dists []float32) error {
	g := l.g
	list, distsValue := growEdgesValue(docID, edges), growDistsValue(dists)
	if err := g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{list}); err != nil {
		return errors.Wrapf(err, "write the edges of doc %d", docID)
	}
	if err := g.dists.Put(growDocKey(docID), distsValue); err != nil {
		return errors.Wrap(err, "write the distances of the edges")
	}
	g.stats.OwnEdges.add(1, len(list))
	g.stats.OwnDists.add(1, len(distsValue))
	return nil
}

func (l growLists) replace(p uint32, docID uint64, old, updated []uint32, dists []float32) error {
	g := l.g
	gone, list, distsValue := growEdgesValue(docID, old), growEdgesValue(docID, updated), growDistsValue(dists)
	if err := g.postings.SetDeleteSingle(diskKey(p, diskBlockEdges), gone); err != nil {
		return errors.Wrapf(err, "take the old edges of doc %d out", docID)
	}
	if err := g.postings.SetAdd(diskKey(p, diskBlockEdges), [][]byte{list}); err != nil {
		return errors.Wrapf(err, "write the edges of doc %d", docID)
	}
	if err := g.dists.Put(growDocKey(docID), distsValue); err != nil {
		return errors.Wrapf(err, "write the distances of the edges of doc %d", docID)
	}
	g.stats.InverseTombs.add(1, len(gone))
	g.stats.InverseLists.add(1, len(list))
	g.stats.InverseDistsW.add(1, len(distsValue))
	return nil
}

func (l growLists) distsOf(_ uint32, docID uint64) (dists []float32, read int, err error) {
	return l.g.storedDists(docID)
}

// retire has nothing to write: the rows of a posting that was split are never
// read again, and the store skips their keys (growIndex.skipKey).
func (l growLists) retire(uint32) error { return nil }

func (l growLists) settle() error { return nil }

func (l growLists) flush() error { return nil }

// joinEdges lays the lists of edges of a fetched posting out by member, in
// the order of its members. Every member has one list and only one.
func (dp *diskPosting) joinEdges(p uint32) error {
	n := len(dp.docIDs)
	if len(dp.lists) != n {
		// Which member is it about: the first with two lists, if any.
		seen := make(map[uint32][]byte, len(dp.lists))
		for _, value := range dp.lists {
			docID := binary.LittleEndian.Uint32(value)
			if other, twice := seen[docID]; twice {
				return errors.Errorf("posting %d has %d members and %d lists of edges: two of doc %d, % x and % x",
					p, n, len(dp.lists), docID, other, value)
			}
			seen[docID] = value
		}
		return errors.Errorf("posting %d has %d members and %d lists of edges", p, n, len(dp.lists))
	}
	dp.listOf = resized(dp.listOf, n)
	for i := range dp.listOf {
		dp.listOf[i] = -1
	}
	total := 0
	// The lists come in the order of the members until one is replaced: the
	// next member is looked at first.
	next := 0
	var byDoc map[uint64]int
	for at, value := range dp.lists {
		docID := uint64(binary.LittleEndian.Uint32(value))
		var member int
		if next < n && dp.docIDs[next] == docID {
			member = next
		} else {
			if byDoc == nil {
				byDoc = make(map[uint64]int, n)
				for i, id := range dp.docIDs {
					byDoc[id] = i
				}
			}
			found, ok := byDoc[docID]
			if !ok {
				return errors.Errorf("posting %d has a list of edges of doc %d, which is not a member", p, docID)
			}
			member = found
		}
		next = member + 1
		if dp.listOf[member] != -1 {
			return errors.Errorf("doc %d has two lists of edges in posting %d", docID, p)
		}
		dp.listOf[member] = int32(at)
		total += len(value)/4 - 1
	}
	dp.edgeStart, dp.edges = resized(dp.edgeStart, n+1), resized(dp.edges, total)
	dp.edgeStart[0] = 0
	at := 0
	for i := range n {
		value := dp.lists[dp.listOf[i]]
		for e := 4; e < len(value); e += 4 {
			dp.edges[at] = binary.LittleEndian.Uint32(value[e:])
			at++
		}
		dp.edgeStart[i+1] = uint32(at)
	}
	dp.lists = nil
	return nil
}
