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
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// What the tests and the benchmarks check an index that grows on disk
// (grow.go) with, and the control of its reorganization: none of it is part
// of the index. It was in grow_test.go with the index until the index moved
// to the package.

// cleanCentroidGraph removes the deleted centroids from the graph for good.
func (g *growIndex) cleanCentroidGraph() error {
	return g.st.centroids.graph.CleanUpTombstonedNodes(func() bool { return false })
}

// checkCoverage checks the invariant of the inserts against what is on disk,
// as the streaming index checks it against its memory
// (streamIndex.checkCoverage): every live vector is in exactly one posting,
// the one the persisted map gives it, with a routing code; no live posting
// holds anything else; every member has one list of edges, which lead to
// live vectors outside its own posting, with a distance each; and the
// centroid graph holds the live postings and none of the retired ones. It
// reads every row of the store.
func (g *growIndex) checkCoverage() error {
	st := g.st
	keys := len(g.live)
	mapped := make([]uint32, keys)
	for i := range mapped {
		mapped[i] = unplaced
	}
	inMap := 0
	err := growScan(g.posts, func(key, value []byte) error {
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= keys {
			return errors.Errorf("the map on disk holds doc %d, past the %d keys", docID, keys)
		}
		mapped[docID] = binary.LittleEndian.Uint32(value)
		inMap++
		return nil
	})
	if err != nil {
		return err
	}
	if inMap != g.meta.Live {
		return errors.Errorf("the map on disk holds %d docs, %d are live", inMap, g.meta.Live)
	}
	for docID, p := range mapped {
		if p != st.side.postingOf[docID] {
			return errors.Errorf("the map on disk puts doc %d in posting %d, the one in memory in %d", docID, p, st.side.postingOf[docID])
		}
		if g.live[docID] != (p != unplaced) {
			return errors.Errorf("doc %d is live: %v, and the map on disk puts it in %d", docID, g.live[docID], p)
		}
	}
	coded := 0
	words := st.side.codeWords
	err = growScan(g.routing, func(key, value []byte) error {
		docID := binary.BigEndian.Uint32(key)
		if int(docID) >= keys || !g.live[docID] {
			return errors.Errorf("a routing code on disk for doc %d, which is not live", docID)
		}
		for i := range words {
			if binary.LittleEndian.Uint64(value[8*i:]) != st.side.codes[int(docID)*words+i] {
				return errors.Errorf("doc %d has another routing code on disk than in memory", docID)
			}
		}
		coded++
		return nil
	})
	if err != nil {
		return err
	}
	if coded != g.meta.Live {
		return errors.Errorf("%d routing codes on disk for %d live docs", coded, g.meta.Live)
	}

	if len(g.retired) != g.meta.Postings || len(g.size) != g.meta.Postings || len(st.centroids.vectors) != g.meta.Postings {
		return errors.Errorf("%d postings, %d retired flags, %d sizes and %d centroids",
			g.meta.Postings, len(g.retired), len(g.size), len(st.centroids.vectors))
	}
	count := make([]uint8, keys)
	var dp diskPosting
	for p := range uint32(g.meta.Postings) {
		if g.retired[p] {
			if g.size[p] != 0 {
				return errors.Errorf("retired posting %d counts %d vectors", p, g.size[p])
			}
			if st.centroids.graph.ContainsDoc(uint64(p)) {
				return errors.Errorf("retired posting %d is still in the centroid graph", p)
			}
			if g.consolidated() {
				// Its row of edges went with it, and nothing of it waits.
				// Next to the members the store may still hold the row,
				// with the members, until a compaction skips its keys.
				var row []byte
				if !g.meta.RowsWithMembers {
					var err error
					if row, err = g.getRow(p); err != nil {
						return errors.Wrapf(err, "read the row of edges of posting %d", p)
					}
				}
				if row != nil || g.pending[p] != nil {
					return errors.Errorf("retired posting %d still has a row of edges, or changes waiting", p)
				}
			}
			continue
		}
		if !st.centroids.graph.ContainsDoc(uint64(p)) {
			return errors.Errorf("live posting %d is not in the centroid graph", p)
		}
		if err := g.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := g.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := g.joinLists(p, &dp); err != nil {
			return err
		}
		if len(dp.docIDs) == 0 {
			return errors.Errorf("live posting %d is empty", p)
		}
		if g.consolidated() {
			// Its row of edges and what waits hold a list for its members
			// and for nobody else: joinLists found one for each.
			lists := len(dp.row.ids)
			if pend := g.pending[p]; pend != nil {
				for _, docID := range pend.order {
					if !slices.Contains(dp.row.ids, uint32(docID)) {
						lists++
					}
				}
			}
			if lists != len(dp.docIDs) {
				return errors.Errorf("posting %d has %d members and %d lists of edges, in its row and waiting", p, len(dp.docIDs), lists)
			}
		}
		for i, docID := range dp.docIDs {
			if docID >= uint64(keys) || !g.live[docID] {
				return errors.Errorf("posting %d holds doc %d, which is not live", p, docID)
			}
			if mapped[docID] != p {
				return errors.Errorf("posting %d holds doc %d, which the map puts in %d", p, docID, mapped[docID])
			}
			if count[docID]++; count[docID] > 1 {
				return errors.Errorf("doc %d is in two postings", docID)
			}
			edges := dp.edgesOfMember(i)
			for _, endpoint := range edges {
				if int(endpoint) >= keys || !g.live[endpoint] {
					return errors.Errorf("doc %d has an edge to doc %d, which is not live", docID, endpoint)
				}
				// A merge of two postings leaves the edges between them
				// as they were.
				if mapped[endpoint] == p && g.meta.Merges == 0 {
					return errors.Errorf("doc %d has an edge into its own posting %d, to doc %d", docID, p, endpoint)
				}
			}
			stored, _, err := g.distsOf(p, docID)
			if err != nil {
				return err
			}
			if len(stored) != len(edges) {
				return errors.Errorf("doc %d has %d edges and %d bytes of distances", docID, len(edges), 4*len(stored))
			}
		}
	}
	for docID, live := range g.live {
		if live && count[docID] != 1 {
			return errors.Errorf("live doc %d is in %d postings", docID, count[docID])
		}
	}
	if err := g.checkWaiting(); err != nil {
		return err
	}
	return g.checkSlots()
}

// checkWaiting checks, in an index that keeps what waits of its edges in a
// bucket, that the bucket holds what waits in memory and nothing else: the
// same members, each with the same list and the same distances.
func (g *growIndex) checkWaiting() error {
	if g.waiting == nil {
		return nil
	}
	inBucket := int64(0)
	err := growScan(g.waiting, func(key, value []byte) error {
		docID := uint64(binary.BigEndian.Uint32(key))
		if docID >= uint64(len(g.live)) || !g.live[docID] {
			return errors.Errorf("a change to the edges of doc %d waits on disk, and the doc is not live", docID)
		}
		p := g.st.side.postingOf[docID]
		pend := g.pending[p]
		if pend == nil {
			return errors.Errorf("a change to the edges of doc %d waits on disk, and nothing of posting %d waits in memory", docID, p)
		}
		list, dists, err := growPendingDecode(value)
		if err != nil {
			return err
		}
		inMemory, waits := pend.lists[docID]
		if !waits || !slices.Equal(inMemory, list) || !slices.Equal(pend.dists[docID], dists) {
			return errors.Errorf("doc %d has the edges %v waiting on disk, and %v in memory (waiting: %v)", docID, list, inMemory, waits)
		}
		inBucket++
		return nil
	})
	if err != nil {
		return err
	}
	if inBucket != g.pendingMembers {
		return errors.Errorf("%d changes to the edges wait on disk, and %d in memory", inBucket, g.pendingMembers)
	}
	return nil
}

// checkSlots checks, in an index whose vectors have slots, that every live
// doc has one of its own, under the next one to give, and that the table on
// disk is the one in memory.
func (g *growIndex) checkSlots() error {
	if g.meta.VectorGen == 0 {
		return nil
	}
	if len(g.slotOf) != len(g.live) {
		return errors.Errorf("a table of %d slots for %d doc ids", len(g.slotOf), len(g.live))
	}
	raw, err := os.ReadFile(filepath.Join(g.dir, growSlotsName(g.meta.VectorGen)))
	if err != nil {
		return err
	}
	taken := make(map[uint32]int, g.meta.Live)
	for docID, live := range g.live {
		slot := g.slotOf[docID]
		if !live {
			continue
		}
		if slot == diskNoSlot || slot >= g.nextSlot {
			return errors.Errorf("live doc %d has the slot %d, with %d given", docID, slot, g.nextSlot)
		}
		if other, twice := taken[slot]; twice {
			return errors.Errorf("docs %d and %d share the slot %d", other, docID, slot)
		}
		taken[slot] = docID
		if 4*docID+4 > len(raw) || binary.LittleEndian.Uint32(raw[4*docID:]) != slot+1 {
			return errors.Errorf("doc %d has the slot %d in memory and another on disk", docID, slot)
		}
	}
	return nil
}

// sameAs tells the first difference between the index on disk and a
// streaming index that grew by the same inserts: the postings with their
// members in order, their codes and their edges, read from the store, the
// posting map and the routing codes. Nil when there is none.
func (g *growIndex) sameAs(s *streamIndex) error {
	lists := s.st.postings.lists
	if len(lists) != g.meta.Postings {
		return errors.Errorf("%d postings on disk, %d in memory", g.meta.Postings, len(lists))
	}
	if !slices.Equal(g.live, s.live) {
		return errors.New("the live docs differ")
	}
	if !slices.Equal(g.st.side.postingOf, s.st.side.postingOf) {
		return errors.New("the posting maps differ")
	}
	if !slices.Equal(g.st.side.codes, s.st.side.codes) {
		return errors.New("the routing codes differ")
	}
	if !slices.Equal(g.retired, s.retired) {
		return errors.New("the retired postings differ")
	}
	var dp diskPosting
	for p := range uint32(len(lists)) {
		l := &lists[p]
		if g.retired[p] {
			continue
		}
		if err := g.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := g.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := g.joinLists(p, &dp); err != nil {
			return err
		}
		switch {
		case !slices.Equal(dp.docIDs, l.docIDs):
			return errors.Errorf("posting %d holds other members on disk, or in another order: %d and %d of them",
				p, len(dp.docIDs), len(l.docIDs))
		case !slices.Equal(dp.codes, l.codes):
			return errors.Errorf("posting %d holds other codes on disk", p)
		}
		for i, docID := range dp.docIDs {
			if !slices.Equal(dp.edgesOfMember(i), l.edgesOf(i)) {
				return errors.Errorf("doc %d, in posting %d, has the edges %v on disk and %v in memory",
					docID, p, dp.edgesOfMember(i), l.edgesOf(i))
			}
		}
	}
	return nil
}

// reorganizeGrowIndex writes to dir, which must exist and be empty, the index
// src holds, laid out again as a reorganization in the background would
// leave it, without the compaction of the store: the control of v5.0
// (bench-report.md, "control de reorganización"). Every row of every live
// posting is read from src and added to a new bucket that is flushed once,
// so that it ends as one segment, with nothing that was replaced in it. The
// format of the rows is the same. The other buckets are written again entry
// by entry, and the centroids and their graph are copied.
//
// With consolidated edges (the combined control of v5.1) the row of edges of
// every live posting, which is one value already, is written to a new bucket
// of rows that is flushed once too, with its lists in the order of the
// members of the posting. Nothing may be waiting in memory: what waits is
// written first (flushPending).
//
// With orderVectors the exact vectors are written again too, those of every
// posting in a row, in the order of its members, and the index gets the
// table that tells where the vector of every doc id is. Without it the
// vectors file is the one of src, linked.
func reorganizeGrowIndex(ctx context.Context, logger logrus.FieldLogger, src *growIndex, dir string, orderVectors bool,
) (err error) {
	if entries, err := os.ReadDir(dir); err != nil {
		return err
	} else if len(entries) > 0 {
		return errors.Errorf("%s is not empty", dir)
	}
	if src.frozen {
		return errors.New("the index was laid out again already")
	}
	if src.meta.VectorGen > 0 {
		return errors.New("the vectors of the index have slots: it lays them out again itself (growIndex.layOut)")
	}
	if len(src.pending) > 0 {
		return errors.Errorf("%d postings have changes to their edges waiting in memory: they are written first", len(src.pending))
	}
	if err := src.sync(); err != nil {
		return err
	}
	path := func(name string) string { return filepath.Join(dir, name) }
	for _, name := range []string{diskMeanFile, diskCentroidFile} {
		raw, err := os.ReadFile(filepath.Join(src.dir, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(path(name), raw, 0o644); err != nil {
			return err
		}
	}
	if err := os.CopyFS(path(diskGraphDir), os.DirFS(filepath.Join(src.dir, diskGraphDir))); err != nil {
		return errors.Wrap(err, "copy the centroid graph")
	}

	dst := &growIndex{dir: dir, logger: logger, provider: src.provider, meta: src.meta}
	if err := dst.openStore(ctx, growOpen{}); err != nil {
		return err
	}
	defer func() {
		if shut := dst.lsm.Shutdown(ctx); err == nil {
			err = errors.Wrap(shut, "close the store")
		}
	}()
	// Nothing is flushed until everything is written: the store only
	// flushes when its cycles are called, and nothing calls them here.
	for _, buckets := range [][2]*lsmkv.Bucket{{src.posts, dst.posts}, {src.routing, dst.routing}, {src.dists, dst.dists}} {
		err := growScan(buckets[0], func(key, value []byte) error {
			return buckets[1].Put(slices.Clone(key), slices.Clone(value))
		})
		if err != nil {
			return errors.Wrap(err, "write a bucket again")
		}
	}

	dims, words := src.meta.Dims, src.meta.ScanCodeWords
	var slotOf []uint32
	var vectors *os.File
	if orderVectors {
		slotOf = make([]uint32, src.meta.Keys)
		for i := range slotOf {
			slotOf[i] = diskNoSlot
		}
		if vectors, err = os.OpenFile(path(diskVectorFile), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
			return err
		}
		defer func() {
			if closed := vectors.Close(); err == nil {
				err = closed
			}
		}()
	}
	var dp diskPosting
	var raw, batch []byte
	slot := uint32(0)
	for p := range uint32(src.meta.Postings) {
		if src.retired[p] {
			continue
		}
		if err := src.fetchMembers(p, &dp); err != nil {
			return err
		}
		if err := src.fetchLists(p, &dp); err != nil {
			return err
		}
		if err := src.joinLists(p, &dp); err != nil {
			return err
		}
		n := len(dp.docIDs)
		members, lists := make([][]byte, n), make([][]byte, n)
		// With consolidated edges, the lists of the row, by member.
		var rowLists [][]uint32
		if src.consolidated() {
			rowLists = make([][]uint32, n)
		}
		batch = batch[:0]
		first := slot
		for i, docID := range dp.docIDs {
			members[i] = growMemberValue(docID, dp.codes[i*words:(i+1)*words])
			if src.consolidated() {
				rowLists[i] = dp.edgesOfMember(i)
			} else {
				lists[i] = growEdgesValue(docID, dp.edgesOfMember(i))
			}
			if orderVectors {
				if raw, _, err = src.exact(docID, raw, nil); err != nil {
					return err
				}
				batch = append(batch, raw...)
				slotOf[docID] = slot
				slot++
			}
		}
		if err := dst.postings.SetAdd(diskKey(p, diskBlockMembers), members); err != nil {
			return errors.Wrapf(err, "write the members of posting %d", p)
		}
		if src.consolidated() {
			block, err := growEdgesBlock(dp.docIDs, rowLists)
			if err != nil {
				return errors.Wrapf(err, "the row of edges of posting %d", p)
			}
			if err := dst.putRow(p, nil, block); err != nil {
				return errors.Wrapf(err, "write the row of edges of posting %d", p)
			}
		} else if err := dst.postings.SetAdd(diskKey(p, diskBlockEdges), lists); err != nil {
			return errors.Wrapf(err, "write the edges of posting %d", p)
		}
		if orderVectors {
			// The vectors of a posting, in a row.
			if _, err := vectors.WriteAt(batch, int64(first)*int64(4*dims)); err != nil {
				return errors.Wrapf(err, "write the vectors of posting %d", p)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if orderVectors {
		if err := writeArray(path(growSlotsFile), 4, binary.LittleEndian.PutUint32, slotOf); err != nil {
			return errors.Wrap(err, "write where the vectors are")
		}
	} else if err := os.Link(filepath.Join(src.dir, diskVectorFile), path(diskVectorFile)); err != nil {
		return errors.Wrap(err, "link the vectors")
	}
	if err := dst.flushAll(); err != nil {
		return err
	}
	return dst.writeMeta()
}

// sizeOnDisk returns the bytes of the files under a directory.
func sizeOnDisk(dir string) (int64, error) {
	total := int64(0)
	err := filepath.WalkDir(dir, func(_ string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}
