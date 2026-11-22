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
	"math"
	"slices"

	"github.com/pkg/errors"
)

// The halo of the postings of an index that grows on disk: what lets it be
// searched with no code of any vector in memory (rq.cacheMode "none"). The
// search ranks the postings it has not loaded by the routing codes of the
// endpoints the edges of a loaded posting lead to. With the codes in memory
// it reads them from a table by doc id. Without them, the code of every
// endpoint travels with the posting that leads to it: a third row of the
// posting, next to its members and its edges, which the search reads with
// them. The index built in batch that the experiments read from disk has it
// (v4.0, bench-report.md, "v4.0: prototipo en disco"); here it is kept up by
// every write.
//
// What holds, after every write: for every edge of a member of a posting, the
// halo of that posting has an entry for its endpoint, with the routing code
// the index keeps for it by doc id.
//
// The row is a set of values, each one entry or several, an entry being the
// doc id of an endpoint and its code. A write adds the entries a posting
// lacks and takes none out, and the row is read as the store holds it, with
// nothing resolved: a row with tombstones goes through a map of all its
// values in the store, which for a halo costs more than the rest of the load.
// Three things follow:
//
//   - An entry stays after the last edge that led to its endpoint is gone. It
//     costs bytes and nothing else.
//   - The same entry may be there more than once: two writes that add it
//     without knowing of each other. The compaction of the store keeps one.
//   - A doc id that left the index and was given again has a new code, and a
//     posting may hold an entry with the old one. The values of a row come
//     back in the order they were added: of two entries for an endpoint, the
//     later one counts.
//
// The entries nothing leads to any more leave when the halo of a posting is
// written whole: for the two halves of a split, and, for a posting that does
// not split, once what was added to its halo since it was last written whole
// is more than half of what its edges need (trimHalo). A halo written whole
// is one value that starts with a mark, and whoever reads the row forgets at
// the mark everything that came before it: the halo is its last whole value
// and the entries added after it. Where a halo is written whole over another
// every value the row held is taken out first, so that a compaction drops
// them; until then the store still returns them, since the row is read with
// nothing resolved, and the mark is what leaves them out.
//
// The routing codes stay in their bucket by doc id, which is where a write
// reads the code of an endpoint from when they are not in memory.

const (
	// growHaloTrimFloor is the entries added to the halo of a posting under
	// which it is not looked at for writing whole again.
	growHaloTrimFloor = 32
	// growHaloWhole is, in the place of the doc id of an endpoint, the mark a
	// halo written whole starts with: no doc id is that large.
	growHaloWhole = math.MaxUint32
)

// growHalo is the halo of one posting, decoded: its entries in the order the
// store returned them, and a table from the doc id of an endpoint to its
// entry.
type growHalo struct {
	words int
	slots []uint32
	codes []uint64
	// table is an open addressing table over the doc ids: 0 for an empty
	// place, or one more than the position of the entry.
	table []uint32
}

// reset empties the halo for entries of codes of the given width.
func (h *growHalo) reset(words int) {
	h.words = words
	h.slots, h.codes = h.slots[:0], h.codes[:0]
	clear(h.table)
}

// place returns the place of a doc id in the table: where it is, or the
// empty one it would take.
func (h *growHalo) place(slot uint32) int {
	mask := uint32(len(h.table) - 1)
	at := (slot * 2654435761) & mask
	for h.table[at] != 0 && h.slots[h.table[at]-1] != slot {
		at = (at + 1) & mask
	}
	return int(at)
}

// room gives the table room for n entries, keeping those it has.
func (h *growHalo) room(n int) {
	size := 64
	for size < 2*n {
		size *= 2
	}
	if size <= len(h.table) {
		return
	}
	if cap(h.table) >= size {
		h.table = h.table[:size]
		clear(h.table)
	} else {
		h.table = make([]uint32, size)
	}
	// Of two entries for a doc id the later one counts: placing them in
	// order leaves the last one in the table.
	for i, slot := range h.slots {
		h.table[h.place(slot)] = uint32(i + 1)
	}
}

// add appends an entry, which counts from now on for its doc id.
func (h *growHalo) add(slot uint32, code []uint64) {
	h.room(len(h.slots) + 1)
	h.slots = append(h.slots, slot)
	h.codes = append(h.codes, code...)
	h.table[h.place(slot)] = uint32(len(h.slots))
}

// find returns the code of an endpoint, nil if the halo has no entry for it.
func (h *growHalo) find(slot uint32) []uint64 {
	if len(h.table) == 0 {
		return nil
	}
	at := h.table[h.place(slot)]
	if at == 0 {
		return nil
	}
	from := int(at-1) * h.words
	return h.codes[from : from+h.words : from+h.words]
}

// has tells whether the halo has an entry for the endpoint with that code.
func (h *growHalo) has(slot uint32, code []uint64) bool {
	held := h.find(slot)
	if held == nil {
		return false
	}
	for i, word := range code {
		if held[i] != word {
			return false
		}
	}
	return true
}

// decode reads the values of a row into the halo, in their order. At the mark
// of a halo written whole it forgets what it read so far.
func (h *growHalo) decode(values [][]byte, words int) error {
	entry := 4 + 8*words
	n := 0
	for _, value := range values {
		if len(value)%entry != 0 {
			return errors.Errorf("a value of %d bytes, an entry takes %d", len(value), entry)
		}
		n += len(value) / entry
	}
	h.reset(words)
	h.room(n)
	if cap(h.codes) < n*words {
		h.codes = make([]uint64, 0, n*words)
	}
	for _, value := range values {
		for at := 0; at < len(value); at += entry {
			slot := binary.LittleEndian.Uint32(value[at:])
			if slot == growHaloWhole {
				h.slots, h.codes = h.slots[:0], h.codes[:0]
				clear(h.table)
				continue
			}
			h.slots = append(h.slots, slot)
			for w := range words {
				h.codes = append(h.codes, binary.LittleEndian.Uint64(value[at+4+8*w:]))
			}
			h.table[h.place(slot)] = uint32(len(h.slots))
		}
	}
	return nil
}

// copyFrom leaves in the halo what another holds.
func (h *growHalo) copyFrom(o *growHalo) {
	h.words = o.words
	h.slots = append(h.slots[:0], o.slots...)
	h.codes = append(h.codes[:0], o.codes...)
	h.table = append(h.table[:0], o.table...)
}

// score estimates the distance to the given endpoints from their codes in
// the halo, as the sidecar does from its own.
func (h *growHalo) score(estimator codeEstimator, slots []uint32, dists []float32) error {
	for i, slot := range slots {
		code := h.find(slot)
		if code == nil {
			return errors.Errorf("endpoint %d is not in the halo of the posting that leads to it", slot)
		}
		dist, err := estimator.Distance(code)
		if err != nil {
			return errors.Wrapf(err, "estimate distance to doc %d", slot)
		}
		dists[i] = dist
	}
	return nil
}

// growHaloWholeValue is a halo written whole: the mark, and the entries of
// the given endpoints after it.
func growHaloWholeValue(endpoints []uint32, codes []uint64, words int) []byte {
	entry := 4 + 8*words
	value := make([]byte, entry, entry*(1+len(endpoints)))
	binary.LittleEndian.PutUint32(value, growHaloWhole)
	return append(value, growHaloEntries(endpoints, codes, words)...)
}

// growHaloEntries is the entries of the given endpoints as one value of a
// row: codes has the code of each, one after another.
func growHaloEntries(endpoints []uint32, codes []uint64, words int) []byte {
	entry := 4 + 8*words
	value := make([]byte, entry*len(endpoints))
	for i, endpoint := range endpoints {
		at := i * entry
		binary.LittleEndian.PutUint32(value[at:], endpoint)
		for w, word := range codes[i*words : (i+1)*words] {
			binary.LittleEndian.PutUint64(value[at+4+8*w:], word)
		}
	}
	return value
}

// haloed tells whether the postings of the index carry their halo.
func (g *growIndex) haloed() bool { return g.meta.Halo }

// fetchHalo reads the halo of a posting as the store holds it.
func (g *growIndex) fetchHalo(p uint32, halo *growHalo) (bytes int, err error) {
	values, err := g.postings.SetRawList(diskKey(p, diskBlockHalo))
	if err != nil {
		return 0, errors.Wrapf(err, "read the halo of posting %d", p)
	}
	if err := halo.decode(values, g.meta.RoutingCodeWords); err != nil {
		return 0, errors.Wrapf(err, "the halo of posting %d", p)
	}
	for _, value := range values {
		bytes += len(value)
	}
	return bytes, nil
}

// routingCode returns the code the index keeps for a doc id: the one in
// memory, or, without them, the one in its bucket, with the bytes it read
// for it. It counts nothing, so that the searches of several inserts can call
// it at once.
func (g *growIndex) routingCode(docID uint64) (code []uint64, read int, err error) {
	if !g.noCodes {
		return g.st.side.code(uint32(docID)), 0, nil
	}
	value, err := g.routing.Get(growDocKey(docID))
	if err != nil {
		return nil, 0, errors.Wrapf(err, "read the routing code of doc %d", docID)
	}
	words := g.meta.RoutingCodeWords
	if len(value) != 8*words {
		return nil, 0, errors.Errorf("the routing code of doc %d takes %d bytes, expected %d", docID, len(value), 8*words)
	}
	code = make([]uint64, words)
	for w := range code {
		code[w] = binary.LittleEndian.Uint64(value[8*w:])
	}
	return code, len(value), nil
}

// haloOwn adds to the halo of posting p the codes of the endpoints of the
// list a plan gives its vector, which are among its neighbors.
func (g *growIndex) haloOwn(plan *growPlan, p uint32, edges []uint32) error {
	if !g.haloed() || len(edges) == 0 {
		return nil
	}
	held, err := g.heldHalo(plan, p)
	if err != nil {
		return err
	}
	words := g.meta.RoutingCodeWords
	return g.addHalo(p, held, edges, func(i int) ([]uint64, error) { return plan.codeOf(uint64(edges[i]), words) })
}

// addHalo adds to the halo of posting q the entries of the given endpoints
// that held does not have already, held being that halo as far as the caller
// knows it, nil if it does not. code gives the code of each.
func (g *growIndex) addHalo(q uint32, held *growHalo, endpoints []uint32, code func(i int) ([]uint64, error)) error {
	if !g.haloed() {
		return nil
	}
	words := g.meta.RoutingCodeWords
	var (
		missing []uint32
		codes   []uint64
	)
	for i, endpoint := range endpoints {
		c, err := code(i)
		if err != nil {
			return err
		}
		if len(c) != words {
			return errors.Errorf("the routing code of doc %d has %d words, expected %d", endpoint, len(c), words)
		}
		if held != nil && held.has(endpoint, c) {
			continue
		}
		missing, codes = append(missing, endpoint), append(codes, c...)
		if held != nil {
			held.add(endpoint, c)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	// A value per entry: of two values that are the same, the compaction of
	// the store keeps one.
	values := make([][]byte, len(missing))
	bytes := 0
	for i := range missing {
		values[i] = growHaloEntries(missing[i:i+1], codes[i*words:(i+1)*words], words)
		bytes += len(values[i])
	}
	if err := g.postings.SetAdd(diskKey(q, diskBlockHalo), values); err != nil {
		return errors.Wrapf(err, "write the halo of posting %d", q)
	}
	g.stats.HaloAdded.add(len(missing), bytes)
	g.haloAdded[q] += uint32(len(missing))
	return nil
}

// trimHalo writes the halo of posting p whole again if what was added to it
// since it last was is more than half of what the given lists, which are all
// those of its members, need: the entries of the endpoints they lead to, each
// once, with the code that counts for it. Every value the row holds is taken
// out first. It is called where the row of edges of the posting is written,
// which is where all its lists are at hand.
func (g *growIndex) trimHalo(p uint32, lists [][]uint32) error {
	if !g.haloed() || g.noHaloTrim || g.haloAdded[p] < growHaloTrimFloor {
		return nil
	}
	seen := make(map[uint32]struct{})
	var endpoints []uint32
	for _, list := range lists {
		for _, endpoint := range list {
			if _, found := seen[endpoint]; !found {
				seen[endpoint] = struct{}{}
				endpoints = append(endpoints, endpoint)
			}
		}
	}
	if 2*int(g.haloAdded[p]) <= len(endpoints) {
		return nil
	}
	key := diskKey(p, diskBlockHalo)
	values, err := g.postings.SetRawList(key)
	if err != nil {
		return errors.Wrapf(err, "read the halo of posting %d", p)
	}
	words := g.meta.RoutingCodeWords
	var halo growHalo
	if err := halo.decode(values, words); err != nil {
		return errors.Wrapf(err, "the halo of posting %d", p)
	}
	codes := make([]uint64, 0, len(endpoints)*words)
	for _, endpoint := range endpoints {
		code := halo.find(endpoint)
		if code == nil {
			return errors.Errorf("STOP: doc %d, where an edge of posting %d leads, is not in its halo", endpoint, p)
		}
		codes = append(codes, code...)
	}
	read := 0
	for _, value := range values {
		read += len(value)
		if err := g.postings.SetDeleteSingle(key, slices.Clone(value)); err != nil {
			return errors.Wrapf(err, "take a value out of the halo of posting %d", p)
		}
	}
	g.stats.HaloTrimmed.add(len(values), read)
	value := growHaloWholeValue(endpoints, codes, words)
	if err := g.postings.SetAdd(key, [][]byte{value}); err != nil {
		return errors.Wrapf(err, "write the halo of posting %d", p)
	}
	g.stats.HaloRows.add(1, len(value))
	g.stats.HaloTrims++
	// What a plan kept of this halo before this write may hold entries that
	// are no longer in it.
	g.haloAdded[p], g.haloWholeSeq[p] = 0, g.seq
	return nil
}

// writeHalo writes whole the halo of a posting that has none: the entries of
// the endpoints of the given lists, each once, in the order the lists have
// them. code gives the code of an endpoint.
func (g *growIndex) writeHalo(p uint32, lists [][]uint32, code func(endpoint uint32) ([]uint64, error)) error {
	if !g.haloed() {
		return nil
	}
	words := g.meta.RoutingCodeWords
	seen := make(map[uint32]struct{})
	var (
		endpoints []uint32
		codes     []uint64
	)
	for _, list := range lists {
		for _, endpoint := range list {
			if _, found := seen[endpoint]; found {
				continue
			}
			seen[endpoint] = struct{}{}
			c, err := code(endpoint)
			if err != nil {
				return err
			}
			if len(c) != words {
				return errors.Errorf("the routing code of doc %d has %d words, expected %d", endpoint, len(c), words)
			}
			endpoints, codes = append(endpoints, endpoint), append(codes, c...)
		}
	}
	value := growHaloWholeValue(endpoints, codes, words)
	if err := g.postings.SetAdd(diskKey(p, diskBlockHalo), [][]byte{value}); err != nil {
		return errors.Wrapf(err, "write the halo of posting %d", p)
	}
	g.stats.HaloRows.add(1, len(value))
	return nil
}
