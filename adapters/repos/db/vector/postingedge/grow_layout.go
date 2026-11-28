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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// The exact vectors of an index that grows, by slot, and laying them out
// again in the order of their postings (bench-report.md, "control de
// reorganización" and "v5.3"): a rescore reads the vectors of the best
// candidates, which are in a few postings, and with the vectors of a posting
// in a row those reads fall close together on disk. The experiments measured
// it on a copy of the index written once and never grown again
// (reorganizeGrowIndex). Here the index that grows does it to itself, while it
// is searched and written to.
//
// The vector of a doc id is in the slot its table gives it. A vector that
// comes in takes the next slot, at the end of the file, and writes its entry
// of the table. Laying out writes every vector again to the file of the next
// generation, those of every posting in a row, with a table of its own, and
// then the meta names that generation as the current one: until it does, the
// index is the old two files, whole. The files of any other generation are
// removed when the index opens.
//
// A pass reads the index as a search does, a few vectors at a time, so
// searches and inserts run next to it, and ends with nobody else in: it
// copies what came in meanwhile, and swaps the files.

// growVectorsName is the name of the file of exact vectors of a generation:
// that of always for an index whose vectors are at their doc ids.
func growVectorsName(gen int) string {
	if gen == 0 {
		return diskVectorFile
	}
	return fmt.Sprintf("vectors-%d.f32", gen)
}

func growSlotsName(gen int) string {
	return fmt.Sprintf("slots-%d.u32", gen)
}

// writeSlots writes a table of slots whole: for every doc id its slot plus
// one, and 0 where it has none, which is what a part of the file that was
// never written reads as.
func writeSlots(path string, slotOf []uint32) error {
	raw := make([]byte, 4*len(slotOf))
	for docID, slot := range slotOf {
		if slot != diskNoSlot {
			binary.LittleEndian.PutUint32(raw[4*docID:], slot+1)
		}
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// openSlots reads the table of slots of the current generation and opens it
// to be written, and removes the files of every other generation. No two doc
// ids share a slot, and the file of vectors reaches the last one.
func (g *growIndex) openSlots() error {
	gen, keys := g.meta.VectorGen, g.meta.Keys
	path := filepath.Join(g.dir, growSlotsName(gen))
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "read where the vectors are")
	}
	slotOf := make([]uint32, keys)
	for i := range slotOf {
		slotOf[i] = diskNoSlot
	}
	next := uint32(0)
	// An entry is written whole or not at all, but for the last one of a
	// file that was being written to when the machine stopped.
	for docID := range len(raw) / 4 {
		entry := binary.LittleEndian.Uint32(raw[4*docID:])
		if entry == 0 {
			continue
		}
		if docID >= keys {
			return errors.Errorf("the vector of doc %d has a slot, and the index has room for %d doc ids", docID, keys)
		}
		slotOf[docID] = entry - 1
		next = max(next, entry)
	}
	taken := make([]bool, next)
	for docID, slot := range slotOf {
		if slot == diskNoSlot {
			continue
		}
		if taken[slot] {
			return errors.Errorf("doc %d has the slot %d of its vector, and so does another doc", docID, slot)
		}
		taken[slot] = true
	}
	info, err := g.vectors.Stat()
	if err != nil {
		return errors.Wrap(err, "look at the file of vectors")
	}
	if need := int64(next) * int64(4*g.meta.Dims); info.Size() < need {
		return errors.Errorf("the file of vectors takes %d bytes, and its %d slots take %d", info.Size(), next, need)
	}
	if g.slots, err = os.OpenFile(path, os.O_RDWR, 0o644); err != nil {
		return errors.Wrap(err, "open where the vectors are")
	}
	g.slotOf, g.nextSlot = slotOf, next
	return g.removeOtherGenerations()
}

// removeOtherGenerations removes the files of vectors and of slots of every
// generation but the current one: those of a pass that did not end, and the
// ones a pass that ended could not remove.
func (g *growIndex) removeOtherGenerations() error {
	entries, err := os.ReadDir(g.dir)
	if err != nil {
		return errors.Wrap(err, "read the directory of the index")
	}
	keep := map[string]bool{growVectorsName(g.meta.VectorGen): true, growSlotsName(g.meta.VectorGen): true}
	for _, entry := range entries {
		name := entry.Name()
		vectors := strings.HasPrefix(name, "vectors-") && strings.HasSuffix(name, ".f32")
		slots := strings.HasPrefix(name, "slots-") && strings.HasSuffix(name, ".u32")
		if (!vectors && !slots) || keep[name] {
			continue
		}
		if err := os.Remove(filepath.Join(g.dir, name)); err != nil {
			return errors.Wrapf(err, "remove %s, of another generation of the vectors", name)
		}
	}
	return nil
}

// takeSlot gives the vector of a doc id, which was just written at the next
// slot, that slot: in its table, on disk and in memory.
func (g *growIndex) takeSlot(docID uint64) error {
	if g.nextSlot >= diskNoSlot-1 {
		return errors.Errorf("the file of vectors has no slot left after %d", g.nextSlot)
	}
	var entry [4]byte
	binary.LittleEndian.PutUint32(entry[:], g.nextSlot+1)
	if err := g.writeFile(growTargetSlots, entry[:], int64(docID)*4); err != nil {
		return errors.Wrapf(err, "write where the vector of doc %d is", docID)
	}
	g.slotOf[docID] = g.nextSlot
	g.nextSlot++
	return nil
}

// layoutDue tells whether the vectors that came in since they were last laid
// out are as many as those that were laid out then: the index has doubled.
// The rule is provisional (integration-plan.md, section 7).
func (g *growIndex) layoutDue() bool {
	return g.layoutDueBy(1)
}

// layoutDueBy tells whether the vectors that came in since they were last
// laid out are factor times those that were laid out then, or more: with 1
// the index has doubled, with 3 it holds four times as many.
func (g *growIndex) layoutDueBy(factor int) bool {
	if g.meta.VectorGen == 0 {
		return false
	}
	return int(g.nextSlot)-g.meta.VectorsLaidOut >= factor*max(g.meta.VectorsLaidOut, 1)
}

// layoutBytes is what a pass that lays the vectors out writes: the exact
// vector of every doc id the index holds, and the table of their slots.
func (g *growIndex) layoutBytes() uint64 {
	return uint64(g.meta.Live)*uint64(4*g.meta.Dims) + 4*uint64(len(g.live))
}

// growLayout is a pass that writes the exact vectors of an index again, those
// of every posting in a row.
type growLayout struct {
	g *growIndex
	// gen is the generation it writes, and vectors its file.
	gen     int
	vectors *os.File
	// order has the doc ids the index held when the pass started, posting
	// after posting, and at how many of them it has copied.
	order []uint32
	at    int
	// slotOf is the table of the generation it writes, and from the slot of
	// the current generation every vector was copied from: a doc id whose
	// vector is somewhere else by the end is not the vector that was copied.
	slotOf []uint32
	from   []uint32
	next   uint32
	raw    []byte
}

// beginLayout starts a pass: the order the vectors are written in is that of
// the postings as they are now. The caller holds what a search holds.
func (g *growIndex) beginLayout() (*growLayout, error) {
	if g.meta.VectorGen == 0 {
		return nil, errors.New("the vectors of the index are at their doc ids: they are not laid out again")
	}
	side := g.st.side
	starts := make([]int, g.meta.Postings+1)
	for docID, held := range g.live {
		if held {
			starts[side.postingOf[docID]+1]++
		}
	}
	for p := range g.meta.Postings {
		starts[p+1] += starts[p]
	}
	order := make([]uint32, starts[g.meta.Postings])
	for docID, held := range g.live {
		if held {
			p := side.postingOf[docID]
			order[starts[p]] = uint32(docID)
			starts[p]++
		}
	}

	l := &growLayout{g: g, gen: g.meta.VectorGen + 1, order: order, raw: make([]byte, 4*g.meta.Dims)}
	// Whatever a pass that did not end left under the names of this one.
	for _, name := range []string{growVectorsName(l.gen), growSlotsName(l.gen)} {
		if err := os.Remove(filepath.Join(g.dir, name)); err != nil && !os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "remove %s", name)
		}
	}
	var err error
	l.vectors, err = os.OpenFile(filepath.Join(g.dir, growVectorsName(l.gen)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "create the file of vectors")
	}
	l.room(len(g.live))
	return l, nil
}

// room gives the tables of the pass room for the doc ids the index has now.
func (l *growLayout) room(keys int) {
	for len(l.slotOf) < keys {
		l.slotOf, l.from = append(l.slotOf, diskNoSlot), append(l.from, diskNoSlot)
	}
}

// copy writes the vector of a doc id at the next slot of the new file.
func (l *growLayout) copy(docID uint32) error {
	g := l.g
	slot := g.slotOf[docID]
	if slot == diskNoSlot {
		return errors.Errorf("doc %d has no vector in the file", docID)
	}
	size := int64(len(l.raw))
	if _, err := g.vectors.ReadAt(l.raw, int64(slot)*size); err != nil {
		return errors.Wrapf(err, "read the vector of doc %d", docID)
	}
	if _, err := l.vectors.WriteAt(l.raw, int64(l.next)*size); err != nil {
		return errors.Wrapf(err, "write the vector of doc %d", docID)
	}
	l.slotOf[docID], l.from[docID] = l.next, slot
	l.next++
	return nil
}

// step copies up to the given number of vectors more, and tells whether all
// those the pass started with are copied. The caller holds what a search
// holds: inserts and searches run between two steps.
func (l *growLayout) step(vectors int) (bool, error) {
	g := l.g
	l.room(len(g.live))
	for ; vectors > 0 && l.at < len(l.order); l.at++ {
		docID := l.order[l.at]
		if !g.live[docID] {
			// It left the index since the pass started.
			continue
		}
		if err := l.copy(docID); err != nil {
			return false, err
		}
		vectors--
	}
	return l.at >= len(l.order), nil
}

// settle has what the pass wrote so far reach the disk. It holds nothing of
// the index, so that the wait is not that of a search.
func (l *growLayout) settle() error {
	return errors.Wrap(l.vectors.Sync(), "sync the file of vectors")
}

// finish ends the pass: it copies the vectors that came in since it started,
// in the order of their doc ids, and makes its two files the ones of the
// index. Nobody else reads or writes the index meanwhile. If it fails the
// index is the one it was, and what the pass wrote is removed.
func (l *growLayout) finish() (err error) {
	g := l.g
	defer func() {
		if err != nil {
			l.abort()
		}
	}()
	for done := false; !done; {
		if done, err = l.step(len(l.order)); err != nil {
			return err
		}
	}
	// Those in the order of their postings end here.
	laidOut := l.next
	for docID, held := range g.live {
		// A vector that is not where the pass read it from is another one:
		// its doc id was given again since.
		if held && (l.slotOf[docID] == diskNoSlot || l.from[docID] != g.slotOf[docID]) {
			if err := l.copy(uint32(docID)); err != nil {
				return err
			}
		}
	}
	for docID, held := range g.live {
		if !held {
			l.slotOf[docID] = diskNoSlot
		}
	}
	if err := l.settle(); err != nil {
		return err
	}
	slotsPath := filepath.Join(g.dir, growSlotsName(l.gen))
	if err := writeSlots(slotsPath, l.slotOf); err != nil {
		return errors.Wrap(err, "write where the vectors are")
	}
	slots, err := os.OpenFile(slotsPath, os.O_RDWR, 0o644)
	if err != nil {
		return errors.Wrap(err, "open where the vectors are")
	}

	// The journal tells of the files of the generation that ends: it is
	// emptied before another takes over, with everything it tells handed to
	// the system.
	if err := g.sync(); err != nil {
		_ = slots.Close()
		return err
	}
	// The meta is what says which generation is the index's.
	oldGen, oldLaidOut := g.meta.VectorGen, g.meta.VectorsLaidOut
	g.meta.VectorGen, g.meta.VectorsLaidOut = l.gen, int(laidOut)
	if err := g.writeMeta(); err != nil {
		g.meta.VectorGen, g.meta.VectorsLaidOut = oldGen, oldLaidOut
		_ = slots.Close()
		return err
	}
	oldVectors, oldSlots := g.vectors, g.slots
	g.vectors, g.slots, g.slotOf, g.nextSlot = l.vectors, slots, l.slotOf, l.next
	// The file is the index's now, and no longer the pass's to remove.
	l.vectors = nil
	g.stats.Layouts++
	g.stats.LayoutVectors += int64(l.next)
	// The index is the new files from here on. The old ones are removed now,
	// or when it opens next.
	for _, file := range []*os.File{oldVectors, oldSlots} {
		if closed := file.Close(); closed != nil {
			g.logger.Warnf("close a file of generation %d of the vectors: %v", oldGen, closed)
		}
	}
	for _, name := range []string{growVectorsName(oldGen), growSlotsName(oldGen)} {
		if removed := os.Remove(filepath.Join(g.dir, name)); removed != nil {
			g.logger.Warnf("remove %s: %v", name, removed)
		}
	}
	return nil
}

// abort ends a pass that will not finish: what it wrote is removed.
func (l *growLayout) abort() {
	if l.vectors == nil {
		return
	}
	_ = l.vectors.Close()
	l.vectors = nil
	for _, name := range []string{growVectorsName(l.gen), growSlotsName(l.gen)} {
		if err := os.Remove(filepath.Join(l.g.dir, name)); err != nil && !os.IsNotExist(err) {
			l.g.logger.Warnf("remove %s, of a pass over the vectors that did not end: %v", name, err)
		}
	}
}

// layOut lays the vectors of the index out again at once, with nothing
// running next to it: what the experiments and the tests call.
func (g *growIndex) layOut() error {
	l, err := g.beginLayout()
	if err != nil {
		return err
	}
	return l.finish()
}
