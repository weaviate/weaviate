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
	"os"
	"path/filepath"
	"slices"

	"github.com/pkg/errors"
)

// The base of the index, built in the background.
//
// An index starts with no postings: its first vectors are kept aside, in a
// bucket, and searched exactly. Once there are enough the base is built from
// them, in batch (Persistent.buildBase), which takes seconds with the ten
// thousand vectors of the default and minutes with more. It is not built
// inside the insert that brings the vector that completes it: what runs in
// the background builds it, with none of the locks of the index, and the
// inserts and the searches go on as before meanwhile.
//
//   - A vector that comes while the base is built is kept aside as those
//     before it. When the base becomes the index it stays aside, and the
//     index takes it as it takes any vector, one after another: in the
//     background, and a couple with every insert, so that they are all taken
//     however the inserts come. Until then a search reads them all, exactly,
//     next to what it finds in the index.
//   - A vector that is deleted while the base is built may be in it. A mark
//     takes its place in the bucket, and becomes the mark of a deleted vector
//     of the index when the base does.
//   - The base has a meta only once it is whole. A process that dies before
//     leaves the vectors aside and what there is of a base, which is removed
//     when the index opens. One that dies after leaves in the bucket vectors
//     the base holds, which leave it when the index opens, the marks, which
//     are carried over then, and the vectors that came meanwhile, which the
//     index goes on taking.
//
// Without anything running in the background (noBackground, noMaintenance)
// the base is built by the insert that completes it, as it was.

// goneMark is what the bucket of the vectors kept aside holds in the place
// of a vector that was deleted while a base was built: a value no vector
// has, whose bytes come in fours.
var goneMark = []byte{0}

func isGoneMark(value []byte) bool { return len(value) == len(goneMark) }

// drainPerInsert is how many of the vectors kept aside the index takes with
// every insert, once it has its base.
const drainPerInsert = 2

// baseDue tells whether there are enough vectors aside to build the base,
// and nobody is building it. After a build that failed it is not due until
// more have come. The caller holds mu.
func (p *Persistent) baseDue() bool {
	// No build starts while a backup holds the index: the vectors wait aside.
	return p.g == nil && !p.building && !p.backupHeld && len(p.staged) >= p.cfg.BaseSize &&
		len(p.staged) > p.buildFailed
}

// baseOn tells whether the base is due or being built, or the index has one
// and vectors are still aside.
func (p *Persistent) baseOn() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.failed != nil {
		return false
	}
	return p.building || p.baseDue() || (p.g != nil && len(p.staged) > 0)
}

// baseHook tells the tests where the build of the base is. What they answer
// is the build failing there, which only its first point can do: after it
// the base is on disk.
func (p *Persistent) baseHook(point string) error {
	if p.cfg.baseHook != nil {
		return p.cfg.baseHook(point)
	}
	return nil
}

// settleBase builds the base if it is due, waiting for whoever is building
// it, and has the index take every vector that is still aside. It runs in
// the goroutine of the caller, which holds none of the locks of the index.
func (p *Persistent) settleBase(ctx context.Context) error {
	// Whoever calls with nothing of this to do takes no lock that a writer
	// waits for.
	p.mu.RLock()
	build := p.building || p.baseDue()
	p.mu.RUnlock()
	if build {
		if err := p.buildBase(ctx, false); err != nil {
			return errors.Wrap(err, "build the first postings")
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.mu.RLock()
		aside := p.g != nil && len(p.staged) > 0
		p.mu.RUnlock()
		if !aside {
			return nil
		}
		p.writeMu.Lock()
		more, err := p.drainStep(ctx)
		p.writeMu.Unlock()
		if err != nil || !more {
			return err
		}
	}
}

// drainInLine has an insert pay its share of the vectors that are still
// aside. The caller holds writeMu.
func (p *Persistent) drainInLine(ctx context.Context) error {
	if p.cfg.noMaintenance {
		return nil
	}
	for range drainPerInsert {
		more, err := p.drainStep(ctx)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

// drainStep has the index take one of the vectors that are still aside, the
// one of the lowest doc id, as it takes any vector, and lets it go from
// where it was kept. It tells whether more are left. A process that dies
// between the two leaves a vector the index holds aside, which leaves when
// the index opens. The caller holds writeMu.
func (p *Persistent) drainStep(ctx context.Context) (bool, error) {
	p.mu.RLock()
	if p.closed || p.failed != nil || p.g == nil || len(p.staged) == 0 {
		p.mu.RUnlock()
		return false, nil
	}
	if len(p.aside) == 0 {
		for id := range p.staged {
			p.aside = append(p.aside, id)
		}
		slices.Sort(p.aside)
	}
	id := p.aside[0]
	p.aside = p.aside[1:]
	_, staged := p.staged[id]
	var raw []byte
	var err error
	if staged {
		raw, err = p.staging.Get(growDocKey(id))
	}
	dims := int(p.dims.Load())
	p.mu.RUnlock()
	if !staged {
		// It was deleted since the list was made.
		return true, nil
	}
	if err != nil {
		return false, errors.Wrapf(err, "read the vector kept aside of doc %d", id)
	}
	if len(raw) != 4*dims {
		return false, errors.Errorf("the vector kept aside of doc %d takes %d bytes, for %d dimensions", id, len(raw), dims)
	}
	vector := make([]float32, dims)
	for i := range vector {
		vector[i] = getFloat32(raw[4*i:])
	}
	if err := p.add(context.WithoutCancel(ctx), id, vector); err != nil {
		return false, errors.Wrapf(err, "take in the vector kept aside of doc %d", id)
	}
	// It is an insert, and pays its share of the passes that are on as any
	// other: left to what runs in the background after it, a refresh of the
	// edges would wait for every vector kept aside to be in.
	if err := p.refreshInLine(ctx); err != nil {
		return false, errors.Wrapf(err, "refresh edges after the vector kept aside of doc %d", id)
	}
	if err := p.cleanInLine(ctx, 1); err != nil {
		return false, errors.Wrapf(err, "take the deleted vectors out after the vector kept aside of doc %d", id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.staging.Delete(growDocKey(id)); err != nil {
		return false, errors.Wrapf(err, "forget the vector kept aside of doc %d", id)
	}
	delete(p.staged, id)
	p.dirty = true
	_ = p.baseHook("a vector kept aside is in the index")
	if len(p.staged) > 0 {
		return true, nil
	}
	p.aside = nil
	return false, p.dropStaging(ctx)
}

// settleGone lets go of the marks of the vectors that were deleted while a
// base was built, where there is no such base: the vectors went with their
// marks. The caller holds mu, or is opening the index.
func (p *Persistent) settleGone() error {
	for id := range p.gone {
		if err := p.staging.Delete(growDocKey(id)); err != nil {
			return errors.Wrapf(err, "forget the mark of doc %d", id)
		}
		delete(p.gone, id)
	}
	return nil
}

// reconcileStaged leaves aside only what an index that has its base does not
// hold yet. The vectors that were deleted while the base was built are
// marked deleted in the index, first, and their marks let go; the vectors the
// base holds are let go; and if nothing is left their bucket goes too. It is
// how a base becomes the index, and what an index that opens next to a
// bucket of vectors kept aside does: done twice it changes nothing. The
// caller holds mu for writing, or is opening the index.
func (p *Persistent) reconcileStaged(ctx context.Context) error {
	g := p.g
	if len(p.gone) > 0 {
		err := g.step(func() error {
			for id := range p.gone {
				if _, err := g.remove(id); err != nil {
					return errors.Wrapf(err, "mark doc %d, deleted while the base was built", id)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err := p.settleGone(); err != nil {
			return err
		}
	}
	for id := range p.staged {
		if id >= uint64(len(g.live)) || !g.live[id] {
			continue
		}
		if err := p.staging.Delete(growDocKey(id)); err != nil {
			return errors.Wrapf(err, "forget the vector kept aside of doc %d", id)
		}
		delete(p.staged, id)
	}
	p.aside = nil
	if len(p.staged) == 0 {
		return p.dropStaging(ctx)
	}
	return nil
}

// openLeftovers opens what is kept aside next to an index that has its base,
// if anything is, and leaves of it what the index does not hold yet.
func (p *Persistent) openLeftovers(ctx context.Context) error {
	name := p.stagingName()
	if _, err := os.Stat(filepath.Join(p.store.GetDir(), name)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "look for the bucket %s", name)
	}
	if err := p.openStaging(ctx); err != nil {
		return err
	}
	if err := p.loadStaged(); err != nil {
		_ = p.closeStaging(ctx)
		return err
	}
	if err := p.reconcileStaged(ctx); err != nil {
		_ = p.closeStaging(ctx)
		return err
	}
	return nil
}

// mergeNearest merges two answers, each nearest first, into the k nearest of
// the two.
func mergeNearest(ids []uint64, dists []float32, moreIDs []uint64, moreDists []float32, k int) ([]uint64, []float32) {
	size := min(k, len(ids)+len(moreIDs))
	outIDs, outDists := make([]uint64, 0, size), make([]float32, 0, size)
	i, j := 0, 0
	for len(outIDs) < size {
		if j >= len(moreIDs) || (i < len(ids) && dists[i] <= moreDists[j]) {
			outIDs, outDists = append(outIDs, ids[i]), append(outDists, dists[i])
			i++
			continue
		}
		outIDs, outDists = append(outIDs, moreIDs[j]), append(outDists, moreDists[j])
		j++
	}
	return outIDs, outDists
}
