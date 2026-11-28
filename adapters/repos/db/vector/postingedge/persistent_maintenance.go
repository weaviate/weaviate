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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// What the index a shard opens does in the background: one goroutine of its
// own, which does one thing at a time when it is told there may be work, and
// ends when the index is shut down or dropped.
//
// It is a goroutine of the index and not a queue of the scheduler of the
// node, which is where this work belongs once the index shares the workers
// of the node (integration-plan.md, section 7): a provisional choice.
//
// What it does takes the locks of the index as its other callers do. A pass
// over the vectors reads next to the searches and the inserts, a few vectors
// at a time, and only its end keeps everybody else out.

// layoutStep is how many vectors a pass over the vectors copies between two
// looks at whether anybody waits to write.
const layoutStep = 256

// maintenanceFlushEvery is how many postings or vectors a pass that writes to
// the store goes through between two flushes of the index.
const maintenanceFlushEvery = 256

// startMaintenance starts the goroutine. With noMaintenance there is none,
// and whoever set it calls the work itself.
func (p *Persistent) startMaintenance() {
	p.wake = make(chan struct{}, 1)
	if p.cfg.noMaintenance || p.cfg.noBackground {
		p.stopMaintenance = func() {}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	p.stopMaintenance = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	enterrors.GoWrapper(func() {
		defer close(done)
		// It looks by itself too, now and then: whether the store has left
		// the bucket of postings as one segment is not something anybody
		// tells it (persistent_merge.go).
		tick := time.NewTicker(mergeCheckEvery)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
			case <-tick.C:
			}
			if err := p.maintain(ctx, false); err != nil && ctx.Err() == nil {
				p.logger.WithFields(logrus.Fields{
					"action": "postingedge_maintenance",
					"id":     p.cfg.ID,
				}).Errorf("the maintenance of the index stopped before it ended: %v", err)
			}
		}
	}, p.logger)
	// Whatever was due when the index was last open.
	p.wakeMaintenance()
}

// wakeMaintenance tells the goroutine there may be work. It never waits.
func (p *Persistent) wakeMaintenance() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// errLayoutNoRoom is a pass that lays the vectors out and did not start: the
// disk has no room for what it writes.
var errLayoutNoRoom = errors.New("the disk has no room to lay the vectors out again")

// layoutRoom is how many bytes the disk must have free for a pass that
// writes the given ones to start: its files, and a quarter more for what the
// inserts and the store write while it runs. The files the index has stay
// until the pass ends, so the vectors are on the disk twice by then.
func layoutRoom(writes uint64) uint64 {
	return writes + writes/4
}

// layoutDue tells whether a pass that lays the vectors out is due. An index
// at rest lays them out when it has doubled since the last pass. One that is
// being ingested into, with an insert in the last layoutQuiet, waits until
// it holds four times as many: a pass writes every vector again, and
// competes with the inserts for the disk. atRest asks for the rule of an
// index at rest whatever came last, for whoever settles the index. The
// caller holds mu, for reading at least.
func (p *Persistent) layoutDue(atRest bool) bool {
	if p.g == nil {
		return false
	}
	if atRest || !p.ingesting() {
		return p.g.layoutDue()
	}
	return p.g.layoutDueBy(layoutIngestFactor)
}

// ingesting tells whether an insert was applied in the last layoutQuiet.
func (p *Persistent) ingesting() bool {
	last := p.lastInsert.Load()
	if last == 0 || p.cfg.layoutQuiet < 0 {
		return false
	}
	return p.cfg.now().Sub(time.Unix(0, last)) < p.cfg.layoutQuiet
}

// Settle does whatever maintenance is due now and returns once nothing is:
// for whoever needs the index to stay as it is for a while, which what runs in
// the background does not promise. It runs in the goroutine of the caller, one
// pass at a time with the one of the index. Whatever the index is given next
// may make more work due.
func (p *Persistent) Settle(ctx context.Context) error {
	for {
		p.mu.RLock()
		due, _ := p.layoutDueLocked(true)
		p.mu.RUnlock()
		if !due && !p.cleanOn() && !p.refreshOn() && !p.baseOn() {
			return nil
		}
		if err := p.maintain(ctx, true); err != nil {
			return err
		}
	}
}

// maintain does what is due, one thing after another: first the deleted
// vectors leave, then the edges that are old are built again, and then the
// vectors are laid out. The bucket of postings is asked to be one segment
// when that is due, which the store then sees to.
//
// atRest has the vectors laid out by the rule of an index at rest
// (layoutDue), for whoever settles it: what runs in the background goes by
// whether the index is being ingested into.
func (p *Persistent) maintain(ctx context.Context, atRest bool) error {
	// The base first, and the vectors that came while it was built: until
	// the index has taken them every search reads them all.
	if err := p.settleBase(ctx); err != nil {
		return err
	}
	p.mergeRows()
	if p.cleanOn() {
		if err := p.cleanPasses(ctx, false); err != nil {
			return err
		}
	}
	if p.refreshOn() {
		if err := p.refresh(ctx); err != nil {
			return err
		}
	}

	p.mu.RLock()
	due, slots := p.layoutDueLocked(atRest)
	p.mu.RUnlock()
	if !due {
		return nil
	}
	if err := p.layOut(ctx); err != nil {
		// A pass writes every vector again: one that failed is not tried
		// again with every insert, but once the index has grown by a tenth
		// more.
		p.layoutNotBefore.Store(slots + max(slots/10, layoutStep))
		if errors.Is(err, errLayoutNoRoom) {
			// Nothing of the index is wrong: its vectors stay in the order
			// they are in, and whoever settles it is not told of an error.
			p.logger.WithFields(logrus.Fields{
				"action": "postingedge_layout",
				"id":     p.cfg.ID,
			}).Warnf("the vectors of the index stay in the order they are in: %v", err)
			return nil
		}
		return err
	}
	return nil
}

// layoutDueLocked tells whether a pass over the vectors is due, by the rule
// of an index at rest or by whether it is being ingested into (layoutDue),
// and how many slots the file of vectors has then. None is while a backup
// holds the index. The caller holds mu.
func (p *Persistent) layoutDueLocked(atRest bool) (bool, int64) {
	if p.closed || p.failed != nil || p.g == nil || p.backupHeld || !p.layoutDue(atRest) ||
		int64(p.g.nextSlot) < p.layoutNotBefore.Load() {
		return false, 0
	}
	return true, int64(p.g.nextSlot)
}

// layOut writes the exact vectors again in the order of their postings
// (grow_layout.go), next to the searches and the inserts. Its end waits for
// the insert that is running and keeps the searches out while it copies what
// came in since it started and swaps the files. If it fails, or the index is
// shut down first, the index keeps the files it had.
func (p *Persistent) layOut(ctx context.Context) error {
	// One pass at a time: two would write the files of the same generation.
	p.passMu.Lock()
	defer p.passMu.Unlock()
	p.mu.RLock()
	if p.closed || p.failed != nil || p.g == nil {
		p.mu.RUnlock()
		return nil
	}
	g := p.g
	// A pass writes every vector again next to the files the index has. One
	// that does not fit would fill the disk before it failed, for the
	// inserts and for everything else that writes to it: it does not start.
	writes := g.layoutBytes()
	if free, err := p.cfg.diskFree(g.dir); err == nil && free < layoutRoom(writes) {
		p.mu.RUnlock()
		return errors.Wrapf(errLayoutNoRoom, "%d bytes free in %s, and the pass writes %d", free, g.dir, writes)
	}
	pass, err := g.beginLayout()
	p.mu.RUnlock()
	if err != nil {
		return err
	}
	for step, done := 0, false; !done; step++ {
		if p.cfg.layoutHook != nil {
			p.cfg.layoutHook(step)
		}
		if err := ctx.Err(); err != nil {
			pass.abort()
			return err
		}
		p.mu.RLock()
		if p.closed || p.failed != nil {
			p.mu.RUnlock()
			pass.abort()
			return nil
		}
		if p.backupHeld {
			// A backup links the files of the generation the pass would
			// remove: the pass waits for it to let go.
			resumed := p.resumed
			p.mu.RUnlock()
			select {
			case <-resumed:
				continue
			case <-ctx.Done():
				pass.abort()
				return ctx.Err()
			}
		}
		done, err = pass.step(layoutStep)
		p.mu.RUnlock()
		if err != nil {
			pass.abort()
			return err
		}
	}
	// The bulk of the new file reaches the disk before anybody waits for it.
	if err := pass.settle(); err != nil {
		pass.abort()
		return err
	}

	if err := p.lockOutsideBackup(ctx); err != nil {
		pass.abort()
		return err
	}
	defer p.writeMu.Unlock()
	defer p.mu.Unlock()
	if p.closed || p.failed != nil || ctx.Err() != nil {
		pass.abort()
		return ctx.Err()
	}
	if err := pass.finish(); err != nil {
		return err
	}
	p.layouts.Add(1)
	p.logger.WithFields(logrus.Fields{
		"action":     "postingedge_layout",
		"id":         p.cfg.ID,
		"vectors":    g.nextSlot,
		"generation": g.meta.VectorGen,
	}).Info("wrote the vectors of the index again in the order of their postings")
	return nil
}
