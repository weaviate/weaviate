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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// dropOrphans asks the shard, for the vectors whose objects may not have
// reached the store, whether their objects are there, and marks deleted those
// whose objects are not. Such a vector has no object to give a search, and
// its doc id may be one the shard hands out again. They are:
//   - the vectors the journal put back when the index opened: a transfer
//     copies the journal after the store was flushed and its counter read,
//     with what was written in between, and a process may die after the
//     index took a vector and before the store writes the log of its object;
//   - the vectors kept aside, which no journal tells: a transfer flushes the
//     buckets of the store one at a time while the shard writes, and the
//     copy may hold the vector of an object that came after the bucket of
//     the objects was flushed. They are fewer than the base size.
//
// The marked vectors are taken out of the index before it opens, so that the
// shard can give their doc ids to new objects: that is one pass over every
// vector marked deleted, the rare open that drops one pays it. Without
// PersistentConfig.ObjectExists it asks nothing.
//
// The doc ids the journal put back wait in growCheckFile until the answer is
// on disk: an open that dies before asking leaves them to the next one.
func (p *Persistent) dropOrphans(ctx context.Context) error {
	if p.cfg.ObjectExists == nil {
		return nil
	}
	start := time.Now()
	var placed []uint64
	if p.g != nil {
		placed = p.g.recovered.Placed
	}
	staged := make([]uint64, 0, len(p.staged))
	for id := range p.staged {
		staged = append(staged, id)
	}
	slices.Sort(staged)
	checked := 0
	var orphans []uint64
	ask := func(ids []uint64, held func(id uint64) bool) error {
		for _, id := range ids {
			if !held(id) {
				continue
			}
			checked++
			exists, err := p.cfg.ObjectExists(ctx, id)
			if err != nil {
				return errors.Wrapf(err, "look for the object of doc %d", id)
			}
			if !exists {
				orphans = append(orphans, id)
			}
		}
		return nil
	}
	// One the journal put back and then took out, or that is marked deleted
	// already, leaves anyway.
	if err := ask(placed, p.g.holds); err != nil {
		return err
	}
	if err := ask(staged, func(uint64) bool { return true }); err != nil {
		return err
	}
	fields := logrus.Fields{
		"action": "postingedge_drop_orphans", "id": p.cfg.ID,
		"journaled": len(placed), "keptAside": len(staged), "checked": checked, "dropped": len(orphans),
	}
	if len(orphans) > 0 {
		p.writeMu.Lock()
		_, err := p.mark(orphans)
		p.writeMu.Unlock()
		if err != nil {
			return errors.Wrap(err, "mark deleted the vectors whose objects are not in the store")
		}
		// A vector kept aside leaves as it is marked. One in a posting
		// leaves with the pass, and an index whose edges are a value per
		// list, which is not cleaned, keeps it marked.
		if p.g != nil && p.g.consolidated() {
			if err := p.clean(ctx); err != nil {
				return errors.Wrap(err, "take out the vectors whose objects are not in the store")
			}
		}
		if err := p.Flush(); err != nil {
			return err
		}
		fields["took"] = time.Since(start).String()
		p.logger.WithFields(fields).Warnf("dropped %d of the %d vectors whose objects may not have reached the store: "+
			"their objects are not there", len(orphans), checked)
	} else if checked > 0 {
		p.logger.WithFields(fields).Infof("the %d vectors whose objects may not have reached the store have them", checked)
	}
	if p.g == nil {
		return nil
	}
	p.g.recovered.Placed = nil
	path := filepath.Join(p.g.dir, growCheckFile)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "remove %s", path)
	}
	return nil
}

// writeObjectsLog has the shard hand the log of its store of objects to the
// system (PersistentConfig.WriteObjectsLog), if it gave a way.
func (p *Persistent) writeObjectsLog() error {
	if p.cfg.WriteObjectsLog == nil {
		return nil
	}
	return errors.Wrap(p.cfg.WriteObjectsLog(), "write the log of the objects")
}
