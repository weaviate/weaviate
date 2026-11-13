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

package db

import (
	"slices"
	"sort"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/usecases/config"
)

// propsWithKeyDocColumn returns those of props whose filterable bucket in store
// holds a key/doc column now.
func propsWithKeyDocColumn(store *lsmkv.Store, props []string) []string {
	var out []string
	for _, prop := range props {
		if b := store.Bucket(helpers.BucketFromPropNameLSM(prop)); b != nil && b.KeyDocColumn() != nil {
			out = append(out, prop)
		}
	}
	return out
}

// keyDocColumnDrops collects the properties a reindex's bucket swaps leave
// without a key/doc column, so each node warns once per task rather than once per shard.
type keyDocColumnDrops struct {
	mu     sync.Mutex
	shards map[keyDocColumnDropShard]struct{}
	props  map[string]struct{}
}

// keyDocColumnDropShard is keyed by collection too: shard names repeat across
// collections, and a run can span all of them.
type keyDocColumnDropShard struct{ collection, shard string }

// keyDocColumnDropsNamedCollections bounds how many collections the report names.
const keyDocColumnDropsNamedCollections = 5

// record notes that props of collection have no column on shard after the swap.
// A shard recorded again, as by a resumed run, is counted once.
func (d *keyDocColumnDrops) record(collection, shard string, props []string) {
	if len(props) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.shards == nil {
		d.shards, d.props = map[keyDocColumnDropShard]struct{}{}, map[string]struct{}{}
	}
	d.shards[keyDocColumnDropShard{collection, shard}] = struct{}{}
	for _, prop := range props {
		d.props[prop] = struct{}{}
	}
}

// absorb adds what other recorded to d.
func (d *keyDocColumnDrops) absorb(other *keyDocColumnDrops) {
	other.mu.Lock()
	defer other.mu.Unlock()
	props := make([]string, 0, len(other.props))
	for prop := range other.props {
		props = append(props, prop)
	}
	for s := range other.shards {
		d.record(s.collection, s.shard, props)
	}
}

// report warns once if any shard was recorded. Call it once the run's shards
// are done.
func (d *keyDocColumnDrops) report(logger logrus.FieldLogger) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.shards) == 0 {
		return
	}
	props := make([]string, 0, len(d.props))
	for prop := range d.props {
		props = append(props, prop)
	}
	sort.Strings(props)
	seen := map[string]struct{}{}
	for s := range d.shards {
		seen[s.collection] = struct{}{}
	}
	collections := make([]string, 0, len(seen))
	for c := range seen {
		collections = append(collections, c)
	}
	sort.Strings(collections)
	logger.WithFields(logrus.Fields{
		"props":       props,
		"shards":      len(d.shards),
		"collections": collections[:min(len(collections), keyDocColumnDropsNamedCollections)],
	}).Warnf("key/doc columns are absent after this reindex swapped the buckets on %d shard(s) "+
		"of %d collection(s); ContainsAny on these properties returns the same documents "+
		"through the standard fold. A node restart, shard reload or tenant reactivation "+
		"builds a column where the property qualifies for one.",
		len(d.shards), len(collections))
}

// recordKeyDocColumnDrops records the swapped filterable buckets whose property
// is configured for a column: the ingest bucket is opened without one.
//
// TODO aliszka:keydoccolumn restore the column after the swap, as
// rebuildRangeableInMemoryReps does.
func (t *ShardReindexTaskGeneric) recordKeyDocColumnDrops(shard ShardLike, swapped map[string]*lsmkv.Bucket) {
	configured := shard.Index().Config.KeyDocColumnProps
	var props []string
	for prop := range swapped {
		if t.strategy.SourceBucketName(prop) == helpers.BucketFromPropNameLSM(prop) &&
			keyDocColumnConfiguredIn(configured, prop) {
			props = append(props, prop)
		}
	}
	t.keyDocColumnDrops.record(shard.Index().Config.ClassName.String(), shard.Name(), props)
}

// keyDocColumnConfigured reports whether this shard's collection configures a
// key/doc column on propName, by name or by [config.AllProperties]. Nothing is
// enabled by default.
func (s *Shard) keyDocColumnConfigured(propName string) bool {
	return keyDocColumnConfiguredIn(s.index.Config.KeyDocColumnProps, propName)
}

func keyDocColumnConfiguredIn(configured []string, propName string) bool {
	return slices.Contains(configured, config.AllProperties) ||
		namesKeyDocColumnProp(configured, propName)
}
