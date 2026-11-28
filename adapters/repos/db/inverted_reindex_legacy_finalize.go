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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/models"
)

// Releases up to v1.39 left a finished migration's data under its staged
// name until the next shard load moved it, and this release has no record
// for it, so without this the property loads an empty index.
type legacyMigrationLayout struct {
	canonical func(prop string) string
	staged    string
	backup    string
}

var legacyMigrationLayouts = map[MigrationStrategyCode]legacyMigrationLayout{
	StrategyCodeSearchableMapToBlockmax:     {helpers.BucketSearchableFromPropNameLSM, "__blockmax_ingest", "__blockmax_map"},
	StrategyCodeFilterableRoaringsetRefresh: {helpers.BucketFromPropNameLSM, "__roaringset_ingest", "__roaringset_backup"},
	StrategyCodeFilterableToRangeable:       {helpers.BucketRangeableFromPropNameLSM, "__rangeable_ingest", "__rangeable_backup"},
	StrategyCodeSearchableRetokenize:        {helpers.BucketSearchableFromPropNameLSM, "__retokenize_ingest", "__retokenize_backup"},
	StrategyCodeFilterableRetokenize:        {helpers.BucketFromPropNameLSM, "__filt_retokenize_ingest", "__filt_retokenize_backup"},
	StrategyCodeEnableFilterable:            {helpers.BucketFromPropNameLSM, "__enable_filterable_ingest", "__enable_filterable_backup"},
	StrategyCodeEnableSearchable:            {helpers.BucketSearchableFromPropNameLSM, "__enable_searchable_ingest", "__enable_searchable_backup"},
	StrategyCodeRebuildSearchable:           {helpers.BucketSearchableFromPropNameLSM, "__rebuild_searchable_ingest", "__rebuild_searchable_backup"},
}

type legacyTracker struct {
	name  string
	code  MigrationStrategyCode
	gen   int
	props []string
}

type legacyMigrationFinalizer struct {
	buckets  bucketDirs
	trackers bucketDirs
	class    *models.Class
}

// finalizeLegacyMigrations returns the folders the load sweep spares. Any error
// stops the load, since the sweep would delete a staged folder holding the only copy.
func (s *Shard) finalizeLegacyMigrations(class *models.Class) (map[string]bool, error) {
	f := legacyMigrationFinalizer{
		buckets: shardBucketDirs(s.pathLSM()), trackers: shardBucketDirs(filepath.Join(s.pathLSM(), migrationsDir)),
		class: class,
	}
	trackers, err := f.finishedTrackers()
	if err != nil {
		return nil, fmt.Errorf("list migrations a previous version finished: %w", err)
	}
	for _, t := range trackers {
		if err := f.promoteStaged(t); err != nil {
			return nil, fmt.Errorf("finish migration %q: %w", t.name, err)
		}
	}
	keep, finalized := map[string]bool{}, 0
	for _, t := range trackers {
		kept, err := f.resolve(t, keep)
		if err != nil {
			return nil, fmt.Errorf("finish migration %q: %w", t.name, err)
		}
		if !kept {
			finalized++
		}
	}
	if finalized > 0 {
		s.index.logger.WithField("shard", s.ID()).
			Infof("moved %d migration(s) a previous version finished onto their canonical names", finalized)
	}
	return keep, nil
}

// Only tidied.mig marks a migration whose task can have FINISHED: the swap
// writes it before acknowledging. Every other tracker is a migration still
// running, which an upgrade does not support, so it is never touched.
func (f legacyMigrationFinalizer) finishedTrackers() ([]legacyTracker, error) {
	entries, err := os.ReadDir(f.trackers.root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []legacyTracker
	for _, entry := range entries {
		prefix, gen, ok := parseMigrationDirName(entry.Name())
		code, known := f.strategyOf(prefix)
		if !entry.IsDir() || !ok || !known {
			continue
		}
		dir := filepath.Join(f.trackers.root, entry.Name())
		if _, err := os.Stat(filepath.Join(dir, "tidied.mig")); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(filepath.Join(dir, "properties.mig"))
		if err != nil {
			return nil, err
		}
		props := strings.Split(strings.TrimSpace(string(raw)), ",")
		out = append(out, legacyTracker{name: entry.Name(), code: code, gen: gen, props: props})
	}
	return out, nil
}

func (f legacyMigrationFinalizer) strategyOf(prefix string) (MigrationStrategyCode, bool) {
	for code := range legacyMigrationLayouts {
		if prefix == string(code) || strings.HasPrefix(prefix, string(code)+"_") {
			return code, true
		}
	}
	return "", false
}

// promoteStaged ignores the class, so no index flag decides whether data survives.
// A newer run renamed an older run's staged folder away, so one tracker at most finds it.
func (f legacyMigrationFinalizer) promoteStaged(t legacyTracker) error {
	layout := legacyMigrationLayouts[t.code]
	for _, prop := range t.props {
		canonical := layout.canonical(prop)
		staged := canonical + layout.staged + genSuffix(t.gen)
		stagedThere, err := f.buckets.Exists(staged)
		if err != nil {
			return err
		}
		if !stagedThere {
			continue
		}
		canonicalThere, err := f.buckets.Exists(canonical)
		if err != nil {
			return err
		}
		if canonicalThere {
			return fmt.Errorf("property %q holds both %q and %q; keeping both", prop, staged, canonical)
		}
		if err := f.buckets.Promote(staged, canonical); err != nil {
			return err
		}
	}
	return nil
}

// resolve keeps the tracker and folder of an index the class has off, for when the flip arrives.
func (f legacyMigrationFinalizer) resolve(t legacyTracker, keep map[string]bool) (kept bool, err error) {
	layout := legacyMigrationLayouts[t.code]
	for _, prop := range t.props {
		canonical := layout.canonical(prop)
		if err := f.buckets.Discard(canonical+layout.backup+genSuffix(t.gen), "the pre-migration copy"); err != nil {
			return false, err
		}
		there, err := f.buckets.Exists(canonical)
		if err != nil {
			return false, err
		}
		on, err := f.indexOn(t.code, prop)
		if err != nil {
			return false, err
		}
		switch {
		case on && !there:
			return false, fmt.Errorf("property %q has neither its staged folder nor %q", prop, canonical)
		case !on && there:
			keep[canonical], kept = true, true
		}
	}
	if kept {
		return true, nil
	}
	return false, f.trackers.Discard(t.name, "the finished migration's markers")
}

func (f legacyMigrationFinalizer) indexOn(code MigrationStrategyCode, prop string) (bool, error) {
	if f.class == nil {
		return false, fmt.Errorf("property %q: the collection is not in the locally applied schema", prop)
	}
	i := slices.IndexFunc(f.class.Properties, func(p *models.Property) bool { return p != nil && p.Name == prop })
	return i < 0 || !propertyIndexRemoved(migrationCanonicalIndexFlag(code, f.class.Properties[i])), nil
}

func propertyIndexRemoved(propertyIndexSetting *bool) bool {
	return propertyIndexSetting != nil && !*propertyIndexSetting
}

func migrationCanonicalIndexFlag(code MigrationStrategyCode, prop *models.Property) *bool {
	switch code {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeEnableSearchable,
		StrategyCodeRebuildSearchable, StrategyCodeSearchableRetokenize:
		return prop.IndexSearchable
	case StrategyCodeFilterableToRangeable:
		return prop.IndexRangeFilters
	case StrategyCodeFilterableRoaringsetRefresh, StrategyCodeFilterableRetokenize,
		StrategyCodeEnableFilterable:
		return prop.IndexFilterable
	}
	return nil
}

type bucketDirs struct {
	root string
}

func shardBucketDirs(lsmPath string) bucketDirs { return bucketDirs{root: lsmPath} }

func (b bucketDirs) Path(dir, what string) (string, error) {
	if !migrationHandleIsOneElement(dir) {
		return "", fmt.Errorf("refusing to act on %s %q: it does not name a single directory under %q",
			what, dir, b.root)
	}
	return filepath.Join(b.root, dir), nil
}

func (b bucketDirs) Exists(dir string) (bool, error) {
	if dir == "" {
		return false, nil
	}
	path, err := b.Path(dir, "a recorded directory")
	if err != nil {
		return false, err
	}
	there, err := diskio.DirExists(path)
	if err != nil {
		return false, fmt.Errorf("stat migration directory %q: %w", path, err)
	}
	return there, nil
}

func (b bucketDirs) Discard(dir, what string) error {
	if dir == "" {
		return nil
	}
	path, err := b.Path(dir, what)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove %s %q: %w", what, dir, err)
	}
	return nil
}

func (b bucketDirs) Promote(from, to string) error {
	fromPath, err := b.Path(from, "the directory to promote")
	if err != nil {
		return err
	}
	toPath, err := b.Path(to, "the name to promote onto")
	if err != nil {
		return err
	}
	if err := diskio.RenameAndSync(fromPath, toPath); err != nil {
		return fmt.Errorf("promote %q to %q: %w", from, to, err)
	}
	return nil
}

func migrationHandleIsOneElement(h string) bool {
	if h == "" || h == "." || h == ".." || filepath.IsAbs(h) {
		return false
	}
	return !strings.ContainsRune(h, '/') && !strings.ContainsRune(h, os.PathSeparator)
}
