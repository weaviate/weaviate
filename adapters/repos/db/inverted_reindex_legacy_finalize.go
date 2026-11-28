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
	"sort"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/errorcompounder"
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
	name string
	code MigrationStrategyCode
	gen  int
}

type legacyMigrationFinalizer struct {
	buckets  bucketDirs
	trackers bucketDirs
	class    *models.Class
}

// Must run before any bucket opens, since it renames bucket directories.
func (s *Shard) finalizeLegacyMigrations(class *models.Class) {
	trackersDir := filepath.Join(s.pathLSM(), migrationsDir)
	f := legacyMigrationFinalizer{
		buckets: shardBucketDirs(s.pathLSM()), trackers: shardBucketDirs(trackersDir), class: class,
	}
	logger := s.index.logger.WithField("shard", s.ID())

	trackers, err := f.finishedTrackers()
	if err != nil {
		logger.WithField("path", trackersDir).Errorf("list migrations a previous version finished: %v", err)
		return
	}
	finalized := 0
	left := errorcompounder.New()
	for _, t := range trackers {
		if err := f.finalize(t); err != nil {
			left.AddWrapf(err, "%s", t.name)
			continue
		}
		finalized++
	}
	if finalized > 0 {
		logger.Infof("moved %d migration(s) a previous version finished onto their canonical names", finalized)
	}
	if err := left.ToErrorLimited(maxReportedErrors); err != nil {
		logger.Warnf("left %d migration(s) a previous version finished in place: %v", left.Len(), err)
	}
}

// Only tidied.mig marks a migration whose task can have FINISHED: the swap
// writes it before acknowledging. Every other tracker is a migration still
// running, which an upgrade does not support, so it is never touched.
// Highest generation first, so an older tracker the newer swap superseded
// finds its property already promoted and only has its markers removed.
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
		if _, err := os.Stat(filepath.Join(f.trackers.root, entry.Name(), "tidied.mig")); err != nil {
			continue
		}
		out = append(out, legacyTracker{name: entry.Name(), code: code, gen: gen})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].gen > out[j].gen })
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

// The markers go last: until then a crash leaves each property either
// unrenamed or renamed, and the next load resumes from whichever it finds.
func (f legacyMigrationFinalizer) finalize(t legacyTracker) error {
	raw, err := os.ReadFile(filepath.Join(f.trackers.root, t.name, "properties.mig"))
	if err != nil {
		return err
	}
	layout := legacyMigrationLayouts[t.code]
	for _, prop := range strings.Split(strings.TrimSpace(string(raw)), ",") {
		canonical := layout.canonical(prop)
		if err := f.promote(t.code, prop, canonical+layout.staged+genSuffix(t.gen), canonical); err != nil {
			return err
		}
		if err := f.buckets.Discard(canonical+layout.backup+genSuffix(t.gen), "the pre-migration copy"); err != nil {
			return err
		}
	}
	return f.trackers.Discard(t.name, "the finished migration's markers")
}

func (f legacyMigrationFinalizer) promote(code MigrationStrategyCode, prop, staged, canonical string) error {
	// An index the schema still disables means the cluster never committed
	// the migration, and the load-time sweep would delete what is renamed.
	if f.class == nil {
		return fmt.Errorf("property %q: the collection is not in the locally applied schema", prop)
	}
	for _, p := range f.class.Properties {
		if p == nil || p.Name != prop {
			continue
		}
		if flag, field := migrationCanonicalIndexFlag(code, p); propertyIndexRemoved(flag) {
			return fmt.Errorf("property %q: the collection sets %s to false", prop, field)
		}
		break
	}
	stagedThere, err := f.buckets.Exists(staged)
	if err != nil {
		return err
	}
	canonicalThere, err := f.buckets.Exists(canonical)
	if err != nil {
		return err
	}
	switch {
	case stagedThere && canonicalThere:
		return fmt.Errorf("property %q holds both %q and %q; keeping both", prop, staged, canonical)
	case stagedThere:
		return f.buckets.Promote(staged, canonical)
	case !canonicalThere:
		return fmt.Errorf("property %q has neither %q nor %q", prop, staged, canonical)
	}
	return nil
}

func propertyIndexRemoved(propertyIndexSetting *bool) bool {
	return propertyIndexSetting != nil && !*propertyIndexSetting
}

func migrationCanonicalIndexFlag(code MigrationStrategyCode, prop *models.Property) (*bool, string) {
	switch code {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeEnableSearchable,
		StrategyCodeRebuildSearchable, StrategyCodeSearchableRetokenize:
		return prop.IndexSearchable, "indexSearchable"
	case StrategyCodeFilterableToRangeable:
		return prop.IndexRangeFilters, "indexRangeFilters"
	case StrategyCodeFilterableRoaringsetRefresh, StrategyCodeFilterableRetokenize,
		StrategyCodeEnableFilterable:
		return prop.IndexFilterable, "indexFilterable"
	}
	return nil, ""
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
