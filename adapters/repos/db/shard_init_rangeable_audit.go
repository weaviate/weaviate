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
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
)

// warnOnUnexplainedEmptyRangeableIndex logs one WARN per property whose
// schema promises a range index this shard cannot serve.
//
// This is the durable signature of weaviate/0-weaviate-issues#464: a
// cluster damaged by the shipped early-flip behavior has
// indexRangeFilters=true in the schema while shards that never finished
// migrating hold an empty bucket. Range filters on those shards return
// zero rows and nothing in the logs says why. The conversion of
// enable-rangeable to a semantic migration makes that state unreachable
// going forward, but it does not repair clusters already in it, so the
// state is worth naming out loud at every startup until an operator runs
// repair-rangeable.
//
// Detection only — nothing is mutated. Startup is the one place the
// question is cheap and unambiguous to answer: the deferred
// ingest→canonical renames have just run, and the buckets are not yet
// serving.
func warnOnUnexplainedEmptyRangeableIndex(s *Shard, class *models.Class) {
	for _, propName := range unexplainedEmptyRangeableProps(s.pathLSM(), class) {
		s.index.logger.WithFields(map[string]any{
			"action":     "rangeable_index_audit",
			"collection": s.index.Config.ClassName.String(),
			"shard":      s.name,
			"property":   propName,
		}).Warnf(
			"shard %q: the schema says property %q has a range index, but this shard holds no "+
				"range-index data and no migration is in progress to build it. Range filters on "+
				"this shard will return no results. Rebuild it with "+
				`PUT /v1/schema/%s/indexes/%s {"rangeable":{"rebuild":true}}`,
			s.name, propName, s.index.Config.ClassName.String(), propName)
	}
}

// unexplainedEmptyRangeableProps returns the properties of class whose
// rangeable index is promised by the schema but absent on disk, with no
// in-flight migration to explain the absence.
//
// Split out from the logging wrapper so the decision can be driven
// directly against a directory layout in tests.
//
// Three conditions must hold together. Any one of them alone is a normal
// state:
//
//   - The schema claims the index. Otherwise there is nothing to promise.
//   - The shard holds objects but the rangeable bucket holds nothing. An
//     empty shard legitimately has an empty index, so the objects check
//     is what separates "damaged" from "new".
//   - No migration tracker exists for the property. A migration that is
//     mid-flight, or whose swap is waiting for the next startup, is the
//     explanation — reporting those would fire on every healthy run.
func unexplainedEmptyRangeableProps(lsmPath string, class *models.Class) []string {
	if class == nil {
		return nil
	}
	// An empty shard has an empty rangeable bucket for legitimate
	// reasons. Check once: it does not vary per property.
	if !bucketDirHoldsData(filepath.Join(lsmPath, helpers.ObjectsBucketLSM)) {
		return nil
	}

	var out []string
	for _, prop := range class.Properties {
		if prop == nil || !inverted.HasRangeableIndex(prop) {
			continue
		}
		bucketDir := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM(prop.Name))
		if bucketDirHoldsData(bucketDir) {
			continue
		}
		if hasAnyMigrationTracker(lsmPath, migrationDirFamiliesForIndexType("rangeable"), prop.Name) {
			continue
		}
		out = append(out, prop.Name)
	}
	return out
}

// bucketDirHoldsData reports whether an LSM bucket directory contains any
// non-empty file. Segments and write-ahead logs both count: a bucket whose
// data is still only in a WAL is populated, it just has not been compacted
// yet. A missing directory holds nothing.
func bucketDirHoldsData(bucketDir string) bool {
	entries, err := os.ReadDir(bucketDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.Size() > 0 {
			return true
		}
	}
	return false
}

// hasAnyMigrationTracker reports whether any generation of a tracker in
// one of the given migration families covers propName, at any stage.
// Unlike [hasUntidiedTracker] this deliberately counts finished trackers
// too: the question here is "is there a migration that explains the
// state", and one whose rename is deferred to the next startup explains
// it just as well as one still running.
//
// An empty tracker dir explains nothing and is not counted. Several
// paths create the dir before deciding there is nothing to do, and a
// leftover would otherwise suppress this warning on every boot from
// then on.
func hasAnyMigrationTracker(lsmPath string, families []string, propName string) bool {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		base, _, ok := parseMigrationDirName(entry.Name())
		if !ok {
			continue
		}
		for _, family := range families {
			if !trackerCoversProp(base, family, propName) {
				continue
			}
			// An unreadable tracker dir reads as empty and so explains
			// nothing, which leaves the caller's damage warning in place.
			// That is the safe direction here: one warning too many is
			// better than one too few.
			if holds, _ := dirHoldsAnyFile(filepath.Join(migrationsDir, entry.Name())); holds {
				return true
			}
		}
	}
	return false
}

// trackerCoversProp reports whether a tracker dir base name of the form
// `<family>_<prop>[_<prop>…]` covers propName. A migration submitted for
// several properties at once produces one tracker naming all of them, so
// matching the single-property name exactly would miss it and report the
// shard as damaged when a live migration explains it.
//
// Property names may themselves contain underscores, so a name that is
// the concatenation of two other property names can still alias. That
// costs a suppressed warning, never a wrong one.
func trackerCoversProp(base, family, propName string) bool {
	if !strings.HasPrefix(base, family+"_") {
		return false
	}
	props := strings.TrimPrefix(base, family+"_")
	return props == propName ||
		strings.HasPrefix(props, propName+"_") ||
		strings.HasSuffix(props, "_"+propName) ||
		strings.Contains(props, "_"+propName+"_")
}
