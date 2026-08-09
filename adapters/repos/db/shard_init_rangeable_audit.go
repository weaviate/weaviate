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
	lsmPath := s.pathLSM()
	for _, prop := range unexplainedEmptyRangeableProps(lsmPath, class) {
		propName := prop.name
		entry := s.index.logger.WithFields(map[string]any{
			"action":     "rangeable_index_audit",
			"collection": s.index.Config.ClassName.String(),
			"shard":      s.name,
			"property":   propName,
		})
		if prop.state == rangeableMigrationPromotionFailed {
			entry.Warnf(
				"shard %q: the schema says property %q has a range index, but the migration that "+
					"built it could not be promoted to its canonical directory, so range filters on "+
					"this shard return no results. The promotion is retried at every startup — see "+
					"the preceding finalize error for what blocks it.",
				s.name, propName)
			continue
		}
		entry.Warnf(
			"shard %q: the schema says property %q has a range index, but this shard holds no "+
				"range-index data and no migration is in progress to build it. Range filters on "+
				"this shard will return no results. Rebuild it with "+
				`PUT /v1/schema/%s/indexes/%s {"rangeable":{"rebuild":true}}`,
			s.name, propName, s.index.Config.ClassName.String(), propName)
	}
}

// unexplainedRangeableProp is one property reported by
// [unexplainedEmptyRangeableProps], carrying the tracker state that got it
// reported so the caller can pick its message without walking
// `.migrations/` again.
type unexplainedRangeableProp struct {
	name  string
	state rangeableMigrationExplanation
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
//   - No migration is still in flight for the property. One that is
//     mid-flight is the explanation — reporting those would fire on every
//     healthy run. A migration whose swap already finished is not an
//     explanation: see [rangeableMigrationState].
func unexplainedEmptyRangeableProps(lsmPath string, class *models.Class) []unexplainedRangeableProp {
	if class == nil {
		return nil
	}
	// An empty shard has an empty rangeable bucket for legitimate
	// reasons. Check once: it does not vary per property.
	if !bucketDirHoldsData(filepath.Join(lsmPath, helpers.ObjectsBucketLSM)) {
		return nil
	}

	var out []unexplainedRangeableProp
	for _, prop := range class.Properties {
		if prop == nil || !inverted.HasRangeableIndex(prop) {
			continue
		}
		bucketDir := filepath.Join(lsmPath, helpers.BucketRangeableFromPropNameLSM(prop.Name))
		if bucketDirHoldsData(bucketDir) {
			continue
		}
		state := rangeableMigrationState(lsmPath, prop.Name)
		if state == rangeableMigrationInFlight {
			continue
		}
		out = append(out, unexplainedRangeableProp{name: prop.Name, state: state})
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

// rangeableMigrationExplanation says what the rangeable migration trackers
// on disk have to say about a property whose range index is empty.
type rangeableMigrationExplanation int

const (
	// noRangeableMigration: no tracker covers the property. Nothing on
	// disk explains the empty index.
	noRangeableMigration rangeableMigrationExplanation = iota
	// rangeableMigrationInFlight: a tracker that has not tidied yet. The
	// index is empty because the migration is still building it.
	rangeableMigrationInFlight
	// rangeableMigrationPromotionFailed: a tidied tracker. Its swap
	// finished, so the finalize pass that runs just before this audit
	// would have promoted its data and removed the tracker. The tracker
	// still being here means that promotion failed.
	rangeableMigrationPromotionFailed
)

// rangeableMigrationState classifies the trackers covering propName. An
// in-flight tracker wins over a failed one: a newer generation still
// running is a live explanation regardless of what an older one left
// behind.
//
// A tidied tracker is deliberately NOT an explanation. It would be one if
// this ran at any other time, but the caller runs right after
// [FinalizeCompletedMigrations], which promotes every tidied generation and
// removes its tracker. Counting the survivors would let the artifact a
// failed promotion keeps for its own retry silence the warning about the
// empty index that same failure caused.
//
// An empty tracker dir explains nothing either. Several paths create the
// dir before deciding there is nothing to do, and a leftover would
// otherwise suppress this warning on every boot from then on.
func rangeableMigrationState(lsmPath, propName string) rangeableMigrationExplanation {
	migrationsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return noRangeableMigration
	}
	families := migrationDirFamiliesForIndexType("rangeable")
	state := noRangeableMigration
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
			trackerDir := filepath.Join(migrationsDir, entry.Name())
			// An unreadable tracker dir reads as empty and so explains
			// nothing, which leaves the caller's damage warning in place.
			// That is the safe direction here: one warning too many is
			// better than one too few.
			if holds, _ := dirHoldsAnyFile(trackerDir); !holds {
				continue
			}
			if !fileExists(filepath.Join(trackerDir, "tidied.mig")) {
				return rangeableMigrationInFlight
			}
			state = rangeableMigrationPromotionFailed
		}
	}
	return state
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
