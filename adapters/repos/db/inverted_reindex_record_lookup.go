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

	"github.com/sirupsen/logrus"
)

// migrationRecordsAt reads one shard's records straight from disk, for sweeps
// and gates deciding about a cold tenant. someRecordsUnreadable scopes
// withholding to the whole shard; recordSetUnreadable is the stronger fault
// (nothing could be read), so a caller that would report clean must fail open.
func migrationRecordsAt(lsmPath string, logger logrus.FieldLogger) (records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) {
	store := NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true, true
	}
	// One line per read, even on the healthy path. One read per shard is the
	// cost every caller is written for, and a probe that reads once per tuple
	// or once per property instead is otherwise invisible until it shows up as
	// startup latency on a many-tenant node.
	logger.WithField("path", store.Dir()).WithField("records", len(store.Records())).
		Debug("read migration records")
	return store.Records(), len(store.Unreadable()) > 0, false
}

// migrationPreservedState names what a sweep of one shard must leave alone: a
// record with complete staged data holds a copy nobody else holds, and
// removing a directory it names loses that copy silently.
type migrationPreservedState struct {
	// records is the whole understood set, not just the preserved part:
	// sweeps also ask an extant record which properties it belongs to.
	records []MigrationRecord
	// buckets maps each preserved bucket directory to whether a load would
	// still do disk work for it, the same question trackers answers.
	buckets map[string]bool
	// trackers maps each preserved migration directory to whether a load
	// would still do disk work for it.
	trackers map[string]bool
	// withholdEverything preserves the whole shard: a record this build
	// cannot read names directories nothing else accounts for.
	withholdEverything bool
	// recordSetUnreadable means no record here could be read at all, so no
	// caller may report the shard clean. It implies withholdEverything.
	recordSetUnreadable bool
}

// migrationPreservedStateAt is the shard-wide preserve state, read off the
// shard's records. The zero value preserves nothing.
func migrationPreservedStateAt(lsmPath string, logger logrus.FieldLogger) migrationPreservedState {
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	return migrationPreservedStateFromRecords(records, someRecordsUnreadable, recordSetUnreadable)
}

func migrationPreservedStateFromRecords(records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) migrationPreservedState {
	state := migrationPreservedState{
		records:             records,
		buckets:             map[string]bool{},
		trackers:            map[string]bool{},
		withholdEverything:  someRecordsUnreadable,
		recordSetUnreadable: recordSetUnreadable,
	}
	for _, rec := range records {
		if !rec.StagedDataComplete() {
			continue
		}
		subject := rec.Subject()
		anyCanAct := false
		for _, prop := range subject.Properties() {
			// Per property, because promotion is: a load promotes every
			// property whose promotion is not lost and skips the ones that
			// are. Folding the properties together claims a load would act on
			// a lost property's directories, or that it would act on none
			// because a sibling's is lost.
			canAct := migrationPropertyLoadCanStillAct(rec, prop)
			anyCanAct = anyCanAct || canAct
			if dir := subject.Props[prop].Staged; dir != "" {
				state.buckets[dir] = canAct
			}
			if dir := subject.Props[prop].Sidecar; dir != "" {
				state.buckets[dir] = canAct
			}
		}
		if subject.TrackerDir != "" {
			// The tracker directory goes when the whole record retires, so one
			// property that can still act keeps it claimed. A promoted
			// record's own directory waits on the schema effect, which no load
			// can force, so it never justifies hydration alone; its owned
			// directories still do, counted from buckets above.
			state.trackers[subject.TrackerDir] = rec.State() != MigrationStatePromoted && anyCanAct
		}
	}
	return state
}

// mirrorFor names the (record, property) whose staged directory is dir. Every
// readable record answers, not just committed ones.
func (s migrationPreservedState) mirrorFor(dir string) (MigrationRecordKey, string, bool) {
	for _, rec := range s.records {
		subject := rec.Subject()
		for _, prop := range subject.Properties() {
			if subject.Props[prop].Staged == dir {
				return subject.Key, prop, true
			}
		}
	}
	return MigrationRecordKey{}, "", false
}

// migrationPreservingOnly is a preserve state naming exactly these
// directories, for a caller that computed the set itself rather than reading it
// off the shard's records.
func migrationPreservingOnly(dirs map[string]bool) migrationPreservedState {
	return migrationPreservedState{buckets: dirs}
}

func (s migrationPreservedState) preservesBucket(dir string) bool {
	if s.withholdEverything {
		return true
	}
	_, ok := s.buckets[dir]
	return ok
}

func (s migrationPreservedState) preservesTracker(dir string) bool {
	if s.withholdEverything {
		return true
	}
	_, ok := s.trackers[dir]
	return ok
}

// trackerNeedsLoad reports whether hydrating this shard would reclaim dir.
func (s migrationPreservedState) trackerNeedsLoad(dir string) bool {
	return s.trackers[dir]
}

// bucketNeedsLoad is [migrationPreservedState.trackerNeedsLoad] for a bucket
// directory: preserved, and a load would still act on it.
func (s migrationPreservedState) bucketNeedsLoad(dir string) bool {
	return s.buckets[dir]
}

// migrationPropertyLoadCanStillAct reports whether a shard load could change
// what this record holds for one property. A lost promotion has no exit
// anywhere in the system: the mark is written when a promoted directory is
// found gone and nothing clears it, so that property can never be promoted and
// a load reclaims nothing on its account.
//
// It is asked per property because promotion is per property: promoteSealed
// skips a lost one and promotes the rest, so a record can hold one property
// nothing will ever move next to one the very next load renames.
//
// Preservation is unaffected — the record and its directories are kept either
// way. Only the claim that hydrating the shard would reclaim them changes, and
// that claim is what drags a cold tenant into a load on every schema operation
// against its collection, forever.
//
// The record is still the exit's own witness: a resubmit supersedes it, and
// retirement removes it from the record set entirely before this is asked.
func migrationPropertyLoadCanStillAct(rec MigrationRecord, prop string) bool {
	sw, ok := rec.(MigrationRecordSwapped)
	if !ok {
		return true
	}
	return sw.PromotionOf(prop) != migrationPromotionLost
}

// bucketsOf names the preserved sidecars of one main bucket, sorted, for a
// log line that would otherwise report every migration on the shard.
func (s migrationPreservedState) bucketsOf(mainBucketName string) []string {
	out := make([]string, 0, len(s.buckets))
	for dir := range s.buckets {
		if isSidecarDirOf(dir, mainBucketName) {
			out = append(out, dir)
		}
	}
	slices.Sort(out)
	return out
}

// migrationRecordForTracker finds the record owning one tracker directory,
// for a reader holding just the directory name.
func migrationRecordForTracker(records []MigrationRecord, trackerDir string) (MigrationRecord, bool) {
	for _, rec := range records {
		if rec.Subject().TrackerDir == trackerDir {
			return rec, true
		}
	}
	return nil, false
}

// migrationRecordStagingIncomplete is the want-predicate for readers asking
// the opposite of StagedDataComplete.
func migrationRecordStagingIncomplete(rec MigrationRecord) bool { return !rec.StagedDataComplete() }

// migrationRecordFor reports whether any record on the shard belongs to the
// named migration and satisfies want. Matching on type and property list,
// not directory names, covers both strategies a change-tokenization fans into.
func migrationRecordFor(records []MigrationRecord, migrationType ReindexMigrationType,
	properties []string, want func(MigrationRecord) bool,
) bool {
	for _, rec := range records {
		subject := rec.Subject()
		if subject.MigrationType != migrationType || !want(rec) {
			continue
		}
		for _, prop := range properties {
			if slices.Contains(subject.Properties(), prop) {
				return true
			}
		}
	}
	return false
}
