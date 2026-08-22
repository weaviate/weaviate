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

// migrationRecordsAt reads one shard's records straight from disk. The sweeps
// and gates that decide what to do about a cold tenant need the same answers a
// loaded shard's store gives, and a record is a few hundred bytes.
//
// someRecordsUnreadable reports that at least one record could not be
// understood. Its property list is exactly what could not be read, so there is
// no way to scope the withholding to the directories it stands on — the same
// shard-wide reading reconciliation applies at load.
//
// recordSetUnreadable is the strictly stronger fault: the record set could not
// be read at all, so this shard's state is not merely undecidable but
// invisible, and a caller that would otherwise report the shard clean has to
// fail open on it. It never comes back true on its own, because a set nothing
// could read is also a set some record could not be read from.
func migrationRecordsAt(lsmPath string, logger logrus.FieldLogger) (records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) {
	store := NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true, true
	}
	// One line per read, on the healthy path too: several gates ask this
	// question once per shard while the payload names many (property, index
	// type) tuples, and a regression to one read per tuple is otherwise
	// invisible until it shows up as startup latency.
	logger.WithField("path", store.Dir()).WithField("records", len(store.Records())).
		Debug("read migration records")
	return store.Records(), len(store.Unreadable()) > 0, false
}

// migrationPreservedState names what a sweep of one shard must leave alone.
// A record whose staged data is complete owns directories that back a live
// in-memory bucket pointer: removing one empties the canonical bucket on the
// node that submitted the migration, and nothing reports it.
type migrationPreservedState struct {
	// records is the whole understood set, not just the preserved part: the
	// sweeps also ask an extant record which properties its directory belongs
	// to, which is a subject fact and true in every state.
	records []MigrationRecord
	buckets map[string]struct{}
	// trackers maps each preserved migration directory to whether a load
	// would still do disk work for it.
	trackers map[string]bool

	// someRecordsUnreadable preserves everything on the shard. A record this
	// build cannot read may name any directory here, and deleting one it names
	// is exactly the loss the three-outcome loader exists to prevent.
	someRecordsUnreadable bool
	// recordSetUnreadable means nothing here could be read at all, which no
	// caller may report as a clean shard. It implies someRecordsUnreadable.
	recordSetUnreadable bool
}

func migrationPreservedStateOf(records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) migrationPreservedState {
	state := migrationPreservedState{
		records:               records,
		buckets:               map[string]struct{}{},
		trackers:              map[string]bool{},
		someRecordsUnreadable: someRecordsUnreadable,
		recordSetUnreadable:   recordSetUnreadable,
	}
	for _, rec := range records {
		if !rec.StagedDataComplete() {
			continue
		}
		subject := rec.Subject()
		for _, dir := range migrationOwnedDirs(subject) {
			state.buckets[dir] = struct{}{}
		}
		if subject.TrackerDir != "" {
			// A promoted record's own directory waits on the schema effect
			// becoming visible, which no load can make happen, so it never
			// justifies a hydration on its own. The directories it still owns
			// are a load's work, and the caller counts those from the buckets
			// above.
			state.trackers[subject.TrackerDir] = rec.State() != MigrationStatePromoted
		}
	}
	return state
}

func (s migrationPreservedState) preservesBucket(dir string) bool {
	if s.someRecordsUnreadable {
		return true
	}
	_, ok := s.buckets[dir]
	return ok
}

func (s migrationPreservedState) preservesTracker(dir string) bool {
	if s.someRecordsUnreadable {
		return true
	}
	_, ok := s.trackers[dir]
	return ok
}

// trackerNeedsLoad reports whether hydrating this shard would reclaim dir.
func (s migrationPreservedState) trackerNeedsLoad(dir string) bool {
	return s.trackers[dir]
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

// migrationRecordForTracker finds the record that owns one tracker directory.
// It is how a reader still holding a directory name — the orphan audit, the
// startup discovery walk — reaches the state that directory is in.
func migrationRecordForTracker(records []MigrationRecord, trackerDir string) (MigrationRecord, bool) {
	for _, rec := range records {
		if rec.Subject().TrackerDir == trackerDir {
			return rec, true
		}
	}
	return nil, false
}

// migrationRecordStagingIncomplete is the want-predicate for the readers
// asking the opposite of StagedDataComplete: a migration whose staged data is
// not yet the data, and whose double-write mirror still has to be armed.
func migrationRecordStagingIncomplete(rec MigrationRecord) bool { return !rec.StagedDataComplete() }

// migrationRecordFor reports whether any record on the shard belongs to the
// named migration and satisfies want. Matching on the migration's own type
// and property list rather than on directory names is what lets one payload
// answer for both strategies a change-tokenization fans a property into.
func migrationRecordFor(records []MigrationRecord, migrationType ReindexMigrationType,
	properties []string, want func(MigrationRecord) bool,
) bool {
	for _, rec := range records {
		subject := rec.Subject()
		if subject.MigrationType != migrationType || !want(rec) {
			continue
		}
		for _, prop := range properties {
			if slices.Contains(subject.Properties, prop) {
				return true
			}
		}
	}
	return false
}
