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
// frozen reports that at least one record could not be understood. Its
// property list is exactly what could not be read, so there is no way to scope
// the withholding to the directories it stands on — the same shard-wide
// reading reconciliation applies at load. A listing this build cannot read at
// all freezes the shard for the same reason.
func migrationRecordsAt(lsmPath string, logger logrus.FieldLogger) (records []MigrationRecord, frozen bool) {
	store := NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true
	}
	return store.Records(), len(store.Unreadable()) > 0
}

// migrationCommittedState names what a sweep of one shard must leave alone.
// A record whose data is committed owns directories that back a live
// in-memory bucket pointer: removing one empties the canonical bucket on the
// node that submitted the migration, and nothing reports it.
type migrationCommittedState struct {
	buckets  map[string]struct{}
	trackers map[string]struct{}

	// frozen preserves everything on the shard. A record this build cannot
	// read may name any directory here, and deleting one it names is exactly
	// the loss the three-outcome loader exists to prevent.
	frozen bool
}

func migrationCommittedStateOf(records []MigrationRecord, frozen bool) migrationCommittedState {
	state := migrationCommittedState{
		buckets:  map[string]struct{}{},
		trackers: map[string]struct{}{},
		frozen:   frozen,
	}
	for _, rec := range records {
		if !rec.DataCommitted() {
			continue
		}
		subject := rec.Subject()
		for _, dir := range migrationOwnedDirs(subject) {
			state.buckets[dir] = struct{}{}
		}
		if subject.TrackerDir != "" {
			state.trackers[subject.TrackerDir] = struct{}{}
		}
	}
	return state
}

func (s migrationCommittedState) preservesBucket(dir string) bool {
	if s.frozen {
		return true
	}
	_, ok := s.buckets[dir]
	return ok
}

func (s migrationCommittedState) preservesTracker(dir string) bool {
	if s.frozen {
		return true
	}
	_, ok := s.trackers[dir]
	return ok
}

// bucketsOf names the preserved sidecars of one main bucket, sorted, for a
// log line that would otherwise report every migration on the shard.
func (s migrationCommittedState) bucketsOf(mainBucketName string) []string {
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

// migrationRecordUncommitted is the want-predicate for the readers asking the
// opposite of DataCommitted: a migration whose staged data is not yet the
// data, and whose double-write mirror therefore still has to be armed.
func migrationRecordUncommitted(rec MigrationRecord) bool { return !rec.DataCommitted() }

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
