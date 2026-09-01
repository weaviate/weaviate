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

func migrationRecordsAt(lsmPath string, logger logrus.FieldLogger) (records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) {
	store, someRecordsUnreadable, recordSetUnreadable := migrationRecordStoreAt(lsmPath, logger)
	if recordSetUnreadable {
		return nil, true, true
	}
	return store.Records(), someRecordsUnreadable, false
}

// migrationRecordStoreAt is migrationRecordsAt for a caller that also writes.
//
// Deliberately silent on the healthy path: every caller fans this out over a
// shard set, so a line here follows the tenant count. Each walk reports its
// own record_set_reads instead, which is where one-read-per-shard is checkable.
func migrationRecordStoreAt(lsmPath string, logger logrus.FieldLogger) (store *MigrationRecordStore, someRecordsUnreadable, recordSetUnreadable bool) {
	store = NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true, true
	}
	return store, len(store.Unreadable()) > 0, false
}

type migrationPreservedState struct {
	records             []MigrationRecord
	buckets             map[string]bool
	trackers            map[string]bool
	withholdEverything  bool
	recordSetUnreadable bool
}

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
			state.trackers[subject.TrackerDir] = rec.State() != MigrationStatePromoted && anyCanAct
		}
	}
	return state
}

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

func (s migrationPreservedState) trackerNeedsLoad(dir string) bool {
	return s.trackers[dir]
}

func (s migrationPreservedState) bucketNeedsLoad(dir string) bool {
	return s.buckets[dir]
}

func migrationPropertyLoadCanStillAct(rec MigrationRecord, prop string) bool {
	sw, ok := rec.(MigrationRecordSwapped)
	if !ok {
		return true
	}
	return sw.PromotionOf(prop) != migrationPromotionLost
}

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

func migrationRecordForTracker(records []MigrationRecord, trackerDir string) (MigrationRecord, bool) {
	for _, rec := range records {
		if rec.Subject().TrackerDir == trackerDir {
			return rec, true
		}
	}
	return nil, false
}

func migrationRecordStagingIncomplete(rec MigrationRecord) bool { return !rec.StagedDataComplete() }

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

// migrationRecordStampedUnmirrored returns rec with MigrationSubject.Unmirrored
// set, keeping everything else. Only a record awaiting its flip can be stamped:
// before the iteration ends there is nothing staged to fall behind, and after
// the promotion the canonical name already holds the migrated data.
func migrationRecordStampedUnmirrored(rec MigrationRecord) (MigrationRecord, bool) {
	switch typed := rec.(type) {
	case MigrationRecordIterated:
		typed.subject.Unmirrored = true
		return typed, true
	case MigrationRecordMerged:
		typed.subject.Unmirrored = true
		return typed, true
	case MigrationRecordSwapped:
		typed.subject.Unmirrored = true
		return typed, true
	default:
		return nil, false
	}
}
