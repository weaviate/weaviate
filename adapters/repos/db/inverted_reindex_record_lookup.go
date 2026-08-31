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
	store := NewMigrationRecordStore(lsmPath, logger)
	if err := store.Load(); err != nil {
		logger.WithField("path", store.Dir()).Errorf("read migration records: %v", err)
		return nil, true, true
	}
	logger.WithField("path", store.Dir()).WithField("records", len(store.Records())).
		Debug("read migration records")
	return store.Records(), len(store.Unreadable()) > 0, false
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
