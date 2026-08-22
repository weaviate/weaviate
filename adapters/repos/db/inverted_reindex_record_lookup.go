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
// A record whose staged data is complete holds a complete copy nobody else
// holds: at Merged the copy the flip is about to make live, from Swapped on
// the live data itself. Removing a directory it names loses that copy, and
// nothing reports it.
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

// migrationPreservedStateAt is the only way to build one, so no sweep can ask
// what it must leave alone and be told about records but not about the
// marker-era directories of the release before them.
func migrationPreservedStateAt(lsmPath string, logger logrus.FieldLogger) migrationPreservedState {
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	state := migrationPreservedStateFromRecords(records, someRecordsUnreadable, recordSetUnreadable)
	if someRecordsUnreadable {
		// Already preserving the whole shard, so the scan would only cost
		// syscalls to reach the same answer.
		return state
	}
	for _, legacy := range migrationLegacyMarkerTrackersAt(lsmPath, records) {
		// false: no load can promote marker-era state on this build, so
		// hydrating the shard for it would do nothing.
		state.trackers[legacy.dirName] = false
		for _, dir := range legacy.sidecars {
			state.buckets[dir] = struct{}{}
		}
	}
	return state
}

func migrationPreservedStateFromRecords(records []MigrationRecord, someRecordsUnreadable, recordSetUnreadable bool) migrationPreservedState {
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

// mirrorFor names the (record, property) whose staged directory is dir. Every
// readable record answers, not just the committed ones: a sweep shuts down the
// buckets of migrations that are not committed, and those are exactly the ones
// whose mirror is still armed.
func (s migrationPreservedState) mirrorFor(dir string) (MigrationRecordKey, string, bool) {
	for _, rec := range s.records {
		subject := rec.Subject()
		for _, prop := range subject.Properties {
			if subject.StagedDirs[prop] == dir {
				return subject.Key, prop, true
			}
		}
	}
	return MigrationRecordKey{}, "", false
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

// migrationLegacyMarkerTracker is a tracker directory the release before the
// migration records left behind carrying its completion marker. The marker
// means the staged directories under it are the property's live data, and
// this build writes records instead, so nothing else on the shard vouches for
// them.
type migrationLegacyMarkerTracker struct {
	dirName  string
	marker   string
	prefix   string
	gen      int
	props    []string
	sidecars []string
}

// migrationLegacyMarkerTrackersAt finds them on one shard. A tracker a record
// names is this build's own and answers from the record; only a record-less
// one can be marker-era.
func migrationLegacyMarkerTrackersAt(lsmPath string, records []MigrationRecord) []migrationLegacyMarkerTracker {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		// Unreadable reads the same as absent on purpose: every caller uses
		// this to preserve more, and the sweeps that would delete on the
		// strength of it fail on the same listing for themselves.
		return nil
	}
	var out []migrationLegacyMarkerTracker
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		prefix, gen, ok := parseMigrationDirName(dirName)
		if !ok {
			continue
		}
		if _, named := migrationRecordForTracker(records, dirName); named {
			continue
		}
		marker, found := migrationCompletionMarker(filepath.Join(migsDir, dirName))
		if !found {
			continue
		}
		props, _ := readTaskProps(filepath.Join(migsDir, dirName))
		out = append(out, migrationLegacyMarkerTracker{
			dirName:  dirName,
			marker:   marker,
			prefix:   prefix,
			gen:      gen,
			props:    append([]string(nil), props.props...),
			sidecars: migrationSidecarDirsFor(dirName, prefix, gen, props.props),
		})
	}
	return out
}

// migrationLegacyMarkerDirsAt is the same answer as a name set, for the
// removal loops that keep their own record check rather than the preserve
// predicate.
func migrationLegacyMarkerDirsAt(lsmPath string, records []MigrationRecord) map[string]struct{} {
	dirs := map[string]struct{}{}
	for _, legacy := range migrationLegacyMarkerTrackersAt(lsmPath, records) {
		dirs[legacy.dirName] = struct{}{}
		for _, sidecar := range legacy.sidecars {
			dirs[sidecar] = struct{}{}
		}
	}
	return dirs
}

// servesEmpty reports the properties whose data this tracker still holds under
// its staged name while the canonical bucket dir is gone. That is the state an
// operator has to act on: the schema flip already committed cluster-wide, and
// no path on this build renames the staged directory back.
func (t migrationLegacyMarkerTracker) servesEmpty(lsmPath string) []string {
	suffixes := migrationSuffixes(t.dirName)
	if suffixes == nil {
		return nil
	}
	genTail := genSuffix(t.gen)
	var out []string
	for _, prop := range t.props {
		canonical := suffixes.sourceBucketName(prop)
		if fileExists(filepath.Join(lsmPath, canonical)) {
			continue
		}
		if !fileExists(filepath.Join(lsmPath, canonical+suffixes.ingestSuffix+genTail)) {
			continue
		}
		out = append(out, prop)
	}
	return out
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
