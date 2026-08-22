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
	// One line per read, even on the healthy path — a per-tuple read
	// regression would otherwise be invisible until startup latency shows it.
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
	buckets map[string]struct{}
	// trackers maps each preserved migration directory to whether a load
	// would still do disk work for it.
	trackers map[string]bool

	// withholdEverything preserves the whole shard: a record this build
	// cannot read, or a marker-era tracker whose payload could not be read,
	// names directories nothing else accounts for.
	withholdEverything bool
	// recordSetUnreadable means no record here could be read at all, so no
	// caller may report the shard clean. It implies withholdEverything.
	recordSetUnreadable bool
	// migrationsDirUnlistable is the same fault one level up: the tracker
	// directory itself could not be enumerated. Kept apart from
	// withholdEverything since an unlistable shard has not been read at all.
	migrationsDirUnlistable bool
}

// migrationPreservedStateAt is the only way to build a migrationPreservedState,
// so no sweep can learn about records but not about marker-era directories.
func migrationPreservedStateAt(lsmPath string, logger logrus.FieldLogger) migrationPreservedState {
	records, someRecordsUnreadable, recordSetUnreadable := migrationRecordsAt(lsmPath, logger)
	state := migrationPreservedStateFromRecords(records, someRecordsUnreadable, recordSetUnreadable)
	legacyTrackers, listed := migrationLegacyMarkerTrackersAt(lsmPath, records)
	if someRecordsUnreadable && listed {
		// Already preserving the whole shard, so reading trackers would only
		// cost syscalls; listing still had to happen to catch an unlistable
		// directory.
		return state
	}
	if !listed {
		// Nothing here could be enumerated, so the preserve set is missing
		// names sweeps could read as permission to delete. Debug here since a
		// DELETE asks this per (property, index type) per shard inside the
		// RAFT apply; the shard load warns once.
		logger.WithField("path", filepath.Join(lsmPath, ".migrations")).
			Debug("the migration directory could not be listed; withholding every removal on this shard")
		state.withholdEverything = true
		state.migrationsDirUnlistable = true
		return state
	}
	for _, legacy := range legacyTrackers {
		if legacy.unreadable {
			// Its payload names the directories holding this marker's data;
			// preserving only the tracker would strand them from the reclaimers.
			state.withholdEverything = true
			continue
		}
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
		records:             records,
		buckets:             map[string]struct{}{},
		trackers:            map[string]bool{},
		withholdEverything:  someRecordsUnreadable,
		recordSetUnreadable: recordSetUnreadable,
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
			// A promoted record's own directory waits on the schema effect, which
			// no load can force, so it never justifies hydration alone; its owned
			// directories still do, counted from buckets above.
			state.trackers[subject.TrackerDir] = rec.State() != MigrationStatePromoted
		}
	}
	return state
}

// mirrorFor names the (record, property) whose staged directory is dir. Every
// readable record answers, not just committed ones.
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

// migrationLegacyMarkerTracker is a tracker directory a pre-migration-records
// release left behind, with a completion marker naming the property's live
// data at that staged name; this build writes records instead.
type migrationLegacyMarkerTracker struct {
	dirName string
	marker  string
	prefix  string
	gen     int
	// unreadable means the payload could not be read, so props and sidecars
	// are empty because nothing could be learned, not because there is none.
	unreadable bool
	props      []string
	sidecars   []string
}

// migrationLegacyMarkerTrackersAt finds legacy trackers on one shard; only a
// record-less tracker can be marker-era.
//
// listed=false is distinct from finding none: a fault hiding every
// marker-era tracker (fd exhaustion on a many-tenant node) would otherwise
// free an upgraded property's only surviving copy to be removed.
func migrationLegacyMarkerTrackersAt(lsmPath string, records []MigrationRecord) (trackers []migrationLegacyMarkerTracker, listed bool) {
	migsDir := filepath.Join(lsmPath, ".migrations")
	entries, err := os.ReadDir(migsDir)
	if err != nil {
		// Absent is the ordinary case — most shards never ran a migration —
		// and it really does mean there is nothing marker-era here.
		return nil, os.IsNotExist(err)
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
			dirName:    dirName,
			marker:     marker,
			prefix:     prefix,
			gen:        gen,
			unreadable: props.unreadable,
			props:      append([]string(nil), props.props...),
			sidecars:   migrationSidecarDirsFor(dirName, prefix, gen, props.props),
		})
	}
	return out, true
}

// migrationLegacyMarkerDirsAt is the same answer as a name set, for removal
// loops keeping their own record check. complete=false means names are
// missing (unreadable payload, or unlistable directory), so callers must stop.
func migrationLegacyMarkerDirsAt(lsmPath string, records []MigrationRecord) (dirs map[string]struct{}, complete bool) {
	dirs = map[string]struct{}{}
	trackers, listed := migrationLegacyMarkerTrackersAt(lsmPath, records)
	if !listed {
		return dirs, false
	}
	complete = true
	for _, legacy := range trackers {
		if legacy.unreadable {
			complete = false
			continue
		}
		dirs[legacy.dirName] = struct{}{}
		for _, sidecar := range legacy.sidecars {
			dirs[sidecar] = struct{}{}
		}
	}
	return dirs, complete
}

// servesEmpty reports properties whose data is still under this tracker's
// staged name while the canonical directory is gone — the schema flip already
// committed cluster-wide, and no path on this build renames it back.
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
			if slices.Contains(subject.Properties, prop) {
				return true
			}
		}
	}
	return false
}
