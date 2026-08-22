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
// loaded shard's store gives. A record scales with the property count of the
// migration it belongs to, so the read is bounded by
// [maxMigrationRecordBytes].
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

	// withholdEverything preserves the whole shard. Two things set it, and
	// they mean the same thing: a record this build cannot read, and a
	// marker-era tracker whose payload could not be read. Either names
	// directories nothing else on the shard vouches for, and deleting one is
	// exactly the loss the three-outcome loader exists to prevent.
	withholdEverything bool
	// recordSetUnreadable means no record here could be read at all, which no
	// caller may report as a clean shard. It implies withholdEverything.
	recordSetUnreadable bool
	// migrationsDirUnlistable is the same fault one level up: the directory
	// holding every tracker could not be enumerated. It is kept apart from
	// withholdEverything because the two call for opposite reporting — a
	// payload this build cannot parse is a settled fact that must not wake the
	// tenant on every sweep, while a directory nobody could list is a shard
	// whose state has not been read at all.
	migrationsDirUnlistable bool
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
	legacyTrackers, listed := migrationLegacyMarkerTrackersAt(lsmPath, records)
	if !listed {
		// Nothing here could be enumerated, so the preserve set is missing
		// names it cannot know. The sweeps read a short set as permission to
		// delete, and they list a directory that is still readable.
		logger.WithField("path", filepath.Join(lsmPath, ".migrations")).
			Warn("the migration directory could not be listed, so nothing on this shard can be shown to be " +
				"reclaimable; withholding every removal until it can be read")
		state.withholdEverything = true
		state.migrationsDirUnlistable = true
		return state
	}
	for _, legacy := range legacyTrackers {
		if legacy.unreadable {
			// Its payload is the only thing that names the directories holding
			// the data this marker claims. Preserving the tracker alone would
			// leave those to the reclaimers, which is the loss the marker
			// exists to prevent. Said out loud once per shard load, by
			// Shard.warnAboutLegacyMarkerMigrations; this runs per sweep.
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
	dirName string
	marker  string
	prefix  string
	gen     int
	// unreadable means the payload naming this tracker's properties could not
	// be read, so props and sidecars are empty because nothing could be
	// learned — not because the migration touched nothing.
	unreadable bool
	props      []string
	sidecars   []string
}

// migrationLegacyMarkerTrackersAt finds them on one shard. A tracker a record
// names is this build's own and answers from the record; only a record-less
// one can be marker-era.
//
// listed=false is the third outcome, and it is not the same as finding none.
// The sweeps that delete on this answer list the shard's LSM root, not the
// directory read here, so a fault that hides every marker-era tracker — fd
// exhaustion on a many-tenant node is the reachable one — leaves them free to
// remove an upgraded property's only surviving copy. A caller that cannot
// tell has to withhold, the same way the record loader's NotUnderstood does.
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

// migrationLegacyMarkerDirsAt is the same answer as a name set, for the
// removal loops that keep their own record check rather than the preserve
// predicate. complete=false means the set is missing names it cannot know —
// one tracker's payload could not be read, or the directory holding all of
// them could not be listed — and a caller that deletes what is not in it has
// to stop instead.
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
