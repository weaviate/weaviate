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
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/diskio"
)

const migrationRecordsDirName = "records"

type MigrationRecordLoadOutcome uint8

const (
	MigrationRecordLoaded MigrationRecordLoadOutcome = iota
	MigrationRecordAbsent
	MigrationRecordNotUnderstood
)

type MigrationRecordFaultScope uint8

const (
	MigrationRecordFaultFile MigrationRecordFaultScope = iota
	MigrationRecordFaultStore
)

type MigrationRecordUnreadable struct {
	FileName string
	Reason   string
	Scope    MigrationRecordFaultScope
}

// A record this build could not read holds whatever a hand edit or a restore
// put in it, and a decode refusal quotes the handle it refused. Only the 8 MiB
// read cap bounds that, and the reason reaches a log line the freeze re-emits
// on every load of the shard.
const maxMigrationFaultReasonBytes = 512

// Both ends, because the quoted handle sits in the middle: the head says which
// record and which role, the tail says what is wrong with it.
func migrationFaultReason(err error) string {
	reason := err.Error()
	if len(reason) <= maxMigrationFaultReasonBytes {
		return reason
	}
	const half = maxMigrationFaultReasonBytes / 2
	return strings.ToValidUTF8(reason[:half], "") + "…(truncated)…" +
		strings.ToValidUTF8(reason[len(reason)-half:], "")
}

// Readers are served from the map: activating a tenant loads its shard on
// the RAFT apply loop, where a read that went to disk would hold up the log.
type MigrationRecordStore struct {
	dir string
	// The unit whose records are this shard's own. Empty where the caller
	// cannot name one, which reads every record as local, as before.
	unitID string
	logger logrus.FieldLogger

	// Separate from mu: mu must never be held across an fsync, or a reader
	// on the apply loop would queue behind the disk.
	writeMu sync.Mutex

	mu         sync.RWMutex
	records    map[MigrationRecordKey]MigrationRecord
	unreadable []MigrationRecordUnreadable
	// Records no pass on this build could advance. Cleared on every load, since
	// the pass that follows a load re-derives it.
	wedged map[MigrationRecordKey]bool
}

// The store names no unit, so every record it loads reads as local. Callers
// that can name the unit owning this shard's records use
// NewMigrationRecordStoreForUnit, which is what tells a foreign one apart.
func NewMigrationRecordStore(lsmPath string, logger logrus.FieldLogger) *MigrationRecordStore {
	return NewMigrationRecordStoreForUnit(lsmPath, "", logger)
}

func NewMigrationRecordStoreForUnit(lsmPath, unitID string, logger logrus.FieldLogger) *MigrationRecordStore {
	return &MigrationRecordStore{
		dir:     filepath.Join(lsmPath, migrationsDir, migrationRecordsDirName),
		unitID:  unitID,
		logger:  logger,
		records: map[MigrationRecordKey]MigrationRecord{},
	}
}

func (s *MigrationRecordStore) Dir() string { return s.dir }

// Two levels, not MkdirAll: a write racing a collection DELETE, which renames
// the class directory away, would otherwise re-materialize the deleted tree.
func (s *MigrationRecordStore) mkdir() error {
	return s.mkdirSynced(diskio.Fsync)
}

// The sync is a parameter for the same reason writeFileAtomicWithSync takes
// one: a test can assert a created directory's own name is on disk before a
// record is written into it.
func (s *MigrationRecordStore) mkdirSynced(sync func(string) error) error {
	for _, dir := range []string{filepath.Dir(s.dir), s.dir} {
		if err := os.Mkdir(dir, 0o777); err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
		// The record's own fsync does not publish the directory entry holding
		// it. Without this a crash leaves the record synced into a directory
		// that is gone, and Load then reports no records at all rather than an
		// unreadable one, which is the reading the freeze cannot see.
		if err := sync(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
}

func (s *MigrationRecordStore) path(key MigrationRecordKey) string {
	return filepath.Join(s.dir, key.fileName())
}

// Kept out of Load: any store may Load over a directory the owning shard is
// writing to, and removing its scratch file breaks that shard's rename. Only
// the owning shard calls this, once, before load.
func (s *MigrationRecordStore) SweepTempFiles() {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.WithField("path", s.dir).Warnf("sweep stale migration record temp files: %v", err)
		}
		return
	}
	var failed int
	var example string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, tmpExt) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
			failed++
			if example == "" {
				example = fmt.Sprintf("%s: %v", name, err)
			}
		}
	}
	if failed > 0 {
		s.logger.WithField("path", s.dir).WithField("file_count", failed).Warnf(
			"could not remove %d stale migration record temp file(s), for example %s", failed, example)
	}
}

func (s *MigrationRecordStore) Load() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			s.publish(map[MigrationRecordKey]MigrationRecord{}, nil)
			return nil
		}
		s.publish(map[MigrationRecordKey]MigrationRecord{}, []MigrationRecordUnreadable{{
			FileName: migrationRecordsDirName,
			Reason:   fmt.Sprintf("read migration records dir: %v", err),
			Scope:    MigrationRecordFaultStore,
		}})
		return fmt.Errorf("read migration records dir %q: %w", s.dir, err)
	}

	records := make(map[MigrationRecordKey]MigrationRecord, len(entries))
	var unreadable, foreign []MigrationRecordUnreadable

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, tmpExt) {
			continue
		}

		rec, outcome, err := loadMigrationRecordFile(filepath.Join(s.dir, name))
		switch outcome {
		case MigrationRecordLoaded:
			if got := rec.Subject().Key.fileName(); got != name {
				unreadable = append(unreadable, MigrationRecordUnreadable{
					FileName: name,
					Reason:   fmt.Sprintf("content names record file %q", got),
					Scope:    MigrationRecordFaultFile,
				})
				continue
			}
			if s.foreignUnit(rec.Subject().Key) {
				foreign = append(foreign, MigrationRecordUnreadable{
					FileName: name,
					Reason:   fmt.Sprintf("unit %q is not this shard's own", rec.Subject().Key.UnitID),
					Scope:    MigrationRecordFaultFile,
				})
				continue
			}
			records[rec.Subject().Key] = rec
		case MigrationRecordAbsent:
			continue
		case MigrationRecordNotUnderstood:
			unreadable = append(unreadable, MigrationRecordUnreadable{
				FileName: name, Reason: migrationFaultReason(err), Scope: MigrationRecordFaultFile,
			})
		}
	}

	unreadable = append(unreadable, refuseRecordsOfSeveralUnits(records)...)
	unreadable = append(unreadable, refuseRecordsOfDuplicateClaims(records)...)
	s.reportForeign(foreign)

	// One line per load, not one per file: Load runs on every shard load, which
	// on a multi-tenant collection is once per tenant. Error, not Warn: this
	// withholds every destructive action on the shard until someone acts.
	if len(unreadable) > 0 {
		names := make([]string, 0, len(unreadable))
		for _, u := range unreadable {
			names = append(names, u.FileName)
		}
		s.logger.WithField("path", s.dir).WithField("file_count", len(unreadable)).Errorf(
			"%d migration record(s) not understood, preserving them and withholding destructive work; "+
				"files: %s; first reason: %s",
			len(unreadable), strings.Join(migrationReportedNames(names), ", "), unreadable[0].Reason)
	}

	s.publish(records, unreadable)
	return nil
}

// A backup restore and a replica move both land another replica's record file
// under this shard, where nothing else tells it apart from a local one: it is
// granted a seal no local worker holds and is then reconciled as local work.
// Set aside rather than adopted, and left on disk for whoever put it there.
func (s *MigrationRecordStore) foreignUnit(key MigrationRecordKey) bool {
	return s.unitID != "" && key.UnitID != s.unitID
}

// One line per load, not one per file: Load runs once per tenant on a
// multi-tenant collection, and a restore lands every replica's records at once.
func (s *MigrationRecordStore) reportForeign(foreign []MigrationRecordUnreadable) {
	if len(foreign) == 0 {
		return
	}
	names := make([]string, 0, len(foreign))
	for _, f := range foreign {
		names = append(names, f.FileName)
	}
	s.logger.WithField("path", s.dir).WithField("file_count", len(foreign)).Warnf(
		"%d migration record(s) here belong to another replica of this shard and are ignored; "+
			"they were most likely left by a restore or a replica move, and the directories they "+
			"name are not this shard's to reclaim. Files: %s",
		len(foreign), strings.Join(migrationReportedNames(names), ", "))
}

// A fault, not a preference: a teardown always seals a foreign unit, so
// dropping foreign records would strand their directories for reclaimers.
func refuseRecordsOfSeveralUnits(records map[MigrationRecordKey]MigrationRecord) []MigrationRecordUnreadable {
	units := map[string]struct{}{}
	for key := range records {
		units[key.UnitID] = struct{}{}
	}
	if len(units) < 2 {
		return nil
	}
	named := make([]string, 0, len(units))
	for unit := range units {
		named = append(named, unit)
	}
	slices.Sort(named)
	return []MigrationRecordUnreadable{{
		FileName: migrationRecordsDirName,
		Reason: fmt.Sprintf(
			"records of %d units are here (%s); this node cannot tell which is its own, "+
				"and sealing a foreign one would not hold back a live local worker. "+
				"Move or remove the record files whose unit is not this shard's, and leave this shard's own in place",
			len(named), strings.Join(migrationReportedNames(named), ", ")),
		Scope: MigrationRecordFaultStore,
	}}
}

// A tracker directory sits under .migrations and every other claim at the shard
// root, so the two are kept apart here or one name shared across them reads as a
// collision over a directory that does not exist.
func migrationExclusiveClaims(subject MigrationSubject) []string {
	claims := migrationOwnedDirs(subject)
	if subject.TrackerDir != "" {
		claims = append(claims, filepath.Join(migrationsDir, subject.TrackerDir))
	}
	return claims
}

// A directory name a migration mints carries its own task version, so only a
// hand-edited or restore-landed file can claim a directory another record
// claims. Both claimants leave the live set: nothing here can tell which one
// the data belongs to, and either answer would delete the other's only copy.
func refuseRecordsOfDuplicateClaims(records map[MigrationRecordKey]MigrationRecord) []MigrationRecordUnreadable {
	keys := slices.SortedFunc(maps.Keys(records), func(a, b MigrationRecordKey) int {
		return cmp.Compare(a.fileName(), b.fileName())
	})
	// Per unit, because two replicas of one shard name the same directories on
	// their own shards. Records of several units under one shard is its own
	// fault, above, and this shard's own records are all of one unit.
	type claimedBy struct{ unit, dir string }
	held := map[claimedBy]MigrationRecordKey{}
	shared := map[MigrationRecordKey]string{}
	against := map[MigrationRecordKey]MigrationRecordKey{}
	for _, key := range keys {
		for _, dir := range migrationExclusiveClaims(records[key].Subject()) {
			claim := claimedBy{key.UnitID, dir}
			first, taken := held[claim]
			if !taken {
				held[claim] = key
				continue
			}
			shared[first], against[first] = dir, key
			shared[key], against[key] = dir, first
		}
	}

	refused := make([]MigrationRecordUnreadable, 0, len(shared))
	for _, key := range keys {
		claim, collided := shared[key]
		if !collided {
			continue
		}
		delete(records, key)
		refused = append(refused, MigrationRecordUnreadable{
			FileName: key.fileName(),
			Reason: fmt.Sprintf(
				"this record and %q both claim directory %q, and this node cannot tell which one holds "+
					"its data. Confirm which migration the directory belongs to, then move or remove the "+
					"record file of the other one",
				against[key].fileName(), claim),
			Scope: MigrationRecordFaultFile,
		})
	}
	return refused
}

func (s *MigrationRecordStore) publish(records map[MigrationRecordKey]MigrationRecord, unreadable []MigrationRecordUnreadable) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = records
	s.unreadable = unreadable
	s.wedged = nil
}

// MarkWedged records that nothing this build runs can move this record on. The
// reconciler holding that knowledge lives for one pass, so without this one
// wedged record costs a leader query and a walk of every loaded shard on the
// node, once a minute, for the life of the process, and never progresses.
func (s *MigrationRecordStore) MarkWedged(key MigrationRecordKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wedged == nil {
		s.wedged = map[MigrationRecordKey]bool{}
	}
	s.wedged[key] = true
}

// Read by every later pass in this incarnation. The reconciler that diagnosed
// the wedge lives for one pass, so without this the next pass derives the same
// verdict, logs it again and counts it again, once a minute, forever.
func (s *MigrationRecordStore) Wedged(key MigrationRecordKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.wedged[key]
}

func (s *MigrationRecordStore) Put(rec MigrationRecord) error {
	data, err := encodeMigrationRecord(rec)
	if err != nil {
		return fmt.Errorf("encode migration record %q: %w", rec.Subject().Key, err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.mayWrite(rec.Subject().Key); err != nil {
		return err
	}
	if err := s.mkdir(); err != nil {
		return fmt.Errorf("create migration records dir %q: %w", s.dir, err)
	}
	if err := writeFileAtomic(s.dir, rec.Subject().Key.fileName(), data); err != nil {
		s.adoptIfPublished(rec, data)
		return fmt.Errorf("write migration record %q: %w", rec.Subject().Key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.Subject().Key] = rec
	return nil
}

// [diskio.RenameAndSync] renames before it syncs, so a failed write can follow
// a rename that already published the record. Nothing else re-reads the
// store, so memory must be settled against the file here or it serves the
// older record for the life of the process.
func (s *MigrationRecordStore) adoptIfPublished(rec MigrationRecord, data []byte) {
	published, err := os.ReadFile(s.path(rec.Subject().Key))
	if err != nil || !bytes.Equal(published, data) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.Subject().Key] = rec
}

func (s *MigrationRecordStore) Remove(key MigrationRecordKey) error {
	return s.removeSynced(key, diskio.Fsync)
}

// The sync is a parameter for the same reason mkdirSynced takes one: a test can
// assert the unlink's own directory entry reached disk.
func (s *MigrationRecordStore) removeSynced(key MigrationRecordKey, sync func(string) error) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.mayWrite(key); err != nil {
		return err
	}
	removed := os.Remove(s.path(key))
	if removed != nil && !os.IsNotExist(removed) {
		return fmt.Errorf("remove migration record %q: %w", key, removed)
	}

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.records, key)
		delete(s.wedged, key)
	}()

	if removed != nil {
		return nil
	}
	// The unlink survives a crash only once the directory holding it is synced.
	// Without this a retired record comes back on the next load, naming
	// directories the pass that retired it already removed.
	if err := sync(s.dir); err != nil {
		return fmt.Errorf("publish the removal of migration record %q: %w", key, err)
	}
	return nil
}

// Any fault at all holds back every write, not only one naming this record's
// own file. A record this build could not read may name the same directories,
// so a write decided without it is decided on a state nobody here can see.
func (s *MigrationRecordStore) mayWrite(key MigrationRecordKey) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.unreadable) == 0 {
		return nil
	}
	name := key.fileName()
	for _, u := range s.unreadable {
		if u.Scope == MigrationRecordFaultFile && u.FileName == name {
			return fmt.Errorf("refusing to write migration record %q over a file this build could not read: %s",
				key, u.Reason)
		}
	}
	u := s.unreadable[0]
	return fmt.Errorf("refusing to write migration record %q: this build could not read %q, "+
		"so it cannot tell what is already recorded here: %s", key, u.FileName, u.Reason)
}

func (s *MigrationRecordStore) Get(key MigrationRecordKey) (MigrationRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[key]
	return rec, ok
}

// slices.SortFunc is not stable, so the order has to cover the whole key or
// two passes over one store disagree.
func (s *MigrationRecordStore) Records() []MigrationRecord {
	out := func() []MigrationRecord {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make([]MigrationRecord, 0, len(s.records))
		for _, rec := range s.records {
			out = append(out, rec)
		}
		return out
	}()

	slices.SortFunc(out, func(a, b MigrationRecord) int {
		ak, bk := a.Subject().Key, b.Subject().Key
		if c := cmp.Compare(ak.TaskVersion, bk.TaskVersion); c != 0 {
			return c
		}
		if c := cmp.Compare(ak.StrategyCode, bk.StrategyCode); c != 0 {
			return c
		}
		return cmp.Compare(ak.UnitID, bk.UnitID)
	})
	return out
}

// HasUndecided reports whether any understood record is still pre-swap and
// still movable. A wedged one is neither, and asking again about it is what
// makes the periodic pass repeat forever with nothing to show for it.
func (s *MigrationRecordStore) HasUndecided() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, rec := range s.records {
		if !rec.PointerSwapped() && !s.wedged[key] {
			return true
		}
	}
	return false
}

func (s *MigrationRecordStore) Unreadable() []MigrationRecordUnreadable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.unreadable)
}

// Load runs inside the RAFT apply of UpdateTenants, holding the FSM loop
// cluster-wide, so this bound is load-bearing; see
// TestTheLargestRecordTheWriterCanBuildFitsTheLoadersBound.
const maxMigrationRecordBytes = 8 << 20

func loadMigrationRecordFile(path string) (MigrationRecord, MigrationRecordLoadOutcome, error) {
	// An over-bound file reads as not-understood, not absent: unread, it may
	// name directories that must not be reclaimed.
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, MigrationRecordAbsent, nil
		}
		return nil, MigrationRecordNotUnderstood, fmt.Errorf("stat %q: %w", filepath.Base(path), err)
	}
	if info.Size() > maxMigrationRecordBytes {
		return nil, MigrationRecordNotUnderstood, fmt.Errorf(
			"record %q holds %d bytes, bound is %d", filepath.Base(path), info.Size(), maxMigrationRecordBytes)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, MigrationRecordAbsent, nil
		}
		return nil, MigrationRecordNotUnderstood, fmt.Errorf("read %q: %w", filepath.Base(path), err)
	}

	rec, err := decodeMigrationRecord(data)
	if err != nil {
		return nil, MigrationRecordNotUnderstood, err
	}
	return rec, MigrationRecordLoaded, nil
}

func writeFileAtomic(dir, name string, content []byte) error {
	return writeFileAtomicWithSync(dir, name, content, (*os.File).Sync)
}

// The sync is a parameter so a test can assert it runs after the bytes and
// before the rename publishes the name.
func writeFileAtomicWithSync(dir, name string, content []byte, sync func(*os.File) error) (err error) {
	tmp, err := os.CreateTemp(dir, name+".*"+tmpExt)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	if _, err = tmp.Write(content); err != nil {
		return err
	}
	// Bytes must reach disk before the name does, or a crash can publish the
	// new name over content that never landed.
	if err = sync(tmp); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return diskio.RenameAndSync(tmpPath, filepath.Join(dir, name))
}
