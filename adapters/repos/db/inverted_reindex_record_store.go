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
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/diskio"
)

// migrationRecordsDirName is a subdirectory of .migrations rather than the
// root of it, so foreign files that already live there — the named-vector
// quantization flag — never reach the record loader at all.
const migrationRecordsDirName = "records"

// MigrationRecordLoadOutcome distinguishes the two ways a record can fail to
// produce a value. Absent means the migration does not exist; NotUnderstood
// means something is there that this build cannot place, which withholds
// destructive work instead of licensing it.
type MigrationRecordLoadOutcome uint8

const (
	MigrationRecordLoaded MigrationRecordLoadOutcome = iota
	MigrationRecordAbsent
	MigrationRecordNotUnderstood
)

// MigrationRecordFaultScope says how much of the store one fault covers. A
// fault that freezes readers has to freeze writers on the same scope, and the
// two scopes are keyed differently, so the scope travels with the fault
// instead of being inferred from its name.
type MigrationRecordFaultScope uint8

const (
	// MigrationRecordFaultFile is one file this build could not place. Only
	// the key that renders to that file name is frozen.
	MigrationRecordFaultFile MigrationRecordFaultScope = iota
	// MigrationRecordFaultStore is the records directory itself. Nothing
	// about any migration could be read, so every key is frozen.
	MigrationRecordFaultStore
)

// MigrationRecordUnreadable is something in the records directory that this
// build could not place. It is kept by file name because the key it would have
// had is exactly what could not be read.
type MigrationRecordUnreadable struct {
	FileName string
	Reason   string
	Scope    MigrationRecordFaultScope
}

// MigrationRecordStore owns one shard's migration records. Disk is durability
// only: readers on the apply path are served from the map.
type MigrationRecordStore struct {
	dir    string
	logger logrus.FieldLogger

	// writeMu orders disk work. It is separate from mu because mu must never
	// be held across an fsync — every apply-path reader would queue behind the
	// disk — while file order and map order still have to agree.
	writeMu sync.Mutex

	mu         sync.RWMutex
	records    map[MigrationRecordKey]MigrationRecord
	unreadable []MigrationRecordUnreadable
}

func NewMigrationRecordStore(lsmPath string, logger logrus.FieldLogger) *MigrationRecordStore {
	return &MigrationRecordStore{
		dir:     filepath.Join(lsmPath, migrationsDir, migrationRecordsDirName),
		logger:  logger,
		records: map[MigrationRecordKey]MigrationRecord{},
	}
}

func (s *MigrationRecordStore) Dir() string { return s.dir }

// mkdir creates the two levels below the shard's LSM directory and not one
// level more. MkdirAll would rebuild the whole path, so a write racing a
// collection DELETE — which renames the class directory away — would
// re-materialize the deleted collection's tree around one record file.
func (s *MigrationRecordStore) mkdir() error {
	for _, dir := range []string{filepath.Dir(s.dir), s.dir} {
		if err := os.Mkdir(dir, 0o777); err != nil && !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func (s *MigrationRecordStore) path(key MigrationRecordKey) string {
	return filepath.Join(s.dir, key.fileName())
}

// SweepTempFiles removes the scratch files a crash left behind. It is separate
// from Load because Load also serves throwaway stores built over a directory
// another store owns and is actively writing: deleting a scratch file there
// makes the owner's rename fail. Only the owning shard calls this, once, before
// it is loaded and while it is therefore the only writer.
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
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, tmpExt) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !os.IsNotExist(err) {
			s.logger.WithField("file", name).Warnf("remove stale migration record temp file: %v", err)
		}
	}
}

// Load replaces the in-memory contents from disk. A missing directory is the
// ordinary no-migration case.
func (s *MigrationRecordStore) Load() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			s.publish(map[MigrationRecordKey]MigrationRecord{}, nil)
			return nil
		}
		// Callers log this and carry on with the shard load, and an empty store
		// reads as "no migration here" — which licenses exactly the destructive
		// work a directory nobody could read has to withhold. Publish the fault
		// so every reader sees a frozen shard rather than a clean one.
		s.publish(map[MigrationRecordKey]MigrationRecord{}, []MigrationRecordUnreadable{{
			FileName: migrationRecordsDirName,
			Reason:   fmt.Sprintf("read migration records dir: %v", err),
			Scope:    MigrationRecordFaultStore,
		}})
		return fmt.Errorf("read migration records dir %q: %w", s.dir, err)
	}

	records := make(map[MigrationRecordKey]MigrationRecord, len(entries))
	var unreadable []MigrationRecordUnreadable

	for _, entry := range entries {
		name := entry.Name()
		// A half-written scratch file is not a record, and it belongs to
		// whoever is writing it.
		if strings.HasSuffix(name, tmpExt) {
			continue
		}

		rec, outcome, err := loadMigrationRecordFile(filepath.Join(s.dir, name))
		switch outcome {
		case MigrationRecordLoaded:
			// The name is derived from the content, so a disagreement means
			// one of the two was tampered with or torn.
			if got := rec.Subject().Key.fileName(); got != name {
				unreadable = append(unreadable, MigrationRecordUnreadable{
					FileName: name,
					Reason:   fmt.Sprintf("content names record file %q", got),
				})
				continue
			}
			records[rec.Subject().Key] = rec
		case MigrationRecordAbsent:
			continue
		case MigrationRecordNotUnderstood:
			unreadable = append(unreadable, MigrationRecordUnreadable{FileName: name, Reason: err.Error()})
		}
	}

	for _, u := range unreadable {
		s.logger.WithField("file", u.FileName).Warnf(
			"migration record not understood, preserving it and withholding destructive work: %s", u.Reason)
	}

	s.publish(records, unreadable)
	return nil
}

func (s *MigrationRecordStore) publish(records map[MigrationRecordKey]MigrationRecord, unreadable []MigrationRecordUnreadable) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = records
	s.unreadable = unreadable
}

// Put durably writes rec and then publishes it in memory, so a record a reader
// can see is always one that survived a crash.
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
		return fmt.Errorf("write migration record %q: %w", rec.Subject().Key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.Subject().Key] = rec
	return nil
}

// Remove drops the record. It is idempotent: the edges that call it are
// re-derived at every load and must be safe to re-run.
func (s *MigrationRecordStore) Remove(key MigrationRecordKey) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := s.mayWrite(key); err != nil {
		return err
	}
	if err := os.Remove(s.path(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove migration record %q: %w", key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

// mayWrite is the one gate every write passes. The artifact a fault covers is
// exactly what the freeze exists to preserve, and what a write would put in
// its place is a guess about the migration nobody could read — so both Put and
// Remove ask here, and both scopes are answered in the same act.
func (s *MigrationRecordStore) mayWrite(key MigrationRecordKey) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name := key.fileName()
	for _, u := range s.unreadable {
		switch u.Scope {
		case MigrationRecordFaultStore:
			return fmt.Errorf("refusing to write migration record %q: this build could not read %q, "+
				"so it cannot tell what is already recorded here: %s", key, u.FileName, u.Reason)
		case MigrationRecordFaultFile:
			if u.FileName == name {
				return fmt.Errorf("refusing to write migration record %q over a file this build could not read: %s",
					key, u.Reason)
			}
		}
	}
	return nil
}

func (s *MigrationRecordStore) Get(key MigrationRecordKey) (MigrationRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[key]
	return rec, ok
}

// Records snapshots every understood record in a fixed order: ascending task
// version, then strategy code. Retirement does not need that order to be
// correct, but a fixed one makes two passes over the same store agree.
func (s *MigrationRecordStore) Records() []MigrationRecord {
	s.mu.RLock()
	out := make([]MigrationRecord, 0, len(s.records))
	for _, rec := range s.records {
		out = append(out, rec)
	}
	s.mu.RUnlock()

	slices.SortFunc(out, func(a, b MigrationRecord) int {
		ak, bk := a.Subject().Key, b.Subject().Key
		if c := cmp.Compare(ak.TaskVersion, bk.TaskVersion); c != 0 {
			return c
		}
		return cmp.Compare(ak.StrategyCode, bk.StrategyCode)
	})
	return out
}

func (s *MigrationRecordStore) Unreadable() []MigrationRecordUnreadable {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.unreadable)
}

// maxMigrationRecordBytes bounds what [loadMigrationRecordFile] reads. Every
// Load runs it, and a Load sits inside the RAFT apply of a property DELETE,
// which holds the FSM loop cluster-wide — the same argument
// [maxRecoveryPayloadBytes] makes about payload.mig, whose read is the other
// half of the same function.
//
// The ceiling it has to clear is a record at [maxReindexPropertiesPerTask]
// properties. Each contributes its own name plus an entry in the staged,
// canonical and displaced directory maps, plus a sidecar and a flipped entry.
// A property name is at most 231 characters and a directory handle is a
// strategy prefix plus one of those, so a property costs well under 4 KiB of
// JSON and 1024 of them under 4 MiB. The bound is twice that: refusing a
// legitimate record freezes migrations on the shard, which is worse than
// reading a large file.
const maxMigrationRecordBytes = 8 << 20

func loadMigrationRecordFile(path string) (MigrationRecord, MigrationRecordLoadOutcome, error) {
	// Stat before read: an over-bound file is refused rather than parsed, and
	// refused reads as not-understood rather than absent, because a record
	// nobody read may name directories that must not be reclaimed.
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
		// An unreadable file is not an absent one: it may name directories
		// that must not be reclaimed.
		return nil, MigrationRecordNotUnderstood, fmt.Errorf("read %q: %w", filepath.Base(path), err)
	}

	rec, err := decodeMigrationRecord(data)
	if err != nil {
		return nil, MigrationRecordNotUnderstood, err
	}
	return rec, MigrationRecordLoaded, nil
}

// writeFileAtomic publishes content under name by renaming a fully written
// temp file over it, so a crash can only leave the previous file or none,
// never a truncated one.
func writeFileAtomic(dir, name string, content []byte) (err error) {
	// Same directory as the target, or the rename would cross filesystems.
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
	// The bytes have to reach the disk before the name does, or a machine
	// crash can publish the new name over content that never landed.
	if err = tmp.Sync(); err != nil {
		return err
	}
	// Close reports write errors the Write above can still be holding.
	if err = tmp.Close(); err != nil {
		return err
	}
	// The rename is itself a directory entry, and survives a machine crash
	// only once the directory holding it is synced.
	return diskio.RenameAndSync(tmpPath, filepath.Join(dir, name))
}
