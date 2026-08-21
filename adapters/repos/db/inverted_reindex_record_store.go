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

// MigrationRecordUnreadable is a file in the records directory that this build
// could not place. It is kept by file name because the key it would have had
// is exactly what could not be read.
type MigrationRecordUnreadable struct {
	FileName string
	Reason   string
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

	// The file this would land on may be one this build could not read. That
	// file is the artifact the freeze exists to preserve, and what would
	// replace it is a guess about the migration it describes.
	if reason, blocked := s.unreadableAt(rec.Subject().Key); blocked {
		return fmt.Errorf("refusing to write migration record %q over a file this build could not read: %s",
			rec.Subject().Key, reason)
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

	if err := os.Remove(s.path(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove migration record %q: %w", key, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

// unreadableAt reports whether the file key would be written to is one the
// last load could not place.
func (s *MigrationRecordStore) unreadableAt(key MigrationRecordKey) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name := key.fileName()
	for _, u := range s.unreadable {
		if u.FileName == name {
			return u.Reason, true
		}
	}
	return "", false
}

func (s *MigrationRecordStore) Get(key MigrationRecordKey) (MigrationRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[key]
	return rec, ok
}

// Records snapshots every understood record in ascending generation order,
// which is the order the supersession retirement has to run in.
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

func loadMigrationRecordFile(path string) (MigrationRecord, MigrationRecordLoadOutcome, error) {
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
	if err = os.Rename(tmpPath, filepath.Join(dir, name)); err != nil {
		return err
	}
	// The rename is itself a directory entry, and survives a machine crash
	// only once the directory holding it is synced.
	return diskio.Fsync(dir)
}
