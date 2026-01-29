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

package compactv2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// migrationSentinelFile is the marker file indicating migration is complete.
	migrationSentinelFile = ".compactv2.migrated"

	// migrationStateFile tracks migration progress for crash recovery.
	migrationStateFile = ".compactv2.migration.state"

	// migratingFileSuffix is used during file moves for crash safety.
	migratingFileSuffix = ".migrating"

	// oldSnapshotDirSuffix is the old directory suffix for snapshots.
	oldSnapshotDirSuffix = ".hnsw.snapshot.d"

	// commitlogDirSuffix is the new unified directory suffix for all files.
	commitlogDirSuffix = ".hnsw.commitlog.d"
)

// MigrationState tracks progress for crash recovery during migration.
type MigrationState struct {
	StartedAt          time.Time `json:"started_at"`
	CondensedConverted []string  `json:"condensed_converted"` // Paths already converted
	CondensedPending   []string  `json:"condensed_pending"`   // Paths to convert
}

// Migrator handles migration from legacy formats to compactv2.
// It supports:
// 1. Moving snapshots from old .hnsw.snapshot.d/ directory to .hnsw.commitlog.d/
// 2. Converting .condensed files to .sorted format during compaction
// 3. Tracking migration state with sentinel file
type Migrator struct {
	dir    string
	logger logrus.FieldLogger
}

// NewMigrator creates a new migrator for the given commitlog directory.
func NewMigrator(dir string, logger logrus.FieldLogger) *Migrator {
	return &Migrator{
		dir:    dir,
		logger: logger,
	}
}

// IsMigrationComplete checks if the sentinel file exists indicating migration is done.
func (m *Migrator) IsMigrationComplete() bool {
	sentinelPath := filepath.Join(m.dir, migrationSentinelFile)
	_, err := os.Stat(sentinelPath)
	return err == nil
}

// NeedsMigration checks if there are .condensed files that need conversion
// and migration has not been completed.
func (m *Migrator) NeedsMigration() (bool, error) {
	if m.IsMigrationComplete() {
		return false, nil
	}

	// Check for .condensed files
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "read directory")
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), suffixCondensed) {
			return true, nil
		}
	}

	return false, nil
}

// LoadState loads existing migration state from the state file.
// Returns nil if no state file exists.
func (m *Migrator) LoadState() (*MigrationState, error) {
	statePath := filepath.Join(m.dir, migrationStateFile)
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "read state file")
	}

	var state MigrationState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, errors.Wrap(err, "unmarshal state")
	}

	return &state, nil
}

// SaveState persists migration state atomically (temp file + rename).
func (m *Migrator) SaveState(state *MigrationState) error {
	statePath := filepath.Join(m.dir, migrationStateFile)
	tmpPath := statePath + tempFileSuffix

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal state")
	}

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return errors.Wrap(err, "write temp state file")
	}

	if err := os.Rename(tmpPath, statePath); err != nil {
		os.Remove(tmpPath)
		return errors.Wrap(err, "rename state file")
	}

	return nil
}

// MarkMigrationComplete creates the sentinel file and removes state file.
func (m *Migrator) MarkMigrationComplete() error {
	// Create sentinel file
	sentinelPath := filepath.Join(m.dir, migrationSentinelFile)
	if err := os.WriteFile(sentinelPath, []byte("migrated"), 0o644); err != nil {
		return errors.Wrap(err, "create sentinel file")
	}

	// Remove state file (best effort)
	statePath := filepath.Join(m.dir, migrationStateFile)
	os.Remove(statePath)

	return nil
}

// MigrateSnapshotDirectory moves snapshots from old directory to commitlog directory.
// Returns error if V1/V2 snapshot detected.
// This MUST be called before Loader.Load() to ensure snapshots are found.
func (m *Migrator) MigrateSnapshotDirectory() error {
	// 1. Derive old snapshot directory path from commitlog dir
	//    e.g., .../main.hnsw.commitlog.d/ -> .../main.hnsw.snapshot.d/
	if !strings.HasSuffix(m.dir, commitlogDirSuffix) {
		// Directory doesn't follow expected naming pattern, skip
		return nil
	}

	oldSnapshotDir := strings.TrimSuffix(m.dir, commitlogDirSuffix) + oldSnapshotDirSuffix

	// 2. Check if old directory exists
	entries, err := os.ReadDir(oldSnapshotDir)
	if os.IsNotExist(err) {
		return nil // No old directory, nothing to migrate
	}
	if err != nil {
		return errors.Wrap(err, "read old snapshot directory")
	}

	// 3. Find .snapshot files (ignore .checkpoints)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), suffixSnapshot) {
			continue
		}

		oldPath := filepath.Join(oldSnapshotDir, entry.Name())

		// 4. Check snapshot version
		version, err := m.detectSnapshotVersion(oldPath)
		if err != nil {
			return errors.Wrapf(err, "detect version of %s", oldPath)
		}

		if version < snapshotVersionV3 {
			return fmt.Errorf("legacy snapshot version %d detected at %s; "+
				"upgrade not supported. Please downgrade Weaviate and either: "+
				"(1) delete the snapshot file and let it regenerate, or "+
				"(2) run a compaction cycle to create a V3 snapshot before upgrading",
				version, oldPath)
		}

		// 5. Move V3 snapshot to new location (atomic on same filesystem)
		newPath := filepath.Join(m.dir, entry.Name())
		if err := m.atomicMoveFile(oldPath, newPath); err != nil {
			return errors.Wrapf(err, "move snapshot %s to %s", oldPath, newPath)
		}

		m.logger.WithFields(logrus.Fields{
			"action":   "hnsw_migrator",
			"old_path": oldPath,
			"new_path": newPath,
		}).Info("migrated snapshot to commitlog directory")
	}

	// 6. Delete .checkpoints files (not needed for V3)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".checkpoints") {
			oldPath := filepath.Join(oldSnapshotDir, entry.Name())
			os.Remove(oldPath) // Best effort, ignore errors
		}
	}

	// 7. Try to remove old directory (will fail if not empty, that's OK)
	os.Remove(oldSnapshotDir)

	return nil
}

// detectSnapshotVersion reads the first byte of a snapshot file to get its version.
func (m *Migrator) detectSnapshotVersion(path string) (uint8, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, errors.Wrap(err, "open snapshot file")
	}
	defer f.Close()

	var version [1]byte
	if _, err := io.ReadFull(f, version[:]); err != nil {
		return 0, errors.Wrap(err, "read version byte")
	}

	return version[0], nil
}

// atomicMoveFile moves a file atomically if on same filesystem,
// or falls back to copy+delete with crash safety.
func (m *Migrator) atomicMoveFile(src, dst string) error {
	// Try rename first (atomic on same filesystem)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	// Fall back to copy + delete
	// 1. Copy to temp file in destination directory
	tmpDst := dst + migratingFileSuffix
	if err := copyFile(src, tmpDst); err != nil {
		return errors.Wrap(err, "copy file")
	}

	// 2. Atomic rename temp to final
	if err := os.Rename(tmpDst, dst); err != nil {
		os.Remove(tmpDst) // Cleanup on failure
		return errors.Wrap(err, "rename temp file")
	}

	// 3. Delete original
	return os.Remove(src)
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return errors.Wrap(err, "open source")
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return errors.Wrap(err, "create destination")
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return errors.Wrap(err, "copy data")
	}

	return dstFile.Sync()
}

// Migrate converts .condensed files to .sorted format.
// Called during compaction, not startup.
// Uses state file for crash recovery.
func (m *Migrator) Migrate(ctx context.Context) error {
	// 1. Load or create migration state
	state, err := m.LoadState()
	if err != nil {
		return errors.Wrap(err, "load migration state")
	}

	if state == nil {
		// First time running migration - discover all .condensed files
		state = &MigrationState{
			StartedAt:          time.Now(),
			CondensedConverted: make([]string, 0),
			CondensedPending:   make([]string, 0),
		}

		entries, err := os.ReadDir(m.dir)
		if err != nil {
			return errors.Wrap(err, "read directory for condensed files")
		}

		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), suffixCondensed) {
				state.CondensedPending = append(state.CondensedPending, filepath.Join(m.dir, entry.Name()))
			}
		}

		if err := m.SaveState(state); err != nil {
			return errors.Wrap(err, "save initial migration state")
		}
	}

	// 2. Build set of already converted files for quick lookup
	convertedSet := make(map[string]struct{}, len(state.CondensedConverted))
	for _, path := range state.CondensedConverted {
		convertedSet[path] = struct{}{}
	}

	// 3. Process pending .condensed files
	for _, path := range state.CondensedPending {
		// Check context for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Skip if already converted
		if _, ok := convertedSet[path]; ok {
			continue
		}

		// Check if file still exists (may have been deleted)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			// Mark as converted (file is gone)
			state.CondensedConverted = append(state.CondensedConverted, path)
			if err := m.SaveState(state); err != nil {
				return errors.Wrap(err, "save state after skipping missing file")
			}
			continue
		}

		// Convert the file
		if err := m.convertCondensedToSorted(ctx, path); err != nil {
			return errors.Wrapf(err, "convert %s", path)
		}

		// Update state (mark as converted)
		state.CondensedConverted = append(state.CondensedConverted, path)
		if err := m.SaveState(state); err != nil {
			return errors.Wrap(err, "save state after conversion")
		}

		m.logger.WithFields(logrus.Fields{
			"action": "hnsw_migrator",
			"file":   filepath.Base(path),
		}).Info("converted condensed file to sorted format")
	}

	// 4. Create sentinel file
	if err := m.MarkMigrationComplete(); err != nil {
		return errors.Wrap(err, "mark migration complete")
	}

	m.logger.WithFields(logrus.Fields{
		"action":          "hnsw_migrator",
		"converted_count": len(state.CondensedConverted),
	}).Info("migration complete")

	return nil
}

// convertCondensedToSorted converts a single .condensed file to .sorted format.
func (m *Migrator) convertCondensedToSorted(ctx context.Context, path string) error {
	// Parse the filename to get timestamps
	baseName := filepath.Base(path)
	baseName = strings.TrimSuffix(baseName, suffixCondensed)

	// Parse timestamp from filename
	var startTS, endTS int64
	if idx := strings.Index(baseName, "_"); idx != -1 {
		// Range format: {start}_{end}
		if _, err := fmt.Sscanf(baseName, "%d_%d", &startTS, &endTS); err != nil {
			return errors.Wrapf(err, "parse timestamp from %s", path)
		}
	} else {
		// Single timestamp format
		if _, err := fmt.Sscanf(baseName, "%d", &startTS); err != nil {
			return errors.Wrapf(err, "parse timestamp from %s", path)
		}
		endTS = startTS
	}

	// Open source file
	srcFile, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open condensed file")
	}
	defer srcFile.Close()

	// Read into memory using WALCommitReader + InMemoryReader
	walReader := NewWALCommitReader(srcFile, m.logger)
	inMemReader := NewInMemoryReader(walReader, m.logger)
	result, err := inMemReader.Do(nil, true) // keepLinkReplaceInformation = true
	if err != nil {
		return errors.Wrap(err, "read condensed file into memory")
	}

	// Build output filename
	outFilename := BuildMergedFilename(startTS, endTS, FileTypeSorted)
	outPath := filepath.Join(m.dir, outFilename)

	// Write using SortedWriter via SafeFileWriter
	sfw, err := NewSafeFileWriter(outPath, DefaultBufferSize)
	if err != nil {
		return errors.Wrap(err, "create safe file writer")
	}
	defer sfw.Abort() // cleanup on error

	sortedWriter := NewSortedWriter(sfw.Writer(), m.logger)
	if err := sortedWriter.WriteAll(result); err != nil {
		return errors.Wrap(err, "write sorted file")
	}

	if err := sfw.Commit(); err != nil {
		return errors.Wrap(err, "commit sorted file")
	}

	// Delete original file after successful conversion
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "remove original condensed file")
	}

	return nil
}

// CleanupMigratingFiles removes any orphaned .migrating files in the directory.
// This should be called during startup to clean up from incomplete migrations.
func CleanupMigratingFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "read directory %s", dir)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasSuffix(name, migratingFileSuffix) {
			migPath := filepath.Join(dir, name)
			if err := os.Remove(migPath); err != nil && !os.IsNotExist(err) {
				return errors.Wrapf(err, "remove orphaned migrating file %s", migPath)
			}
		}
	}

	return nil
}
