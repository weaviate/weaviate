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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

// -----------------------------------------------------------------------------
// Reindex tracker interface and file-based implementation
// -----------------------------------------------------------------------------

type reindexTracker interface {
	IsStarted() bool
	markStarted(time.Time) error
	getStarted() (time.Time, error)

	markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error
	GetProgress() (indexKey, *time.Time, error)

	IsReindexed() bool
	markReindexed() error
	unmarkReindexed() error

	IsPrepended() bool
	markPrepended() error

	IsMerged() bool
	markMerged() error

	IsSwapped() bool
	markSwapped() error
	IsSwappedProp(propName string) bool
	markSwappedProp(propName string) error

	IsTidied() bool
	markTidied() error

	HasProps() bool
	GetProps() ([]string, error)
	saveProps([]string) error
}

// NewFileReindexTracker creates a file-based reindex tracker under
// <lsmPath>/.migrations/<migrationDirName>/
func NewFileReindexTracker(lsmPath, migrationDirName string, keyParser indexKeyParser) *fileReindexTracker {
	return &fileReindexTracker{
		progressCheckpoint: 1,
		keyParser:          keyParser,
		config: fileReindexTrackerConfig{
			filenameStarted:    "started.mig",
			filenameProgress:   "progress.mig",
			filenameReindexed:  "reindexed.mig",
			filenamePrepended:  "prepended.mig",
			filenameMerged:     "merged.mig",
			filenameSwapped:    "swapped.mig",
			filenameTidied:     "tidied.mig",
			filenameProperties: "properties.mig",
			migrationPath:      filepath.Join(lsmPath, ".migrations", migrationDirName),
		},
	}
}

type fileReindexTracker struct {
	progressCheckpoint int
	keyParser          indexKeyParser
	config             fileReindexTrackerConfig

	// mkdirGuard, when non-nil (expected: Index.withCloseRLockGuard), wraps
	// init()'s MkdirAll so it cannot re-create a class dir Index.drop just
	// renamed away; context.Canceled means the index is closing.
	mkdirGuard func(func() error) error
}

type fileReindexTrackerConfig struct {
	filenameStarted    string
	filenameProgress   string
	filenameReindexed  string
	filenamePrepended  string
	filenameMerged     string
	filenameSwapped    string
	filenameTidied     string
	filenameProperties string
	migrationPath      string
}

func (t *fileReindexTracker) init() error {
	mkdir := func() error {
		return os.MkdirAll(t.config.migrationPath, 0o777)
	}

	if t.mkdirGuard != nil {
		return t.mkdirGuard(mkdir)
	}
	return mkdir()
}

func (t *fileReindexTracker) IsStarted() bool {
	return t.fileExists(t.config.filenameStarted)
}

func (t *fileReindexTracker) markStarted(started time.Time) error {
	return t.createFile(t.config.filenameStarted, []byte(t.encodeTime(started)))
}

func (t *fileReindexTracker) getTime(filePath string) (time.Time, error) {
	path := t.filepath(filePath)
	content, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	return t.decodeTime(string(content))
}

func (t *fileReindexTracker) getStarted() (time.Time, error) {
	return t.getTime(t.config.filenameStarted)
}

func (t *fileReindexTracker) findLastProgressFile() (string, error) {
	prefix := t.config.filenameProgress + "."
	expectedLen := len(prefix) + 9 // 9 digits

	lastProgressFilename := ""
	err := filepath.WalkDir(t.config.migrationPath, func(path string, d os.DirEntry, err error) error {
		// skip parent and children dirs
		if path != t.config.migrationPath {
			if d.IsDir() {
				return filepath.SkipDir
			}
			if name := d.Name(); len(name) == expectedLen && strings.HasPrefix(name, prefix) {
				lastProgressFilename = name
			}
		}
		return nil
	})

	return lastProgressFilename, err
}

func (t *fileReindexTracker) markProgress(lastProcessedKey indexKey, processedCount, indexedCount int) error {
	filename := fmt.Sprintf("%s.%09d", t.config.filenameProgress, t.progressCheckpoint)
	content := strings.Join([]string{
		t.encodeTime(time.Now()),
		lastProcessedKey.String(),
		fmt.Sprintf("all %d", processedCount),
		fmt.Sprintf("idx %d", indexedCount),
	}, "\n")

	if err := t.createFile(filename, []byte(content)); err != nil {
		return err
	}
	t.progressCheckpoint++
	return nil
}

func (t *fileReindexTracker) GetProgress() (indexKey, *time.Time, error) {
	filename, err := t.findLastProgressFile()
	if err != nil {
		return nil, nil, err
	}
	if filename == "" {
		return t.keyParser.FromBytes(nil), nil, nil
	}

	checkpoint, err := strconv.Atoi(strings.TrimPrefix(filename, t.config.filenameProgress+"."))
	if err != nil {
		return nil, nil, err
	}

	path := t.filepath(filename)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	split := strings.Split(string(content), "\n")
	key, err := t.keyParser.FromString(split[1])
	if err != nil {
		return nil, nil, err
	}

	timeStr := strings.TrimSpace(split[0])
	if timeStr == "" {
		return key, nil, fmt.Errorf("progress file '%s' is empty", filename)
	}

	tm, err := t.decodeTime(timeStr)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding time from '%s': %w", timeStr, err)
	}

	t.progressCheckpoint = checkpoint + 1
	return key, &tm, nil
}

func (t *fileReindexTracker) IsReindexed() bool {
	return t.fileExists(t.config.filenameReindexed)
}

func (t *fileReindexTracker) markReindexed() error {
	return t.createFile(t.config.filenameReindexed, []byte(t.encodeTimeNow()))
}

// unmarkReindexed deletes the reindexed.mig sentinel AND every
// progress.mig.<N> checkpoint. Called by the torn-state recovery in
// [ShardReindexTaskGeneric.OnAfterLsmInit] when IsReindexed=true but
// the reindex bucket dirs are missing on disk. Clearing the progress
// checkpoints is what makes "unmark = redo from scratch" actually
// hold — without it, the resumed iteration reads the stale
// lastProcessedKey from disk and silently skips every object <= that
// key. weaviate/0-weaviate-issues#244.
func (t *fileReindexTracker) unmarkReindexed() error {
	if err := t.removeFile(t.config.filenameReindexed); err != nil {
		return err
	}
	return t.clearProgressFiles()
}

// clearProgressFiles removes every progress.mig.<N> checkpoint and
// resets the in-memory checkpoint counter. Used by unmarkReindexed to
// keep the "next iteration runs from scratch" invariant.
//
// MUST NOT run concurrently with any markProgress emitter. Today this
// holds because only the torn-state guard in OnAfterLsmInit calls it,
// and that runs before the async reindex loop spawns.
func (t *fileReindexTracker) clearProgressFiles() error {
	prefix := t.config.filenameProgress + "."
	expectedLen := len(prefix) + 9 // matches findLastProgressFile
	entries, err := os.ReadDir(t.config.migrationPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) != expectedLen || !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := t.removeFile(name); err != nil {
			return err
		}
	}
	t.progressCheckpoint = 1
	return nil
}

func (t *fileReindexTracker) IsPrepended() bool {
	return t.fileExists(t.config.filenamePrepended)
}

func (t *fileReindexTracker) markPrepended() error {
	return t.createFile(t.config.filenamePrepended, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) IsMerged() bool {
	return t.fileExists(t.config.filenameMerged)
}

func (t *fileReindexTracker) markMerged() error {
	return t.createFile(t.config.filenameMerged, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) IsSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) IsSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) IsTidied() bool {
	return t.fileExists(t.config.filenameTidied)
}

func (t *fileReindexTracker) markTidied() error {
	return t.createFile(t.config.filenameTidied, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) filepath(filename string) string {
	return filepath.Join(t.config.migrationPath, filename)
}

func (t *fileReindexTracker) fileExists(filename string) bool {
	_, err := os.Stat(t.filepath(filename))
	return err == nil
}

func (t *fileReindexTracker) createFile(filename string, content []byte) error {
	path := t.filepath(filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o777)
	if err != nil {
		return err
	}
	defer file.Close()

	if len(content) > 0 {
		_, err = file.Write(content)
		return err
	}
	return nil
}

// createFileAtomic publishes content under filename by renaming a fully
// written temp file over it, so a crash can only leave the previous file or
// none — never a truncated one. Unlike [fileReindexTracker.createFile] it
// overwrites, which is what lets a caller repair a torn file. Use it wherever
// a reader keys off the file's content rather than its mere existence.
func (t *fileReindexTracker) createFileAtomic(filename string, content []byte) (err error) {
	// Same directory as the target, or the rename would cross filesystems.
	// Nothing sweeps a temp file a crash leaves behind, so it stays out of
	// backups by its .tmp extension alone — every walk that reaches this
	// directory has to skip that extension.
	tmp, err := os.CreateTemp(t.config.migrationPath, filename+".*.tmp")
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
	if err = os.Rename(tmpPath, t.filepath(filename)); err != nil {
		return err
	}
	// The rename is itself a directory entry, and survives a machine crash
	// only once the directory holding it is synced.
	return diskio.Fsync(t.config.migrationPath)
}

func (t *fileReindexTracker) removeFile(filename string) error {
	if err := os.Remove(t.filepath(filename)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (t *fileReindexTracker) encodeTimeNow() string {
	return t.encodeTime(time.Now())
}

func (t *fileReindexTracker) encodeTime(tm time.Time) string {
	return tm.UTC().Format(time.RFC3339Nano)
}

func (t *fileReindexTracker) decodeTime(tm string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, tm)
}

// HasProps sizes the file rather than merely stating it: a zero-byte
// properties.mig is a torn write, and reporting it as a recorded list would
// retire the shard's reindex with nothing left to repair it.
func (t *fileReindexTracker) HasProps() bool {
	info, err := os.Stat(t.filepath(t.config.filenameProperties))
	return err == nil && info.Size() > 0
}

func (t *fileReindexTracker) saveProps(propNames []string) error {
	if len(propNames) == 0 {
		// Nothing to record, and a zero-byte file is reserved for the torn
		// write [fileReindexTracker.HasProps] has to reject. A shard with no
		// reindexable property rediscovers that from its buckets instead.
		return nil
	}
	props := []byte(strings.Join(propNames, ","))
	return t.createFileAtomic(t.config.filenameProperties, props)
}

func (t *fileReindexTracker) GetProps() ([]string, error) {
	content, err := os.ReadFile(t.filepath(t.config.filenameProperties))
	if err != nil {
		return nil, err
	}
	// Trim before the split, as readMigrationProps does: content that is all
	// whitespace names no property, and splitting it manufactures one whose
	// name is empty.
	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return []string{}, nil
	}
	return strings.Split(trimmed, ","), nil
}
