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
	"github.com/sirupsen/logrus"
	entcfg "github.com/weaviate/weaviate/entities/config"
)

// -----------------------------------------------------------------------------
// Reindex tracker interface and file-based implementation
// -----------------------------------------------------------------------------

type reindexTracker interface {
	HasStartCondition() bool
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
	unmarkSwapped() error
	IsSwappedProp(propName string) bool
	markSwappedProp(propName string) error
	unmarkSwappedProp(propName string) error

	IsTidied() bool
	markTidied() error

	HasProps() bool
	GetProps() ([]string, error)
	saveProps([]string) error

	IsPaused() bool
	IsRollback() bool
	IsReset() bool

	reset() error

	checkOverrides(logger logrus.FieldLogger, config *reindexTaskConfig)
}

// NewFileReindexTracker creates a file-based reindex tracker under
// <lsmPath>/.migrations/<migrationDirName>/
func NewFileReindexTracker(lsmPath, migrationDirName string, keyParser indexKeyParser) *fileReindexTracker {
	return &fileReindexTracker{
		progressCheckpoint: 1,
		keyParser:          keyParser,
		config: fileReindexTrackerConfig{
			filenameStart:      "start.mig",
			filenameStarted:    "started.mig",
			filenameProgress:   "progress.mig",
			filenameReindexed:  "reindexed.mig",
			filenamePrepended:  "prepended.mig",
			filenameMerged:     "merged.mig",
			filenameSwapped:    "swapped.mig",
			filenameTidied:     "tidied.mig",
			filenameProperties: "properties.mig",
			filenameRollback:   "rollback.mig",
			filenameReset:      "reset.mig",
			filenamePaused:     "paused.mig",
			filenameOverrides:  "overrides.mig",
			migrationPath:      filepath.Join(lsmPath, ".migrations", migrationDirName),
		},
	}
}

type fileReindexTracker struct {
	progressCheckpoint int
	keyParser          indexKeyParser
	config             fileReindexTrackerConfig
}

type fileReindexTrackerConfig struct {
	filenameStart      string
	filenameStarted    string
	filenameProgress   string
	filenameReindexed  string
	filenamePrepended  string
	filenameMerged     string
	filenameSwapped    string
	filenameTidied     string
	filenameProperties string
	filenameRollback   string
	filenameReset      string
	filenamePaused     string
	filenameOverrides  string
	migrationPath      string
}

func (t *fileReindexTracker) init() error {
	if err := os.MkdirAll(t.config.migrationPath, 0o777); err != nil {
		return err
	}
	return nil
}

func (t *fileReindexTracker) HasStartCondition() bool {
	return t.fileExists(t.config.filenameStart)
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

func (t *fileReindexTracker) parseProgressFile(filename string) (lastProcessedKey indexKey, tm time.Time, allCount int, idxCount int, err error) {
	progressFilePath := filename
	progressFile, err := os.ReadFile(progressFilePath)
	if err != nil {
		err = fmt.Errorf("failed to read %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	if len(progressFile) == 0 {
		err = fmt.Errorf("progress file %s is empty", progressFilePath)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	progressFileFields := strings.Split(string(progressFile), "\n")
	if len(progressFileFields) != 4 {
		err = fmt.Errorf("progress file %s has unexpected format, expected 4 lines, got %d", progressFilePath, len(progressFileFields))
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	tm, err = t.decodeTime(strings.TrimSpace(progressFileFields[0]))
	if err != nil {
		err = fmt.Errorf("failed to parse timestamp from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	lastProcessedKey, err = t.keyParser.FromString(progressFileFields[1])
	if err != nil {
		err = fmt.Errorf("failed to parse last processed key from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	allCount, err = strconv.Atoi(strings.Split(progressFileFields[2], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse objects migrated count from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	idxCount, err = strconv.Atoi(strings.Split(progressFileFields[3], " ")[1])
	if err != nil {
		err = fmt.Errorf("failed to parse index count from %s: %w", progressFilePath, err)
		return lastProcessedKey, tm, allCount, idxCount, err
	}

	return lastProcessedKey, tm, allCount, idxCount, err
}

func (t *fileReindexTracker) GetMigratedCount() (objectsMigratedCountTotal int, snapshots []map[string]string, err error) {
	snapshots = make([]map[string]string, 0)
	files, err := os.ReadDir(t.config.migrationPath)
	objectsMigratedCountTotal = 0
	progressCount := 0

	if err != nil {
		return objectsMigratedCountTotal, snapshots, err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "progress.mig.") {
			snapshot := map[string]string{
				"checkpoint": strings.TrimPrefix(file.Name(), "progress.mig."),
			}
			progressCount++
			progressFilePath := t.config.migrationPath + "/" + file.Name()
			key, tm, allCount, idxCount, err2 := t.parseProgressFile(progressFilePath)
			if err2 != nil {
				err = fmt.Errorf("failed to parse progress file %s: %w", progressFilePath, err2)
				return objectsMigratedCountTotal, snapshots, err
			}

			objectsMigratedCountTotal += allCount
			snapshot["lastProcessedKey"] = key.String()
			snapshot["timestamp"] = tm.Format(time.RFC3339)
			snapshot["allCount"] = fmt.Sprintf("%d", allCount)
			snapshot["idxCount"] = fmt.Sprintf("%d", idxCount)
			snapshots = append(snapshots, snapshot)
		}
	}
	return objectsMigratedCountTotal, snapshots, err
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
func (t *fileReindexTracker) clearProgressFiles() error {
	prefix := t.config.filenameProgress + "."
	entries, err := os.ReadDir(t.config.migrationPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		if err := t.removeFile(e.Name()); err != nil {
			return err
		}
	}
	t.progressCheckpoint = 1
	return nil
}

func (t *fileReindexTracker) getReindexed() (time.Time, error) {
	return t.getTime(t.config.filenameReindexed)
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

func (t *fileReindexTracker) getMerged() (time.Time, error) {
	return t.getTime(t.config.filenameMerged)
}

func (t *fileReindexTracker) IsSwappedProp(propName string) bool {
	return t.fileExists(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) markSwappedProp(propName string) error {
	return t.createFile(t.config.filenameSwapped+"."+propName, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) unmarkSwappedProp(propName string) error {
	return t.removeFile(t.config.filenameSwapped + "." + propName)
}

func (t *fileReindexTracker) IsSwapped() bool {
	return t.fileExists(t.config.filenameSwapped)
}

func (t *fileReindexTracker) markSwapped() error {
	return t.createFile(t.config.filenameSwapped, []byte(t.encodeTimeNow()))
}

func (t *fileReindexTracker) unmarkSwapped() error {
	return t.removeFile(t.config.filenameSwapped)
}

func (t *fileReindexTracker) getSwapped() (time.Time, error) {
	return t.getTime(t.config.filenameSwapped)
}

func (t *fileReindexTracker) IsTidied() bool {
	return t.fileExists(t.config.filenameTidied)
}

func (t *fileReindexTracker) getTidied() (time.Time, error) {
	return t.getTime(t.config.filenameTidied)
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

func (t *fileReindexTracker) HasProps() bool {
	return t.fileExists(t.config.filenameProperties)
}

func (t *fileReindexTracker) saveProps(propNames []string) error {
	props := []byte(strings.Join(propNames, ","))
	return t.createFile(t.config.filenameProperties, props)
}

func (t *fileReindexTracker) GetProps() ([]string, error) {
	content, err := os.ReadFile(t.filepath(t.config.filenameProperties))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return []string{}, nil
	}
	return strings.Split(strings.TrimSpace(string(content)), ","), nil
}

func (t *fileReindexTracker) IsReset() bool {
	return t.fileExists(t.config.filenameReset)
}

func (t *fileReindexTracker) reset() error {
	return os.RemoveAll(t.config.migrationPath)
}

func (t *fileReindexTracker) IsRollback() bool {
	return t.fileExists(t.config.filenameRollback)
}

func (t *fileReindexTracker) IsPaused() bool {
	return t.fileExists(t.config.filenamePaused)
}

func (t *fileReindexTracker) GetStatusStrings() (status string, message string, action string) {
	if !t.IsStarted() {
		status = "not started"
		message = "reindexing not started"
		action = "use PUT /v1/schema/{collection}/indexes/{property} API to trigger reindex"
		if t.HasStartCondition() {
			message = "reindexing will start on next restart"
			action = "restart"
		}
		return status, message, action
	}
	message = "reindexing started"
	action = "wait"

	if !t.HasProps() {
		status = "computing properties"
		message = "computing properties to reindex"
		return status, message, action
	}

	count, _, err := t.GetMigratedCount()
	if err != nil {
		status = "error"
		message = fmt.Sprintf("failed to get migrated count: %v", err)
		return status, message, action
	}

	status = "in progress"

	if count == 0 {
		message = "reindexing just started, no snapshots yet"
	}

	if t.IsReindexed() {
		status = "reindexed"
		message = "reindexing done, needs restart to merge buckets"
		action = "restart"
	}

	if t.IsPrepended() {
		status = "prepended"
		message = "reindexing done, segments prepended at runtime"
		action = "wait"
	}

	if t.IsMerged() {
		status = "merged"
		message = "reindexing done, buckets merged"
		action = "restart"
	}

	if t.IsSwapped() {
		status = "swapped"
		message = "reindexing done, buckets swapped"
		action = "restart"
	}

	if t.IsPaused() {
		status = "paused"
		message = "reindexing paused, needs resume or rollback"
		action = "resume or rollback"
	}

	if t.IsRollback() {
		status = "rollback"
		message = "reindexing rollback in progress, will finish on next restart"
		action = "restart"
	}

	if t.IsTidied() {
		status = "tidied"
		message = "reindexing done, buckets tidied"
		action = "nothing to do"
	}

	return status, message, action
}

func (t *fileReindexTracker) GetTimes() map[string]string {
	times := map[string]string{}

	started, err := t.getStarted()
	if err != nil {
		times["started"] = ""
	} else {
		times["started"] = t.encodeTime(started)
	}
	_, tm, _ := t.GetProgress()
	if tm == nil {
		times["reindexSnapshot"] = ""
	} else {
		times["reindexSnapshot"] = t.encodeTime(*tm)
	}

	reindexed, err := t.getReindexed()
	if err != nil {
		times["reindexFinished"] = ""
	} else {
		times["reindexFinished"] = t.encodeTime(reindexed)
	}
	merged, err := t.getMerged()
	if err != nil {
		times["merged"] = ""
	} else {
		times["merged"] = t.encodeTime(merged)
	}

	swapped, err := t.getSwapped()
	if err != nil {
		times["swapped"] = ""
	} else {
		times["swapped"] = t.encodeTime(swapped)
	}

	tidied, err := t.getTidied()
	if err != nil {
		times["tidied"] = ""
	} else {
		times["tidied"] = t.encodeTime(tidied)
	}

	return times
}

func (t *fileReindexTracker) checkOverrides(logger logrus.FieldLogger, config *reindexTaskConfig) {
	if !t.fileExists(t.config.filenameOverrides) {
		return
	}
	if config == nil {
		return
	}
	content, err := os.ReadFile(t.filepath(t.config.filenameOverrides))
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) == 0 {
		return
	}

	for _, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			logger.WithField("line", line).Warn("invalid override line, expected 'key=value'")
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		logger.WithFields(logrus.Fields{
			"key":   key,
			"value": value,
		}).Info("processing override")

		switch key {
		case "swapBuckets":
			config.swapBuckets = entcfg.Enabled(value)
		case "unswapBuckets":
			config.unswapBuckets = entcfg.Enabled(value)
		case "tidyBuckets":
			config.tidyBuckets = entcfg.Enabled(value)
		case "rollback":
			config.rollback = entcfg.Enabled(value)
		case "conditionalStart":
			config.conditionalStart = entcfg.Enabled(value)
		case "concurrency":
			if n, ok := parsePositiveInt(logger, "concurrency", value); ok {
				config.concurrency = n
			}
		case "memtableOptBlockmaxFactor", "memtableOptFactor":
			if n, ok := parsePositiveInt(logger, "memtableOptFactor", value); ok {
				config.memtableOptFactor = n
			}
		case "processingDuration":
			if d, ok := parsePositiveDuration(logger, "processingDuration", value, false); ok {
				config.processingDuration = d
			}
		case "pauseDuration":
			if d, ok := parsePositiveDuration(logger, "pauseDuration", value, false); ok {
				config.pauseDuration = d
			}
		case "perObjectDelay":
			if d, ok := parsePositiveDuration(logger, "perObjectDelay", value, true); ok {
				config.perObjectDelay = d
			}
		case "checkProcessingEveryNoObjects":
			if n, ok := parsePositiveInt(logger, "checkProcessingEveryNoObjects", value); ok {
				config.checkProcessingEveryNoObjects = n
			}
		default:
			logger.WithField("key", key).Warnf("unknown override key, ignoring: %s", key)
			continue
		}
	}

	logger.WithField("config", fmt.Sprintf("%+v", config)).Debug("reindex config overrides applied")
}

// parsePositiveInt parses a positive (>0) integer override. Logs a warning
// and returns ok=false if value cannot be parsed or is not positive.
func parsePositiveInt(logger logrus.FieldLogger, key, value string) (int, bool) {
	n, err := strconv.Atoi(value)
	if err != nil {
		logger.WithField("value", value).Warnf("invalid %s value, must be an integer", key)
		return 0, false
	}
	if n <= 0 {
		logger.WithField("value", value).Warnf("invalid %s value, must be greater than 0", key)
		return 0, false
	}
	return n, true
}

// parsePositiveDuration parses a duration override. If allowZero is false the
// value must be > 0; if allowZero is true it must be >= 0. Logs a warning and
// returns ok=false on parse failure or constraint violation.
func parsePositiveDuration(logger logrus.FieldLogger, key, value string, allowZero bool) (time.Duration, bool) {
	d, err := time.ParseDuration(value)
	if err != nil {
		logger.WithField("value", value).Warnf("invalid %s value: %v", key, err)
		return 0, false
	}
	if allowZero {
		if d < 0 {
			logger.WithField("value", value).Warnf("invalid %s value, must be greater than or equal to 0", key)
			return 0, false
		}
	} else if d <= 0 {
		logger.WithField("value", value).Warnf("invalid %s value, must be greater than 0", key)
		return 0, false
	}
	return d, true
}
