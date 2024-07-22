//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/errors"
)

const (
	migrationDir = "migration"

	migrationFileInitedGlob   = "migration.inited.*.mig"
	migrationFileInitedRegex  = "migration.inited.([0-9]{18,})\\.mig"
	migrationFileInitedFormat = "migration.inited.%d.mig"
	migrationFileTodo         = "migration.todo.mig"
	migrationFileCompleted    = "migration.completed.mig"

	segmentFileTodoGlob   = "segment-*.db.todo.mig"
	segmentFileTodoRegex  = "segment-[0-9]{18,}\\.db\\.todo\\.mig"
	segmentFileTodoSuffix = ".todo.mig"

	segmentFileTempGlob        = "segment-*.db.*.temp"
	segmentFileTempRegex       = "segment-[0-9]{18,}\\.db\\.[0-9]{4,}(-[0-9]{4,})?\\.temp"
	segmentFileTempFormat      = "%s.%04d.temp"
	segmentFileTempMergeFormat = "%s.%04d-%04d.temp"

	segmentFileDbGlob  = "segment-*.db"
	segmentFileDbRegex = "segment-([0-9]{18,})\\.db"
)

var (
	regexMigrationFileInited = regexp.MustCompile(migrationFileInitedRegex)
	regexSegmentFileTodo     = regexp.MustCompile(segmentFileTodoRegex)
	regexSegmentFileTemp     = regexp.MustCompile(segmentFileTempRegex)
	regexSegmentFileDb       = regexp.MustCompile(segmentFileDbRegex)
)

type MigratorFilterableToRangeable struct {
	propName      string
	logger        logrus.FieldLogger
	routinesLimit int
	srcBucket     *Bucket
	dstBucket     *Bucket
}

func NewMigratorFilterableToRangeable(store *Store, propName string, routinesLimit int,
) (*MigratorFilterableToRangeable, error) {
	srcBucket := store.Bucket(helpers.BucketFromPropNameLSM(propName))
	if srcBucket == nil {
		return nil, fmt.Errorf("rangeable migration %q: filterable bucket not found", propName)
	}
	dstBucket := store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	if dstBucket == nil {
		return nil, fmt.Errorf("rangeable migration %q: rangeable bucket not found", propName)
	}

	return &MigratorFilterableToRangeable{
		propName: propName,
		logger: store.logger.
			WithField("action", "migrate_filterable_to_rangeable").
			WithField("property", propName),
		routinesLimit: routinesLimit,
		srcBucket:     srcBucket,
		dstBucket:     dstBucket,
	}, nil
}

func (m *MigratorFilterableToRangeable) Migrate(ctx context.Context) error {
	start := time.Now()
	m.logF(map[string]interface{}{"start": start.String()}, "migration started")

	var err error
	if err = ctx.Err(); err != nil {
		m.logErr(err)
		return m.errw(err)
	}

	if m.isCompleted() {
		m.log("migration ALREADY completed")
		return nil
	}

	timestamp, inited := m.isInited()
	if !inited {
		if m.anySegmentExists() {
			err = fmt.Errorf("migration not inited, but segments already exist. Bucket should be empty.")
			m.logErr(err)
			return m.errw(err)
		}
		timestamp, err = m.init()
		if err != nil {
			m.logErr(err)
			return m.errw(err)
		}
		m.logF(map[string]interface{}{"timestamp": timestamp},
			"migration marked as inited")
	} else {
		m.logF(map[string]interface{}{"timestamp": timestamp},
			"migration ALREADY marked as inited")
	}

	if err = ctx.Err(); err != nil {
		m.logErr(err)
		return m.errw(err)
	}

	if !m.isTodo() {
		filenames, err := m.cleanupTodoAndReady()
		if err != nil {
			m.logErr(err)
			return m.errw(err)
		}
		if len(filenames) > 0 {
			m.logF(map[string]interface{}{"files": filenames},
				"removed todo & ready files")
		}

		filenames, err = m.todo(timestamp)
		if err != nil {
			m.logErr(err)
			return m.errw(err)
		}
		m.logF(map[string]interface{}{"files": filenames},
			"migration marked as todo")

		if err = ctx.Err(); err != nil {
			m.logErr(err)
			return m.errw(err)
		}
	} else {
		m.log("migration ALREADY marked as todo")
	}

	filenames, err := m.cleanupTemp()
	if err != nil {
		m.logErr(err)
		return m.errw(err)
	}
	if len(filenames) > 0 {
		m.logF(map[string]interface{}{"files": filenames},
			"removed temp files")
	}

	if err = ctx.Err(); err != nil {
		m.logErr(err)
		return m.errw(err)
	}

	leftTodoNames, invalidReadyNames := m.notReady()
	if len(invalidReadyNames) > 0 {
		if err = m.cleanupReady(invalidReadyNames); err != nil {
			m.logErr(err)
			return m.errw(err)
		}
		m.logF(map[string]interface{}{"files": invalidReadyNames},
			"removed invalid segment files")
	}
	if len(leftTodoNames) > 0 {
		if err = m.migrateSegments(ctx, leftTodoNames); err != nil {
			m.logErr(err)
			return m.errw(err)
		}
	} else {
		m.log("no segments to migrate")
	}

	if err = ctx.Err(); err != nil {
		m.logErr(err)
		return m.errw(err)
	}

	if err = m.complete(); err != nil {
		m.logErr(err)
		return m.errw(err)
	}

	m.logF(map[string]interface{}{"took": time.Since(start).String()},
		"migration completed")

	return nil
}

func (m *MigratorFilterableToRangeable) isInited() (int64, bool) {
	if filename := m.dstDirMatchingFilename(migrationFileInitedGlob, regexMigrationFileInited); filename != "" {
		timestamp, err := m.extractTimestamp(filename, regexMigrationFileInited)
		if err != nil {
			return 0, false
		}
		return timestamp, true
	}
	return 0, false
}

func (m *MigratorFilterableToRangeable) init() (int64, error) {
	timestamp := time.Now().UnixNano()
	if err := os.MkdirAll(m.dstDirPath(""), 0o700); err != nil {
		return 0, err
	}
	if err := m.dstDirCreateFile(fmt.Sprintf(migrationFileInitedFormat, timestamp)); err != nil {
		return 0, err
	}
	return timestamp, nil
}

func (m *MigratorFilterableToRangeable) isTodo() bool {
	return m.dstDirFileExists(migrationFileTodo)
}

func (m *MigratorFilterableToRangeable) cleanupTodoAndReady() ([]string, error) {
	filenames := []string{}
	ec := &errorcompounder.ErrorCompounder{}
	for _, todoName := range m.dstDirMatchingFilenames(segmentFileTodoGlob, regexSegmentFileTodo) {
		filenames = append(filenames, todoName)
		ec.Add(os.Remove(m.dstDirPath(todoName)))
	}
	for _, readyName := range m.dstDirMatchingFilenames(segmentFileDbGlob, regexSegmentFileDb) {
		filenames = append(filenames, readyName)
		ec.Add(os.Remove(m.dstDirPath(readyName)))
	}
	return filenames, ec.ToError()
}

func (m *MigratorFilterableToRangeable) todo(timestamp int64) ([]string, error) {
	filenames := []string{}
	segmentNames := m.srcSegmentNames(timestamp)
	ec := &errorcompounder.ErrorCompounder{}
	for _, name := range segmentNames {
		filename := name + segmentFileTodoSuffix
		filenames = append(filenames, filename)
		ec.Add(m.dstDirCreateFile(filename))
	}
	if err := ec.ToError(); err != nil {
		return filenames, err
	}

	filenames = append(filenames, migrationFileTodo)
	return filenames, m.dstDirCreateFile(migrationFileTodo)
}

func (m *MigratorFilterableToRangeable) cleanupTemp() ([]string, error) {
	filenames := []string{}
	ec := &errorcompounder.ErrorCompounder{}
	for _, tempName := range m.dstDirMatchingFilenames(segmentFileTempGlob, regexSegmentFileTemp) {
		filenames = append(filenames, tempName)
		ec.Add(os.Remove(m.dstDirPath(tempName)))
	}
	return filenames, ec.ToError()
}

func (m *MigratorFilterableToRangeable) notReady() ([]string, []string) {
	todoNames := m.dstDirMatchingFilenames(segmentFileTodoGlob, regexSegmentFileTodo)
	readyNames := m.dstDirMatchingFilenames(segmentFileDbGlob, regexSegmentFileDb)

	if len(readyNames) == 0 {
		return todoNames, readyNames
	}

	readyMap := make(map[string]struct{})
	for _, readyName := range readyNames {
		readyMap[readyName] = struct{}{}
	}

	leftTodoNames := make([]string, 0, len(todoNames))
	for _, todoName := range todoNames {
		matchingReadyName := strings.TrimSuffix(todoName, segmentFileTodoSuffix)
		if _, ok := readyMap[matchingReadyName]; ok {
			delete(readyMap, matchingReadyName)
		} else {
			leftTodoNames = append(leftTodoNames, todoName)
		}
	}

	invalidReadyNames := make([]string, 0, len(readyMap))
	for readyName := range readyMap {
		invalidReadyNames = append(invalidReadyNames, readyName)
	}

	return leftTodoNames, invalidReadyNames
}

func (m *MigratorFilterableToRangeable) cleanupReady(readyNames []string) error {
	ec := &errorcompounder.ErrorCompounder{}
	for _, readyName := range readyNames {
		ec.Add(os.Remove(m.dstDirPath(readyName)))
	}
	return ec.ToError()
}

func (m *MigratorFilterableToRangeable) migrateSegments(ctx context.Context, todoNames []string) error {
	eg := errors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(m.routinesLimit)
	mut := new(sync.Mutex)

	ec := &errorcompounder.ErrorCompounder{}
	for _, todoName := range todoNames {
		segmentName := strings.TrimSuffix(todoName, segmentFileTodoSuffix)
		segment := m.srcSegmentByName(segmentName)
		if segment == nil {
			ec.Add(fmt.Errorf("segment %s not found in filterable bucket", segmentName))
			continue
		}

		eg.Go(func() error {
			err := m.migrateSegment(ctx, segment, segmentName)

			mut.Lock()
			ec.Add(err)
			mut.Unlock()

			return err
		})
	}

	eg.Wait()
	return ec.ToError()
}

func (m *MigratorFilterableToRangeable) migrateSegment(ctx context.Context, segment *segment, name string) error {
	start := time.Now()
	m.logF(map[string]interface{}{
		"start":   start.String(),
		"segment": name,
	}, "segment migration started")

	elementsPerSegment := 1_000_000
	elementsCount := 0
	subSegmentsCounter := 1

	cursor := segment.newRoaringSetCursor()
	tempPath := m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, subSegmentsCounter))
	mem := roaringsetrange.NewMemtable(m.logger)

	for k, bml, _ := cursor.First(); k != nil; k, bml, _ = cursor.Next() {
		deletions := bml.Deletions.ToArray()
		elementsCount += len(deletions)
		mem.Delete(binary.BigEndian.Uint64(k), deletions)

		if elementsCount >= elementsPerSegment {
			err := m.flushRangeMemtable(ctx, mem, segment.level, tempPath)
			if err != nil {
				m.logErrF(map[string]interface{}{"segment": name}, err)
				return err
			}
			m.logF(map[string]interface{}{
				"segment":        name,
				"elements_count": elementsCount,
				"sub_segment_id": subSegmentsCounter,
			}, "flushed deletions")

			elementsCount = 0
			subSegmentsCounter++
			tempPath = m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, subSegmentsCounter))
			mem = roaringsetrange.NewMemtable(m.logger)
		}
	}
	for k, bml, _ := cursor.First(); k != nil; k, bml, _ = cursor.Next() {
		additions := bml.Additions.ToArray()
		elementsCount += len(additions)
		mem.Insert(binary.BigEndian.Uint64(k), additions)

		if elementsCount >= elementsPerSegment {
			err := m.flushRangeMemtable(ctx, mem, segment.level, tempPath)
			if err != nil {
				m.logErrF(map[string]interface{}{"segment": name}, err)
				return err
			}
			m.logF(map[string]interface{}{
				"segment":        name,
				"elements_count": elementsCount,
				"sub_segment_id": subSegmentsCounter,
			}, "flushed additions")

			elementsCount = 0
			subSegmentsCounter++
			tempPath = m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, subSegmentsCounter))
			mem = roaringsetrange.NewMemtable(m.logger)
		}
	}
	if elementsCount > 0 {
		err := m.flushRangeMemtable(ctx, mem, segment.level, tempPath)
		if err != nil {
			m.logErrF(map[string]interface{}{"segment": name}, err)
			return err
		}
		m.logF(map[string]interface{}{
			"segment":        name,
			"elements_count": elementsCount,
			"sub_segment_id": subSegmentsCounter,
		}, "flushed remaining elements")
	}

	for subSegmentsCounter > 1 {
		mergedId := 1

		for c, sc := 1, subSegmentsCounter; c <= sc; c += 2 {
			leftId := c
			rightId := c + 1

			if c == sc {
				leftId = mergedId - 1
				rightId = c
				mergedId = mergedId - 1
			}

			leftPath := m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, leftId))
			rightPath := m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, rightId))
			mergePath := m.dstDirPath(fmt.Sprintf(segmentFileTempMergeFormat, name, leftId, rightId))
			tempPath = m.dstDirPath(fmt.Sprintf(segmentFileTempFormat, name, mergedId))

			if err := m.compactSubSegments(ctx, leftPath, rightPath, mergePath, tempPath,
				m.dstBucket.mmapContents, segment.level); err != nil {
				m.logErrF(map[string]interface{}{"segment": name}, err)
				return err
			}
			m.logF(map[string]interface{}{
				"segment":   name,
				"left_id":   leftId,
				"right_id":  rightId,
				"merged_id": mergedId,
			}, "compacted subsegments")

			mergedId++
			subSegmentsCounter--
		}
	}

	err := os.Rename(tempPath, m.dstDirPath(name))
	if err != nil {
		m.logErrF(map[string]interface{}{"segment": name}, err)
		return err
	}

	m.logF(map[string]interface{}{
		"took":    time.Since(start).String(),
		"segment": name,
	}, "segment migration completed")
	return nil
}

func (m *MigratorFilterableToRangeable) flushRangeMemtable(ctx context.Context,
	mem *roaringsetrange.Memtable, level uint16, filePath string,
) error {
	if ctx.Err() != nil {
		return m.errw(ctx.Err())
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	nodes := mem.Nodes()
	totalDataLength := totalPayloadSizeRoaringSetRange(nodes)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            level,
		Version:          0, // always version 0 for now
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSetRange,
	}

	if _, err = header.WriteTo(w); err != nil {
		return err
	}

	for i, node := range nodes {
		sn, err := roaringsetrange.NewSegmentNode(node.Key, node.Additions, node.Deletions)
		if err != nil {
			return fmt.Errorf("create segment node: %w", err)
		}

		if _, err = w.Write(sn.ToBuffer()); err != nil {
			return fmt.Errorf("write segment node %d: %w", i, err)
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func (m *MigratorFilterableToRangeable) compactSubSegments(ctx context.Context,
	leftPath, rightPath, mergePath, finalPath string, mmapContents bool, level uint16,
) error {
	if ctx.Err() != nil {
		return m.errw(ctx.Err())
	}

	eol := func(key []byte) (bool, error) { return false, nil }

	f, err := os.OpenFile(mergePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}

	left, err := newSegment(leftPath, m.logger, nil, eol, mmapContents, false, false, false)
	if err != nil {
		return err
	}
	right, err := newSegment(rightPath, m.logger, nil, eol, mmapContents, false, false, false)
	if err != nil {
		return err
	}

	compactor := roaringsetrange.NewCompactor(f,
		left.newRoaringSetRangeCursor(), right.newRoaringSetRangeCursor(),
		level, false)
	if err := compactor.Do(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync compacted segment file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close compacted segment file: %w", err)
	}

	if err := os.Remove(leftPath); err != nil {
		return err
	}
	if err := os.Remove(rightPath); err != nil {
		return err
	}
	if err := os.Rename(mergePath, finalPath); err != nil {
		return err
	}
	return nil
}

func (m *MigratorFilterableToRangeable) srcSegmentByName(segmentName string) *segment {
	m.srcBucket.disk.maintenanceLock.RLock()
	defer m.srcBucket.disk.maintenanceLock.RUnlock()

	for _, segment := range m.srcBucket.disk.segments {
		if name, _ := filepath.Rel(m.srcBucket.dir, segment.path); name == segmentName {
			return segment
		}
	}
	return nil
}

func (m *MigratorFilterableToRangeable) isCompleted() bool {
	return m.dstFileExists(migrationFileCompleted)
}

func (m *MigratorFilterableToRangeable) complete() error {
	todoNames := m.dstDirMatchingFilenames(segmentFileTodoGlob, regexSegmentFileTodo)
	readyNames := m.dstDirMatchingFilenames(segmentFileDbGlob, regexSegmentFileDb)

	if len(todoNames) != len(readyNames) {
		return fmt.Errorf("number of ready and todo files does not match")
	}

	readyMap := make(map[string]struct{})
	for _, readyName := range readyNames {
		readyMap[readyName] = struct{}{}
	}

	for _, todoName := range todoNames {
		matchingReadyName := strings.TrimSuffix(todoName, segmentFileTodoSuffix)
		if _, ok := readyMap[matchingReadyName]; !ok {
			return fmt.Errorf("missing segment file %q", matchingReadyName)
		}
	}

	var err error
	movedSegments := make(map[string]string)
	for _, readyName := range readyNames {
		readyPath := m.dstDirPath(readyName)
		segmentPath := m.dstPath(readyName)
		if err = os.Rename(readyPath, segmentPath); err != nil {
			break
		}
		movedSegments[segmentPath] = readyPath
	}

	if err == nil {
		err = m.dstCreateFile(migrationFileCompleted)
	}

	// move back copied segments
	if err != nil {
		ec := &errorcompounder.ErrorCompounder{}
		ec.Add(err)

		for segmentPath, readyPath := range movedSegments {
			ec.Add(os.Rename(segmentPath, readyPath))
		}
		return ec.ToError()
	}

	return os.RemoveAll(m.dstDirPath(""))
}

func (m *MigratorFilterableToRangeable) anySegmentExists() bool {
	return m.dstMatchingFilename(segmentFileDbGlob, regexSegmentFileDb) != ""
}

func (m *MigratorFilterableToRangeable) dstFileExists(filename string) bool {
	return m.fileExists(m.dstPath(filename))
}

func (m *MigratorFilterableToRangeable) dstDirFileExists(filename string) bool {
	return m.fileExists(m.dstDirPath(filename))
}

func (m *MigratorFilterableToRangeable) fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (m *MigratorFilterableToRangeable) dstMatchingFilename(glob string, reg *regexp.Regexp) string {
	if filenames := m.matchingFilenames(m.dstPath(glob), reg, 1); len(filenames) == 1 {
		return filenames[0]
	}
	return ""
}

func (m *MigratorFilterableToRangeable) dstDirMatchingFilename(glob string, reg *regexp.Regexp) string {
	if filenames := m.matchingFilenames(m.dstDirPath(glob), reg, 1); len(filenames) == 1 {
		return filenames[0]
	}
	return ""
}

func (m *MigratorFilterableToRangeable) dstDirMatchingFilenames(glob string, reg *regexp.Regexp) []string {
	return m.matchingFilenames(m.dstDirPath(glob), reg, 0)
}

func (m *MigratorFilterableToRangeable) matchingFilenames(globpath string, reg *regexp.Regexp, limit int) []string {
	matches, _ := filepath.Glob(globpath)
	files := make([]string, 0, limit)
	for _, match := range matches {
		f, _ := os.Stat(match)
		if reg.MatchString(f.Name()) {
			files = append(files, f.Name())
			if len(files) == limit {
				break
			}
		}
	}
	return files
}

func (m *MigratorFilterableToRangeable) srcSegmentNames(timestamp int64) []string {
	m.srcBucket.disk.maintenanceLock.RLock()
	defer m.srcBucket.disk.maintenanceLock.RUnlock()

	names := make([]string, 0, len(m.srcBucket.disk.segments))
	for _, segment := range m.srcBucket.disk.segments {
		name, _ := filepath.Rel(m.srcBucket.dir, segment.path)
		segTimestamp, err := m.extractTimestamp(name, regexSegmentFileDb)
		if err == nil && segTimestamp <= timestamp {
			names = append(names, name)
		}
	}

	return names
}

func (m *MigratorFilterableToRangeable) dstCreateFile(filename string) error {
	return m.createFile(m.dstPath(filename))
}

func (m *MigratorFilterableToRangeable) dstDirCreateFile(filename string) error {
	return m.createFile(m.dstDirPath(filename))
}

func (m *MigratorFilterableToRangeable) createFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

func (m *MigratorFilterableToRangeable) extractTimestamp(filename string, reg *regexp.Regexp) (int64, error) {
	strings := reg.FindStringSubmatch(filename)
	if len(strings) != 2 {
		return 0, fmt.Errorf("timestamp not found in filename %q", filename)
	}
	timestamp, err := strconv.Atoi(strings[1])
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp in filename %q: %w", filename, err)
	}
	return int64(timestamp), nil
}

func (m *MigratorFilterableToRangeable) dstPath(filename string) string {
	return filepath.Join(m.dstBucket.dir, filename)
}

func (m *MigratorFilterableToRangeable) dstDirPath(filename string) string {
	return filepath.Join(m.dstBucket.dir, migrationDir, filename)
}

func (m *MigratorFilterableToRangeable) log(msg string, args ...interface{}) {
	m.logger.Debugf(msg, args...)
}

func (m *MigratorFilterableToRangeable) logF(fields logrus.Fields, msg string, args ...interface{}) {
	m.logger.WithFields(fields).Debugf(msg, args...)
}

func (m *MigratorFilterableToRangeable) logErr(err error) {
	m.logger.WithError(err).Errorf(err.Error())
}

func (m *MigratorFilterableToRangeable) logErrF(fields logrus.Fields, err error) {
	m.logger.WithFields(fields).WithError(err).Errorf(err.Error())
}

func (m *MigratorFilterableToRangeable) errw(err error) error {
	return fmt.Errorf("rangeable migration %q: %w", m.propName, err)
}
