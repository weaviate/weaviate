//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

type memtable interface {
	get(key []byte) ([]byte, error)
	getBySecondary(pos int, key []byte) ([]byte, error)
	put(key, value []byte, opts ...SecondaryKeyOption) error
	setTombstone(key []byte, opts ...SecondaryKeyOption) error
	setTombstoneWith(key []byte, deletionTime time.Time, opts ...SecondaryKeyOption) error

	getCollection(key []byte) ([]value, error)
	getMap(key []byte) ([]MapPair, error)
	append(key []byte, values []value) error
	appendMapSorted(key []byte, pair MapPair) error

	Size() uint64
	Path() string
	ActiveDuration() time.Duration
	DirtyDuration() time.Duration
	updateDirtyAt()
	countStats() *countStats
	getStrategy() string
	commitlogSize() int64
	commitlogWalPath() string

	writeWAL() error
	flushWAL() error
	flush() (string, error)
	setAveragePropertyLength(avgPropLength float64, propLengthCount uint64)
	getAndUpdateWritesSinceLastSync(logger logrus.FieldLogger) bool

	ReadOnlyTombstones() (*sroar.Bitmap, error)
	SetTombstone(docId uint64) error
	GetPropLengths() (uint64, uint64, error)

	newCursor() innerCursorReplace
	newBlockingCursor() (innerCursorReplace, func())
	newCursorWithSecondaryIndex(pos int) innerCursorReplace
	newCollectionCursor() innerCursorCollection
	newRoaringSetCursor() roaringset.InnerCursor
	newRoaringSetRangeReader() roaringsetrange.InnerReader
	newMapCursor() innerCursorMap

	roaringSetAddOne(key []byte, value uint64) error
	roaringSetAddList(key []byte, values []uint64) error
	roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error
	roaringSetRemoveOne(key []byte, value uint64) error
	roaringSetRemoveList(key []byte, values []uint64) error
	roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error
	roaringSetAddRemoveSlices(key []byte, additions []uint64, deletions []uint64) error
	roaringSetGet(key []byte) (roaringset.BitmapLayer, error)
	roaringSetAdjustMeta(entriesChanged int)
	roaringSetAddCommitLog(key []byte, additions []uint64, deletions []uint64) error

	roaringSetRangeAdd(key uint64, values ...uint64) error
	roaringSetRangeRemove(key uint64, values ...uint64) error
	roaringSetRangeAddRemove(key uint64, additions []uint64, deletions []uint64) error
	roaringSetRangeAdjustMeta(entriesChanged int)
	roaringSetRangeAddCommitLog(key uint64, additions []uint64, deletions []uint64) error
	extractRoaringSetRange() *roaringsetrange.Memtable

	flushDataReplace(f *segmentindex.SegmentFile) ([]segmentindex.Key, error)
	flushDataSet(f *segmentindex.SegmentFile) ([]segmentindex.Key, error)
	flushDataMap(f *segmentindex.SegmentFile) ([]segmentindex.Key, error)
	flushDataCollection(f *segmentindex.SegmentFile, flat []*binarySearchNodeMulti) ([]segmentindex.Key, error)
	flushDataInverted(f *segmentindex.SegmentFile, ogF *diskio.MeteredWriter, bufw *bufio.Writer) ([]segmentindex.Key, *sroar.Bitmap, error)
	flushDataRoaringSet(f *segmentindex.SegmentFile) ([]segmentindex.Key, error)
	flushDataRoaringSetRange(f *segmentindex.SegmentFile) ([]segmentindex.Key, error)

	incWriterCount()
	decWriterCount()
	getWriterCount() int64
}

type Memtable struct {
	sync.RWMutex
	key                *binarySearchTree
	keyMulti           *binarySearchTreeMulti
	keyMap             *binarySearchTreeMap
	primaryIndex       *binarySearchTree
	roaringSet         *roaringset.BinarySearchTree
	roaringSetRange    *roaringsetrange.Memtable
	commitlog          memtableCommitLogger
	allocChecker       memwatch.AllocChecker
	size               uint64
	path               string
	strategy           string
	secondaryIndices   uint16
	secondaryToPrimary []map[string][]byte
	// stores time memtable got dirty to determine when flush is needed
	dirtyAt             time.Time
	createdAt           time.Time
	metrics             *memtableMetrics
	writesSinceLastSync bool

	tombstones *sroar.Bitmap

	enableChecksumValidation bool

	bm25config                   *models.BM25Config
	averagePropLength            float64
	propLengthCount              uint64
	writeSegmentInfoIntoFileName bool

	// We're only tracking the refcount for writers. Readers get a consistent
	// view of all memtables & segments, so they don't need ref-counting.
	// Writers do, because if we have an ongoing write, we cannot start flushing
	// the memtable. This prevents the memtable from being flushed while writers
	// are active, ensuring data consistency during concurrent operations.
	writerCount atomic.Int64
}

func newMemtable(path string, strategy string, secondaryIndices uint16,
	cl memtableCommitLogger, metrics *Metrics, logger logrus.FieldLogger,
	enableChecksumValidation bool, bm25config *models.BM25Config, writeSegmentInfoIntoFileName bool, allocChecker memwatch.AllocChecker,
) (*Memtable, error) {
	memtableMetrics, err := newMemtableMetrics(metrics, filepath.Dir(path), strategy)
	if err != nil {
		return nil, fmt.Errorf("init memtable metrics: %w", err)
	}

	m := &Memtable{
		key:                          &binarySearchTree{},
		keyMulti:                     &binarySearchTreeMulti{},
		keyMap:                       &binarySearchTreeMap{},
		primaryIndex:                 &binarySearchTree{}, // todo, sort upfront
		roaringSet:                   &roaringset.BinarySearchTree{},
		roaringSetRange:              roaringsetrange.NewMemtable(logger),
		commitlog:                    cl,
		path:                         path,
		strategy:                     strategy,
		secondaryIndices:             secondaryIndices,
		dirtyAt:                      time.Time{},
		createdAt:                    time.Now(),
		metrics:                      memtableMetrics,
		enableChecksumValidation:     enableChecksumValidation,
		bm25config:                   bm25config,
		writeSegmentInfoIntoFileName: writeSegmentInfoIntoFileName,
	}

	if m.secondaryIndices > 0 {
		m.secondaryToPrimary = make([]map[string][]byte, m.secondaryIndices)
		for i := range m.secondaryToPrimary {
			m.secondaryToPrimary[i] = map[string][]byte{}
		}
	}

	m.metrics.observeSize(m.size)

	if m.strategy == StrategyInverted {
		m.tombstones = sroar.NewBitmap()
	}

	return m, nil
}

func (m *Memtable) get(key []byte) ([]byte, error) {
	start := time.Now()
	defer m.metrics.observeGet(start.UnixNano())

	if m.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	m.RLock()
	defer m.RUnlock()

	return m.key.get(key)
}

func (m *Memtable) getBySecondary(pos int, key []byte) ([]byte, error) {
	start := time.Now()
	defer m.metrics.observeGetBySecondary(start.UnixNano())

	if m.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	m.RLock()
	defer m.RUnlock()

	primary := m.secondaryToPrimary[pos][string(key)]
	if primary == nil {
		return nil, lsmkv.NotFound
	}

	return m.key.get(primary)
}

func (m *Memtable) put(key, value []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer m.metrics.observePut(start.UnixNano())

	if m.strategy != StrategyReplace {
		return errors.Errorf("put only possible with strategy 'replace'")
	}

	m.Lock()
	defer m.Unlock()
	m.writesSinceLastSync = true

	var secondaryKeys [][]byte
	if m.secondaryIndices > 0 {
		secondaryKeys = make([][]byte, m.secondaryIndices)
		for _, opt := range opts {
			if err := opt(secondaryKeys); err != nil {
				return err
			}
		}
	}

	if err := m.commitlog.put(segmentReplaceNode{
		primaryKey:          key,
		value:               value,
		secondaryIndexCount: m.secondaryIndices,
		secondaryKeys:       secondaryKeys,
		tombstone:           false,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	netAdditions, previousKeys := m.key.insert(key, value, secondaryKeys)

	for i, sec := range previousKeys {
		m.secondaryToPrimary[i][string(sec)] = nil
	}

	for i, sec := range secondaryKeys {
		m.secondaryToPrimary[i][string(sec)] = key
	}

	m.size += uint64(netAdditions)
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()

	return nil
}

func (m *Memtable) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer m.metrics.observeSetTombstone(start.UnixNano())

	if m.strategy != "replace" {
		return errors.Errorf("setTombstone only possible with strategy 'replace'")
	}

	m.Lock()
	defer m.Unlock()
	m.writesSinceLastSync = true

	var secondaryKeys [][]byte
	if m.secondaryIndices > 0 {
		secondaryKeys = make([][]byte, m.secondaryIndices)
		for _, opt := range opts {
			if err := opt(secondaryKeys); err != nil {
				return err
			}
		}
	}

	if err := m.commitlog.put(segmentReplaceNode{
		primaryKey:          key,
		value:               nil,
		secondaryIndexCount: m.secondaryIndices,
		secondaryKeys:       secondaryKeys,
		tombstone:           true,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	m.key.setTombstone(key, nil, secondaryKeys)
	m.size += uint64(len(key)) + 1 // 1 byte for tombstone
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()

	return nil
}

func (m *Memtable) setTombstoneWith(key []byte, deletionTime time.Time, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer m.metrics.observeSetTombstone(start.UnixNano())

	if m.strategy != "replace" {
		return errors.Errorf("setTombstone only possible with strategy 'replace'")
	}

	m.Lock()
	defer m.Unlock()
	m.writesSinceLastSync = true

	var secondaryKeys [][]byte
	if m.secondaryIndices > 0 {
		secondaryKeys = make([][]byte, m.secondaryIndices)
		for _, opt := range opts {
			if err := opt(secondaryKeys); err != nil {
				return err
			}
		}
	}

	tombstonedVal := tombstonedValue(deletionTime)

	if err := m.commitlog.put(segmentReplaceNode{
		primaryKey:          key,
		value:               tombstonedVal[:],
		secondaryIndexCount: m.secondaryIndices,
		secondaryKeys:       secondaryKeys,
		tombstone:           true,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	m.key.setTombstone(key, tombstonedVal[:], secondaryKeys)
	m.size += uint64(len(key)) + 1 // 1 byte for tombstone
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()

	return nil
}

func tombstonedValue(deletionTime time.Time) []byte {
	var tombstonedVal [1 + 8]byte // version=1 deletionTime
	tombstonedVal[0] = 1
	binary.LittleEndian.PutUint64(tombstonedVal[1:], uint64(deletionTime.UnixMilli()))
	return tombstonedVal[:]
}

func errorFromTombstonedValue(tombstonedVal []byte) error {
	if len(tombstonedVal) == 0 {
		return lsmkv.Deleted
	}

	if tombstonedVal[0] != 1 {
		return fmt.Errorf("unexpected tomstoned value, unsupported version %d", tombstonedVal[0])
	}

	if len(tombstonedVal) != 9 {
		return fmt.Errorf("unexpected tomstoned value, invalid length")
	}

	deletionTimeUnixMilli := int64(binary.LittleEndian.Uint64(tombstonedVal[1:]))

	return lsmkv.NewErrDeleted(time.UnixMilli(deletionTimeUnixMilli))
}

func (m *Memtable) getCollection(key []byte) ([]value, error) {
	start := time.Now()
	defer m.metrics.observeGetCollection(start.UnixNano())

	// TODO amourao: check if this is needed for StrategyInverted
	if m.strategy != StrategySetCollection && m.strategy != StrategyMapCollection && m.strategy != StrategyInverted {
		return nil, errors.Errorf("getCollection only possible with strategies %q, %q, %q",
			StrategySetCollection, StrategyMapCollection, StrategyInverted)
	}

	m.RLock()
	defer m.RUnlock()

	v, err := m.keyMulti.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *Memtable) getMap(key []byte) ([]MapPair, error) {
	start := time.Now()
	defer m.metrics.observeGetMap(start.UnixNano())

	if m.strategy != StrategyMapCollection && m.strategy != StrategyInverted {
		return nil, errors.Errorf("getMap only possible with strategies %q, %q",
			StrategyMapCollection, StrategyInverted)
	}

	m.RLock()
	defer m.RUnlock()

	v, err := m.keyMap.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *Memtable) append(key []byte, values []value) error {
	start := time.Now()
	defer m.metrics.observeAppend(start.UnixNano())

	if m.strategy != StrategySetCollection && m.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	m.Lock()
	defer m.Unlock()
	m.writesSinceLastSync = true

	if err := m.commitlog.append(segmentCollectionNode{
		primaryKey: key,
		values:     values,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	m.keyMulti.insert(key, values)
	m.size += uint64(len(key))
	for _, value := range values {
		m.size += uint64(len(value.value))
	}
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()

	return nil
}

func (m *Memtable) appendMapSorted(key []byte, pair MapPair) error {
	start := time.Now()
	defer m.metrics.observeAppendMapSorted(start.UnixNano())

	if m.strategy != StrategyMapCollection && m.strategy != StrategyInverted {
		return errors.Errorf("append only possible with strategy %q, %q",
			StrategyMapCollection, StrategyInverted)
	}

	valuesForCommitLog, err := pair.Bytes()
	if err != nil {
		return err
	}

	newNode := segmentCollectionNode{
		primaryKey: key,
		values: []value{
			{
				value:     valuesForCommitLog,
				tombstone: pair.Tombstone,
			},
		},
	}

	m.Lock()
	defer m.Unlock()
	m.writesSinceLastSync = true

	if err := m.commitlog.append(newNode); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	m.keyMap.insert(key, pair)
	m.size += uint64(len(key) + len(valuesForCommitLog))
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()

	return nil
}

func (m *Memtable) Size() uint64 {
	m.RLock()
	defer m.RUnlock()

	return m.size
}

func (m *Memtable) Path() string {
	m.RLock()
	defer m.RUnlock()

	return m.path
}

func (m *Memtable) ActiveDuration() time.Duration {
	m.RLock()
	defer m.RUnlock()

	return time.Since(m.createdAt)
}

func (m *Memtable) updateDirtyAt() {
	if m.dirtyAt.IsZero() {
		m.dirtyAt = time.Now()
	}
}

// returns time memtable got dirty (1st write occurred)
// (0 if clean)
func (m *Memtable) DirtyDuration() time.Duration {
	m.RLock()
	defer m.RUnlock()

	if m.dirtyAt.IsZero() {
		return 0
	}
	return time.Since(m.dirtyAt)
}

func (m *Memtable) countStats() *countStats {
	m.RLock()
	defer m.RUnlock()
	return m.key.countStats()
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to safge unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (m *Memtable) writeWAL() error {
	m.Lock()
	defer m.Unlock()

	return m.commitlog.flushBuffers()
}

func (m *Memtable) ReadOnlyTombstones() (*sroar.Bitmap, error) {
	if m.strategy != StrategyInverted {
		return nil, errors.Errorf("tombstones only supported for strategy %q", StrategyInverted)
	}

	m.RLock()
	defer m.RUnlock()

	if m.tombstones != nil {
		return m.tombstones.Clone(), nil
	}

	return nil, lsmkv.NotFound
}

func (m *Memtable) SetTombstone(docId uint64) error {
	if m.strategy != StrategyInverted {
		return errors.Errorf("tombstones only supported for strategy %q", StrategyInverted)
	}

	m.Lock()
	defer m.Unlock()

	m.tombstones.Set(docId)

	return nil
}

func (m *Memtable) GetPropLengths() (uint64, uint64, error) {
	m.RLock()
	flatA := m.keyMap.flattenInOrder()
	m.RUnlock()

	docIdsLengths := make(map[uint64]uint32)
	propLengthSum := uint64(0)
	propLengthCount := uint64(0)

	for _, mapNode := range flatA {
		for j := range mapNode.values {
			docId := binary.BigEndian.Uint64(mapNode.values[j].Key)
			if !mapNode.values[j].Tombstone {
				fieldLength := math.Float32frombits(binary.LittleEndian.Uint32(mapNode.values[j].Value[4:]))
				if _, ok := docIdsLengths[docId]; !ok {
					propLengthSum += uint64(fieldLength)
					propLengthCount++
				}
				docIdsLengths[docId] = uint32(fieldLength)
			}
		}
	}

	return propLengthSum, propLengthCount, nil
}

func (m *Memtable) incWriterCount() {
	m.writerCount.Add(1)
}

func (m *Memtable) decWriterCount() {
	m.writerCount.Add(-1)
}

func (m *Memtable) getWriterCount() int64 {
	return m.writerCount.Load()
}

func (m *Memtable) getStrategy() string {
	return m.strategy
}

func (m *Memtable) setAveragePropertyLength(avgPropLength float64, propLengthCount uint64) {
	m.Lock()
	defer m.Unlock()

	m.averagePropLength = avgPropLength
	m.propLengthCount = propLengthCount
}

func (m *Memtable) commitlogSize() int64 {
	return m.commitlog.size()
}

func (m *Memtable) commitlogWalPath() string {
	return m.commitlog.walPath()
}

func (m *Memtable) getAndUpdateWritesSinceLastSync(logger logrus.FieldLogger) bool {
	m.Lock()
	defer m.Unlock()

	hasWrites := m.writesSinceLastSync
	if !hasWrites {
		// had no work this iteration, cycle manager can back off
		return false
	}

	err := m.commitlog.flushBuffers()
	if err != nil {
		logger.WithField("action", "lsm_memtable_flush").
			WithField("path", m.path).
			WithError(err).
			Errorf("flush and switch failed")

		return false
	}

	err = m.commitlog.sync()
	if err != nil {
		logger.WithField("action", "lsm_memtable_flush").
			WithField("path", m.path).
			WithError(err).
			Errorf("flush and switch failed")

		return false
	}
	m.writesSinceLastSync = false
	// there was work in this iteration, cycle manager should not back off and revisit soon
	return true
}

func (m *Memtable) extractRoaringSetRange() *roaringsetrange.Memtable {
	m.RLock()
	defer m.RUnlock()

	result := m.roaringSetRange
	return result
}
