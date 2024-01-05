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
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type Memtable struct {
	sync.RWMutex
	key                *binarySearchTree
	keyMulti           *binarySearchTreeMulti
	keyMap             *binarySearchTreeMap
	primaryIndex       *binarySearchTree
	roaringSet         *roaringset.BinarySearchTree
	commitlog          *commitLogger
	size               uint64
	path               string
	strategy           string
	secondaryIndices   uint16
	secondaryToPrimary []map[string][]byte
	lastWrite          time.Time
	createdAt          time.Time
	metrics            *memtableMetrics
}

func newMemtable(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics,
) (*Memtable, error) {
	cl, err := newCommitLogger(path)
	if err != nil {
		return nil, errors.Wrap(err, "init commit logger")
	}

	m := &Memtable{
		key:              &binarySearchTree{},
		keyMulti:         &binarySearchTreeMulti{},
		keyMap:           &binarySearchTreeMap{},
		primaryIndex:     &binarySearchTree{}, // todo, sort upfront
		roaringSet:       &roaringset.BinarySearchTree{},
		commitlog:        cl,
		path:             path,
		strategy:         strategy,
		secondaryIndices: secondaryIndices,
		lastWrite:        time.Now(),
		createdAt:        time.Now(),
		metrics:          newMemtableMetrics(metrics, filepath.Dir(path), strategy),
	}

	if m.secondaryIndices > 0 {
		m.secondaryToPrimary = make([]map[string][]byte, m.secondaryIndices)
		for i := range m.secondaryToPrimary {
			m.secondaryToPrimary[i] = map[string][]byte{}
		}
	}

	m.metrics.size(m.size)

	return m, nil
}

func (m *Memtable) get(key []byte) ([]byte, error) {
	start := time.Now()
	defer m.metrics.get(start.UnixNano())

	if m.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	m.RLock()
	defer m.RUnlock()

	v, err := m.key.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *Memtable) getBySecondary(pos int, key []byte) ([]byte, error) {
	start := time.Now()
	defer m.metrics.getBySecondary(start.UnixNano())

	if m.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	m.RLock()
	defer m.RUnlock()

	primary := m.secondaryToPrimary[pos][string(key)]
	if primary == nil {
		return nil, lsmkv.NotFound
	}

	v, err := m.key.get(primary)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *Memtable) put(key, value []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer m.metrics.put(start.UnixNano())

	if m.strategy != StrategyReplace {
		return errors.Errorf("put only possible with strategy 'replace'")
	}

	m.Lock()
	defer m.Unlock()

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
	m.size += uint64(netAdditions)
	m.metrics.size(m.size)

	for i, sec := range previousKeys {
		m.secondaryToPrimary[i][string(sec)] = nil
	}

	for i, sec := range secondaryKeys {
		m.secondaryToPrimary[i][string(sec)] = key
	}

	m.lastWrite = time.Now()

	return nil
}

func (m *Memtable) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer m.metrics.setTombstone(start.UnixNano())

	if m.strategy != "replace" {
		return errors.Errorf("setTombstone only possible with strategy 'replace'")
	}

	m.Lock()
	defer m.Unlock()

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

	m.key.setTombstone(key, secondaryKeys)
	m.size += uint64(len(key)) + 1 // 1 byte for tombstone
	m.lastWrite = time.Now()
	m.metrics.size(m.size)

	return nil
}

func (m *Memtable) getCollection(key []byte) ([]value, error) {
	start := time.Now()
	defer m.metrics.getCollection(start.UnixNano())

	if m.strategy != StrategySetCollection && m.strategy != StrategyMapCollection {
		return nil, errors.Errorf("getCollection only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
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
	defer m.metrics.getMap(start.UnixNano())

	if m.strategy != StrategyMapCollection {
		return nil, errors.Errorf("getCollection only possible with strategy %q",
			StrategyMapCollection)
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
	defer m.metrics.append(start.UnixNano())

	if m.strategy != StrategySetCollection && m.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	m.Lock()
	defer m.Unlock()
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

	m.metrics.size(m.size)
	m.lastWrite = time.Now()
	return nil
}

func (m *Memtable) appendMapSorted(key []byte, pair MapPair) error {
	start := time.Now()
	defer m.metrics.appendMapSorted(start.UnixNano())

	if m.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategy %q",
			StrategyMapCollection)
	}

	m.Lock()
	defer m.Unlock()

	valuesForCommitLog, err := pair.Bytes()
	if err != nil {
		return err
	}

	if err := m.commitlog.append(segmentCollectionNode{
		primaryKey: key,
		values: []value{
			{
				value: valuesForCommitLog,
			},
		},
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	m.keyMap.insert(key, pair)

	m.size += uint64(len(key) + len(valuesForCommitLog))
	m.lastWrite = time.Now()
	m.metrics.size(m.size)

	return nil
}

func (m *Memtable) Size() uint64 {
	m.RLock()
	defer m.RUnlock()

	return m.size
}

func (m *Memtable) ActiveDuration() time.Duration {
	m.RLock()
	defer m.RUnlock()

	return time.Since(m.createdAt)
}

func (m *Memtable) IdleDuration() time.Duration {
	m.RLock()
	defer m.RUnlock()

	return time.Since(m.lastWrite)
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
