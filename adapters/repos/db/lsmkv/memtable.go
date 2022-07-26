//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Memtable struct {
	sync.RWMutex
	key                *binarySearchTree
	keyMulti           *binarySearchTreeMulti
	keyMap             *binarySearchTreeMap
	primaryIndex       *binarySearchTree
	commitlog          *commitLogger
	size               uint64
	path               string
	pathDir            string
	strategy           string
	secondaryIndices   uint16
	secondaryToPrimary []map[string][]byte
	lastWrite          time.Time
	metrics            *memtableMetrics
}

func newMemtable(path string, strategy string,
	secondaryIndices uint16, metrics *Metrics) (*Memtable, error) {
	cl, err := newCommitLogger(path)
	if err != nil {
		return nil, errors.Wrap(err, "init commit logger")
	}

	m := &Memtable{
		key:              &binarySearchTree{},
		keyMulti:         &binarySearchTreeMulti{},
		keyMap:           &binarySearchTreeMap{},
		primaryIndex:     &binarySearchTree{}, // todo, sort upfront
		commitlog:        cl,
		path:             path,
		strategy:         strategy,
		secondaryIndices: secondaryIndices,
		lastWrite:        time.Now(),
		metrics:          newMemtableMetrics(metrics, filepath.Dir(path), strategy),
	}

	if m.secondaryIndices > 0 {
		m.secondaryToPrimary = make([]map[string][]byte, m.secondaryIndices)
		for i := range m.secondaryToPrimary {
			m.secondaryToPrimary[i] = map[string][]byte{}
		}
	}

	metrics.MemtableSize(m.pathDir, m.strategy, m.size)

	return m, nil
}

type keyIndex struct {
	key           []byte
	secondaryKeys [][]byte
	valueStart    int
	valueEnd      int
}

func (l *Memtable) get(key []byte) ([]byte, error) {
	if l.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	l.RLock()
	defer l.RUnlock()

	v, err := l.key.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (l *Memtable) getBySecondary(pos int, key []byte) ([]byte, error) {
	if l.strategy != StrategyReplace {
		return nil, errors.Errorf("get only possible with strategy 'replace'")
	}

	l.RLock()
	defer l.RUnlock()

	primary := l.secondaryToPrimary[pos][string(key)]
	if primary == nil {
		return nil, NotFound
	}

	v, err := l.key.get(primary)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (l *Memtable) put(key, value []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer l.metrics.put(start.UnixNano())

	if l.strategy != StrategyReplace {
		return errors.Errorf("put only possible with strategy 'replace'")
	}

	l.Lock()
	defer l.Unlock()

	var secondaryKeys [][]byte
	if l.secondaryIndices > 0 {
		secondaryKeys = make([][]byte, l.secondaryIndices)
		for _, opt := range opts {
			if err := opt(secondaryKeys); err != nil {
				return err
			}
		}
	}

	if err := l.commitlog.put(segmentReplaceNode{
		primaryKey:          key,
		value:               value,
		secondaryIndexCount: l.secondaryIndices,
		secondaryKeys:       secondaryKeys,
		tombstone:           false,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	netAdditions := l.key.insert(key, value, secondaryKeys)
	l.size += uint64(netAdditions)
	// l.metrics.MemtableSize(l.pathDir, l.strategy, l.size)

	for i, sec := range secondaryKeys {
		l.secondaryToPrimary[i][string(sec)] = key
	}

	l.lastWrite = time.Now()

	return nil
}

func (l *Memtable) setTombstone(key []byte, opts ...SecondaryKeyOption) error {
	start := time.Now()
	defer l.metrics.setTombstone(start.UnixNano())

	if l.strategy != "replace" {
		return errors.Errorf("setTombstone only possible with strategy 'replace'")
	}

	l.Lock()
	defer l.Unlock()

	var secondaryKeys [][]byte
	if l.secondaryIndices > 0 {
		secondaryKeys = make([][]byte, l.secondaryIndices)
		for _, opt := range opts {
			if err := opt(secondaryKeys); err != nil {
				return err
			}
		}
	}

	if err := l.commitlog.put(segmentReplaceNode{
		primaryKey:          key,
		value:               nil,
		secondaryIndexCount: l.secondaryIndices,
		secondaryKeys:       secondaryKeys,
		tombstone:           true,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.key.setTombstone(key, secondaryKeys)
	l.size += uint64(len(key)) + 1 // 1 byte for tombstone
	l.lastWrite = time.Now()
	// l.metrics.MemtableSize(l.pathDir, l.strategy, l.size)

	return nil
}

func (l *Memtable) getCollection(key []byte) ([]value, error) {
	start := time.Now()
	defer l.metrics.getCollection(start.UnixNano())

	if l.strategy != StrategySetCollection && l.strategy != StrategyMapCollection {
		return nil, errors.Errorf("getCollection only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	l.RLock()
	defer l.RUnlock()

	v, err := l.keyMulti.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (l *Memtable) getMap(key []byte) ([]MapPair, error) {
	start := time.Now()
	defer l.metrics.getMap(start.UnixNano())

	if l.strategy != StrategyMapCollection {
		return nil, errors.Errorf("getCollection only possible with strategy %q",
			StrategyMapCollection)
	}

	l.RLock()
	defer l.RUnlock()

	v, err := l.keyMap.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (l *Memtable) append(key []byte, values []value) error {
	start := time.Now()
	defer l.metrics.append(start.UnixNano())

	if l.strategy != StrategySetCollection && l.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	l.Lock()
	defer l.Unlock()
	if err := l.commitlog.append(segmentCollectionNode{
		primaryKey: key,
		values:     values,
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.keyMulti.insert(key, values)
	l.size += uint64(len(key))
	for _, value := range values {
		l.size += uint64(len(value.value))
	}

	// l.metrics.MemtableSize(l.pathDir, l.strategy, l.size)
	l.lastWrite = time.Now()
	return nil
}

func (l *Memtable) appendMapSorted(key []byte, pair MapPair) error {
	start := time.Now()
	defer l.metrics.appendMapSorted(start.UnixNano())

	if l.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategy %q",
			StrategyMapCollection)
	}

	l.Lock()
	defer l.Unlock()

	valuesForCommitLog, err := pair.Bytes()
	if err != nil {
		return err
	}

	if err := l.commitlog.append(segmentCollectionNode{
		primaryKey: key,
		values: []value{
			{
				value: valuesForCommitLog,
			},
		},
	}); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.keyMap.insert(key, pair)

	l.size += uint64(len(key) + len(valuesForCommitLog))
	l.lastWrite = time.Now()
	// l.metrics.MemtableSize(l.pathDir, l.strategy, l.size)

	return nil
}

func (l *Memtable) Size() uint64 {
	l.RLock()
	defer l.RUnlock()

	return l.size
}

func (l *Memtable) IdleDuration() time.Duration {
	l.RLock()
	defer l.RUnlock()

	return time.Since(l.lastWrite)
}

func (l *Memtable) countStats() *countStats {
	return l.key.countStats()
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to safge unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (l *Memtable) writeWAL() error {
	l.Lock()
	defer l.Unlock()

	return l.commitlog.flushBuffers()
}
