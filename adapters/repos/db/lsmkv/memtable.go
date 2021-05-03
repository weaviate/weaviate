package lsmkv

import (
	"sync"

	"github.com/pkg/errors"
)

type Memtable struct {
	sync.RWMutex
	key          *binarySearchTree
	keyMulti     *binarySearchTreeMulti
	primaryIndex *binarySearchTree
	commitlog    *commitLogger
	size         uint64
	path         string
	strategy     string
}

func newMemtable(path string, strategy string) (*Memtable, error) {
	cl, err := newCommitLogger(path)
	if err != nil {
		return nil, errors.Wrap(err, "init commit logger")
	}

	return &Memtable{
		key:          &binarySearchTree{},
		keyMulti:     &binarySearchTreeMulti{},
		primaryIndex: &binarySearchTree{}, // todo, sort upfront
		commitlog:    cl,
		path:         path,
		strategy:     strategy,
	}, nil
}

type keyIndex struct {
	key        []byte
	valueStart int
	valueEnd   int
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

func (l *Memtable) put(key, value []byte) error {
	if l.strategy != StrategyReplace {
		return errors.Errorf("put only possible with strategy 'replace'")
	}

	l.Lock()
	defer l.Unlock()
	if err := l.commitlog.put(key, value); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.key.insert(key, value)
	l.size += uint64(len(key))
	l.size += uint64(len(value))

	return nil
}

func (l *Memtable) setTombstone(key []byte) error {
	if l.strategy != "replace" {
		return errors.Errorf("setTombstone only possible with strategy 'replace'")
	}

	l.Lock()
	defer l.Unlock()

	if err := l.commitlog.setTombstone(key); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.key.setTombstone(key)

	return nil
}

func (l *Memtable) getCollection(key []byte) ([]value, error) {
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

func (l *Memtable) append(key []byte, values []value) error {
	if l.strategy != StrategySetCollection && l.strategy != StrategyMapCollection {
		return errors.Errorf("append only possible with strategies %q, %q",
			StrategySetCollection, StrategyMapCollection)
	}

	l.Lock()
	defer l.Unlock()
	// TODO: commit log
	// if err := l.commitlog.put(key, value); err != nil {
	// 	return errors.Wrap(err, "write into commit log")
	// }

	l.keyMulti.insert(key, values)
	l.size += uint64(len(key))
	for _, value := range values {
		l.size += uint64(len(value.value))
	}

	return nil
}

// func (l *Memtable) mapSet(rowKey []byte, values []value) error {
// 	if l.strategy != StrategySetCollection {
// 		return errors.Errorf("append only possible with strategy %q", StrategySetCollection)
// 	}

// 	l.Lock()
// 	defer l.Unlock()
// 	// TODO: commit log
// 	// if err := l.commitlog.put(key, value); err != nil {
// 	// 	return errors.Wrap(err, "write into commit log")
// 	// }

// 	l.keyMulti.insert(key, values)
// 	l.size += uint64(len(key))
// 	for _, value := range values {
// 		l.size += uint64(len(value.value))
// 	}

// 	return nil
// }

func (l *Memtable) Size() uint64 {
	l.RLock()
	defer l.RUnlock()

	return l.size
}
