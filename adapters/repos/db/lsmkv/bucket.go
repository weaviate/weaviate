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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Bucket struct {
	dir      string
	active   *Memtable
	flushing *Memtable
	disk     *SegmentGroup
	logger   logrus.FieldLogger

	// Lock() means a move from active to flushing is happening, RLock() is
	// normal operation
	flushLock sync.RWMutex

	memTableThreshold uint64
	strategy          string
	secondaryIndices  uint16

	stopFlushCycle chan struct{}
}

func NewBucket(ctx context.Context, dir string, logger logrus.FieldLogger,
	opts ...BucketOption) (*Bucket, error) {
	defaultThreshold := uint64(10 * 1024 * 1024)
	defaultStrategy := StrategyReplace

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	sg, err := newSegmentGroup(dir, 3*time.Second, logger)
	if err != nil {
		return nil, errors.Wrap(err, "init disk segments")
	}

	b := &Bucket{
		dir:               dir,
		disk:              sg,
		memTableThreshold: defaultThreshold,
		strategy:          defaultStrategy,
		stopFlushCycle:    make(chan struct{}),
		logger:            logger,
	}

	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	if err := b.setNewActiveMemtable(); err != nil {
		return nil, err
	}

	if err := b.recoverFromCommitLogs(ctx); err != nil {
		return nil, err
	}

	b.initFlushCycle()

	return b, nil
}

func (b *Bucket) SetMemtableThreshold(size uint64) {
	b.memTableThreshold = size
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	v, err := b.active.get(key)
	if err == nil {
		// item found and no error, return and stop searching, since the strategy
		// is replace
		return v, nil
	}
	if err == Deleted {
		// deleted in the mem-table (which is always the latest) means we don't
		// have to check the disk segments, return nil now
		return nil, nil
	}

	if err != NotFound {
		panic("unsupported error in bucket.Get")
	}

	if b.flushing != nil {
		v, err := b.flushing.get(key)
		if err == nil {
			// item found and no error, return and stop searching, since the strategy
			// is replace
			return v, nil
		}
		if err == Deleted {
			// deleted in the now most recent memtable  means we don't have to check
			// the disk segments, return nil now
			return nil, nil
		}

		if err != NotFound {
			panic("unsupported error in bucket.Get")
		}
	}

	return b.disk.get(key)
}

func (b *Bucket) GetBySecondary(pos int, key []byte) ([]byte, error) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	v, err := b.active.getBySecondary(pos, key)
	if err == nil {
		// item found and no error, return and stop searching, since the strategy
		// is replace
		return v, nil
	}
	if err == Deleted {
		// deleted in the mem-table (which is always the latest) means we don't
		// have to check the disk segments, return nil now
		return nil, nil
	}

	if err != NotFound {
		panic("unsupported error in bucket.Get")
	}

	if b.flushing != nil {
		v, err := b.flushing.getBySecondary(pos, key)
		if err == nil {
			// item found and no error, return and stop searching, since the strategy
			// is replace
			return v, nil
		}
		if err == Deleted {
			// deleted in the now most recent memtable  means we don't have to check
			// the disk segments, return nil now
			return nil, nil
		}

		if err != NotFound {
			panic("unsupported error in bucket.Get")
		}
	}

	return b.disk.getBySecondary(pos, key)
}

func (b *Bucket) SetList(key []byte) ([][]byte, error) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	var out []value

	v, err := b.disk.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	out = v

	if b.flushing != nil {
		v, err = b.flushing.getCollection(key)
		if err != nil {
			if err != nil && err != NotFound {
				return nil, err
			}
		}
		out = append(out, v...)

	}

	v, err = b.active.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	if len(v) > 0 {
		// skip the expensive append operation if there was no memtable
		out = append(out, v...)
	}

	return newSetDecoder().Do(out), nil
}

func (b *Bucket) Put(key, value []byte, opts ...SecondaryKeyOption) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.put(key, value, opts...)
}

func (b *Bucket) SetAdd(key []byte, values [][]byte) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.append(key, newSetEncoder().Do(values))
}

func (b *Bucket) SetDeleteSingle(key []byte, valueToDelete []byte) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.append(key, []value{
		{
			value:     valueToDelete,
			tombstone: true,
		},
	})
}

type MapListOptionConfig struct {
	acceptDuplicates bool
}

type MapListOption func(c *MapListOptionConfig)

func MapListAcceptDuplicates() MapListOption {
	return func(c *MapListOptionConfig) {
		c.acceptDuplicates = true
	}
}

func (b *Bucket) MapList(key []byte, cfgs ...MapListOption) ([]MapPair, error) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	c := MapListOptionConfig{}
	for _, cfg := range cfgs {
		cfg(&c)
	}

	segments := [][]MapPair{}
	// before := time.Now()
	disk, err := b.disk.getCollectionBySegments(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}

	for i := range disk {
		segmentDecoded := make([]MapPair, len(disk[i]))
		for j, v := range disk[i] {
			if err := segmentDecoded[j].FromBytes(v.value, false); err != nil {
				return nil, err
			}
			segmentDecoded[j].Tombstone = v.tombstone
		}
		segments = append(segments, segmentDecoded)
	}

	// fmt.Printf("--map-list: get all disk segments took %s\n", time.Since(before))

	// before = time.Now()
	// fmt.Printf("--map-list: apend all disk segments took %s\n", time.Since(before))

	if b.flushing != nil {
		v, err := b.flushing.getMap(key)
		if err != nil {
			if err != nil && err != NotFound {
				return nil, err
			}
		}

		segments = append(segments, v)
	}

	// before = time.Now()
	v, err := b.active.getMap(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	segments = append(segments, v)
	// fmt.Printf("--map-list: get all active segments took %s\n", time.Since(before))

	// before = time.Now()
	for i := range segments {
		sort.Slice(segments[i], func(a, b int) bool {
			return bytes.Compare(segments[i][a].Key, segments[i][b].Key) == -1
		})
	}
	// fmt.Printf("--map-list: sort all segments took %s\n", time.Since(before))

	// before = time.Now()
	// defer func() {
	// 	fmt.Printf("--map-list: run decoder took %s\n", time.Since(before))
	// }()

	return newSortedMapMerger().do(segments)
}

func (b *Bucket) MapSet(rowKey []byte, kv MapPair) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.appendMapSorted(rowKey, kv)
}

func (b *Bucket) MapSetMulti(rowKey []byte, kvs []MapPair) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	for _, kv := range kvs {
		if err := b.active.appendMapSorted(rowKey, kv); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) MapDeleteKey(rowKey, mapKey []byte) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	pair := MapPair{
		Key:       mapKey,
		Tombstone: true,
	}

	return b.active.appendMapSorted(rowKey, pair)
}

func (b *Bucket) Delete(key []byte, opts ...SecondaryKeyOption) error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.setTombstone(key, opts...)
}

// meant to be called from situations where a lock is already held, does not
// lock on its own
func (b *Bucket) setNewActiveMemtable() error {
	mt, err := newMemtable(filepath.Join(b.dir, fmt.Sprintf("segment-%d",
		time.Now().UnixNano())), b.strategy, b.secondaryIndices)
	if err != nil {
		return err
	}

	b.active = mt
	return nil
}

func (b *Bucket) Count() int {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	if b.strategy != StrategyReplace {
		panic("Count() called on strategy other than 'replace'")
	}

	memtableCount := b.memtableNetCount(b.active.countStats())
	if b.flushing != nil {
		memtableCount += b.memtableNetCount(b.flushing.countStats())
	}
	diskCount := b.disk.count()

	return memtableCount + diskCount
}

func (b *Bucket) memtableNetCount(stats *countStats) int {
	netCount := 0

	// TODO: this uses regular get, given that this may be called quite commonly,
	// we might consider building a pure Exists(), which skips reading the value
	// and only checks for tombstones, etc.
	for _, key := range stats.upsertKeys {
		v, _ := b.disk.get(key) // current implementation can't error
		if v == nil {
			// this key didn't exist before
			netCount++
		}
	}

	for _, key := range stats.tombstonedKeys {
		v, _ := b.disk.get(key) // current implementation can't error
		if v != nil {
			// this key existed before
			netCount--
		}
	}

	return netCount
}

func (b *Bucket) Shutdown(ctx context.Context) error {
	if err := b.disk.shutdown(ctx); err != nil {
		return err
	}

	b.stopFlushCycle <- struct{}{}

	b.flushLock.Lock()
	if err := b.active.flush(); err != nil {
		return err
	}
	b.flushLock.Unlock()

	if b.flushing == nil {
		// active has flushing, no one else was currently flushing, it's safe to
		// exit
		return nil
	}

	// it seems we still need to wait for someone to finish flushing
	t := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t:
			if b.flushing == nil {
				return nil
			}
		}
	}
}

func (b *Bucket) initFlushCycle() {
	go func() {
		t := time.Tick(100 * time.Millisecond)
		for {
			select {
			case <-b.stopFlushCycle:
				return
			case <-t:
				b.flushLock.Lock()
				shouldSwitch := b.active.Size() >= b.memTableThreshold
				b.flushLock.Unlock()
				if shouldSwitch {
					if err := b.FlushAndSwitch(); err != nil {
						b.logger.WithField("action", "lsm_memtable_flush").
							WithField("path", b.dir).
							WithError(err).
							Errorf("flush and switch failed")
					}
				}
			}
		}
	}()
}

// FlushAndSwitch is typically called periodically and does not require manual
// calling, but there are some situations where this might be intended, such as
// in test scenarios or when a force flush is desired.
func (b *Bucket) FlushAndSwitch() error {
	before := time.Now()

	b.logger.WithField("action", "lsm_memtable_flush_start").
		WithField("path", b.dir).
		Trace("start flush and switch")
	if err := b.atomicallySwitchMemtable(); err != nil {
		return errors.Wrap(err, "switch active memtable")
	}

	if err := b.flushing.flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	if err := b.atomicallyAddDiskSegmentAndRemoveFlushing(); err != nil {
		return errors.Wrap(err, "add segment and remove flushing")
	}

	took := time.Since(before)
	b.logger.WithField("action", "lsm_memtable_flush_complete").
		WithField("path", b.dir).
		Trace("finish flush and switch")

	b.logger.WithField("action", "lsm_memtable_flush_complete").
		WithField("path", b.dir).
		WithField("took", took).
		Debugf("flush and switch took %s\n", took)

	return nil
}

func (b *Bucket) atomicallyAddDiskSegmentAndRemoveFlushing() error {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	path := b.flushing.path
	if err := b.disk.add(path + ".db"); err != nil {
		return err
	}
	b.flushing = nil

	return nil
}

func (b *Bucket) atomicallySwitchMemtable() error {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.flushing = b.active
	return b.setNewActiveMemtable()
}

func (b *Bucket) Strategy() string {
	return b.strategy
}

// the WAL uses a buffer and isn't written until the buffer size is crossed or
// this function explicitly called. This allows to safge unnecessary disk
// writes in larger operations, such as batches. It is sufficient to call write
// on the WAL just once. This does not make a batch atomic, but it guarantees
// that the WAL is written before a successful response is returned to the
// user.
func (b *Bucket) WriteWAL() error {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.writeWAL()
}
