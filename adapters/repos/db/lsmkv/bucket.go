package lsmkv

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Bucket struct {
	dir      string
	active   *Memtable
	flushing *Memtable
	disk     *SegmentGroup

	// Lock() means a move from active to flushing is happening, RLock() is
	// normal operation
	flushLock sync.RWMutex

	memTableThreshold uint64
	strategy          string
}

func NewBucketWithStrategy(dir, strategy string) (*Bucket, error) {
	// TODO: check if there are open commit logs: recover

	// TODO: create folder if not exists

	sg, err := newSegmentGroup(dir)
	if err != nil {
		return nil, errors.Wrap(err, "init disk segments")
	}

	b := &Bucket{
		dir:               dir,
		disk:              sg,
		memTableThreshold: 10 * 1024 * 1024,
		strategy:          strategy,
	}

	if err := b.setNewActiveMemtable(); err != nil {
		return nil, err
	}
	b.initFlushCycle()

	return b, nil
}

func NewBucket(dir string) (*Bucket, error) {
	return NewBucketWithStrategy(dir, StrategyReplace)
}

func (b *Bucket) SetMemtableThreshold(size uint64) {
	b.memTableThreshold = size
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	// TODO: allow other strategies than latest

	v, err := b.active.get(key)
	switch err {
	case nil:
		// item found and no error, return and stop searching, since the strategy
		// is replace
		return v, nil
	case Deleted:
		// deleted in the mem-table (which is always the latest) means we don't
		// have to check the disk segments, return nil now
		return nil, nil
	case NotFound:
		return b.disk.get(key)
	default:
		panic("unsupported error in memtable.Get")
	}
}

func (b *Bucket) GetCollection(key []byte) ([][]byte, error) {
	var out []value

	v, err := b.disk.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	out = append(out, v...)

	v, err = b.active.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	out = append(out, v...)

	return b.decodeCollectionValue(out), nil
}

// iterates twice to see if a value ends in a tombstone, if yes, omits it
func (b *Bucket) decodeCollectionValue(in []value) [][]byte {
	return newSetDecoder().Do(in)
}

func (b *Bucket) Put(key, value []byte) error {
	return b.active.put(key, value)
}

func (b *Bucket) Append(key []byte, values [][]byte) error {
	return b.active.append(key, b.encodeSetValues(values))
}

func (b *Bucket) DeleteFromCollection(key []byte, valueToDelete []byte) error {
	return b.active.append(key, []value{
		{
			value:     valueToDelete,
			tombstone: true,
		},
	})
}

func (b *Bucket) encodeSetValues(in [][]byte) []value {
	return newSetEncoder().Do(in)
}

func (b *Bucket) MapList(key []byte) ([]MapPair, error) {
	var raw []value

	v, err := b.disk.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	raw = append(raw, v...)

	v, err = b.active.getCollection(key)
	if err != nil {
		if err != nil && err != NotFound {
			return nil, err
		}
	}
	raw = append(raw, v...)

	return b.decodeMapCollectionValues(raw)
}

func (b *Bucket) decodeMapCollectionValues(in []value) ([]MapPair, error) {
	out := make([]MapPair, len(in))

	for i, value := range in {
		// TODO: handle tombstones
		kv := MapPair{}
		err := kv.FromBytes(value.value)
		if err != nil {
			return nil, err
		}
		out[i] = kv
	}

	return b.mergeDuplicateMapKeys(out), nil
}

// assumes a latest-takes-presence strategy
// iterates the array twice, first to register duplicates with counts, then a
// second time to skip all but the last count of a duplicate. possibly not a
// perfect algo, but considerably better than some O(n^2) attempt
func (b *Bucket) mergeDuplicateMapKeys(in []MapPair) []MapPair {
	seenKeys := map[string]uint{}

	for _, pair := range in {
		count := seenKeys[string(pair.Key)]
		seenKeys[string(pair.Key)] = count + 1
	}

	out := make([]MapPair, len(in))
	i := 0
	for _, pair := range in {
		count := seenKeys[string(pair.Key)]
		if count == 1 {
			out[i] = pair
			i++
			continue
		}

		seenKeys[string(pair.Key)] = count - 1
	}

	return out[:i]
}

func (b *Bucket) MapSet(rowKey []byte, kv MapPair) error {
	v, err := b.encodeMapValue(kv)
	if err != nil {
		return err
	}

	return b.active.append(rowKey, v)
}

func (b *Bucket) encodeMapValue(kv MapPair) ([]value, error) {
	v, err := kv.Bytes()
	if err != nil {
		return nil, err
	}

	out := make([]value, 1)
	out[0] = value{
		tombstone: false,
		value:     v,
	}

	return out, nil
}

func (b *Bucket) Delete(key []byte) error {
	return b.active.setTombstone(key)
}

// meant to be called from situations where a lock is already held, does not
// lock on its own
func (b *Bucket) setNewActiveMemtable() error {
	mt, err := newMemtable(filepath.Join(b.dir, fmt.Sprintf("segment-%d",
		time.Now().UnixNano())), b.strategy)
	if err != nil {
		return err
	}

	b.active = mt
	return nil
}

func (b *Bucket) Shutdown(ctx context.Context) error {
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
			<-t
			fmt.Printf("current size: %d\nthreshold: %d\n\n", b.active.Size(), b.memTableThreshold)
			if b.active.Size() >= b.memTableThreshold {
				if err := b.FlushAndSwitch(); err != nil {
					// TODO: structured logging
					fmt.Printf("Error flush and switch: %v\n", err)
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
	fmt.Printf("start flush and switch\n")
	if err := b.atomicallySwitchMemtable(); err != nil {
		return err
	}

	if err := b.flushing.flush(); err != nil {
		return err
	}

	if err := b.atomicallyAddDiskSegmentAndRemoveFlushing(); err != nil {
		return err
	}

	fmt.Printf("flush and switch took %s\n", time.Since(before))

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
