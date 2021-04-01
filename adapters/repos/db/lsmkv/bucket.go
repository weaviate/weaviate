package lsmkv

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace = "replace"
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

	b.setNewActiveMemtable()
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

func (b *Bucket) Put(key, value []byte) error {
	return b.active.put(key, value)
}

func (b *Bucket) Delete(key []byte) error {
	return b.active.setTombstone(key)
}

// meant to be called from situations where a lock is already held, does not
// lock on its own
func (b *Bucket) setNewActiveMemtable() {
	b.active = newMemtable(filepath.Join(b.dir, fmt.Sprintf("segment-%d",
		time.Now().UnixNano())))
}

func (b *Bucket) Shutdown() error {
	// TODO: Orderly shutdown, flush active until everything persisted and all
	// commit logs are gone

	return nil
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
	b.atomicallySwitchMemtable()
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
	if err := b.disk.add(path); err != nil {
		return nil
	}
	b.flushing = nil

	return nil
}

func (b *Bucket) atomicallySwitchMemtable() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	b.flushing = b.active
	b.setNewActiveMemtable()
}
