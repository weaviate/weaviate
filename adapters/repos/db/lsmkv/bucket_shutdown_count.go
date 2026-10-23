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

package lsmkv

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/diskio"
)

// shutdownNetCount is the net count additions of the active memtable at
// shutdown. The shutdown leaves that memtable on disk as a segment or as a
// reused WAL, and neither carries a count sidecar yet: a segment gets one
// only when it is next loaded, and a WAL never. An unloaded shard's object
// count sums sidecars, so without this one it misses every write since the
// last regular flush.
//
// The count has to be taken before the segment group shuts down, because it
// checks the memtable's keys against the segments on disk.
type shutdownNetCount struct {
	memtable      memtable
	flushing      memtable
	commitlogSize int64
	value         int
}

// takeShutdownNetCount counts the active memtable against the segments on
// disk and a memtable still being flushed. It returns nil for buckets that do
// not count net additions.
func (b *Bucket) takeShutdownNetCount(ctx context.Context) (*shutdownNetCount, error) {
	if !b.calcCountNetAdditions || b.strategy != StrategyReplace {
		return nil, nil
	}

	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	b.waitForZeroWriters(b.active)

	segments, release := b.disk.getConsistentViewOfSegments()
	defer release()

	var flushing *countStats
	if b.flushing != nil {
		flushing = b.flushing.countStats()
	}
	value, err := b.memtableNetCount(ctx, b.active.countStats(), flushing, segments)
	if err != nil {
		return nil, err
	}
	return &shutdownNetCount{
		memtable:      b.active,
		flushing:      b.flushing,
		commitlogSize: b.active.commitlogSize(),
		value:         value,
	}, nil
}

// describes reports whether the count still describes the bucket's
// memtables: the WAL only grows, so an unchanged size means no write landed
// after the count.
func (c *shutdownNetCount) describes(active, flushing memtable) bool {
	return c != nil && c.memtable == active && c.flushing == flushing &&
		active.commitlogSize() == c.commitlogSize
}

// store writes the count as the sidecar of path, a segment or a WAL. The
// cold count fails on a torn sidecar, so it is synced before it is renamed
// into place. The rename also leaves the old file, which a backup may have
// linked, untouched.
func (c *shutdownNetCount) store(path string) error {
	sidecar := countNetPathFor(path)
	tmp := sidecar + ".tmp"
	if err := storeCountNetOnDisk(tmp, c.value, nil); err != nil {
		return err
	}
	if err := diskio.Fsync(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, sidecar); err != nil {
		return err
	}
	return diskio.Fsync(filepath.Dir(sidecar))
}

// countNetPathFor returns the count sidecar path of a segment or WAL file,
// named the way a segment names its own.
func countNetPathFor(path string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + CountNetAdditionsFileSuffix
}

// removeWALCountNet removes the sidecar a shutdown wrote for a reused WAL.
// Recovery replays the WAL into a memtable that takes further writes, so the
// sidecar would no longer describe it.
func removeWALCountNet(walPath string) error {
	if err := os.Remove(countNetPathFor(walPath)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
