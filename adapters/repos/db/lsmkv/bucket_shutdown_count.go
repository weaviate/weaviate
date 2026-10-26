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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// WALCountNetAdditionsFileSuffix names the count sidecar of a WAL that a
// shutdown kept. It differs from the segment suffix because older versions
// would take a .cna next to a WAL for the count of the segment they flush the
// WAL into, and never remove it.
const WALCountNetAdditionsFileSuffix = ".wal-cna"

// shutdownNetCount is the net count additions of the memtables at shutdown.
// The shutdown leaves them on disk as segments or as a reused WAL, and none
// carries a count sidecar yet: a segment gets one only when it is next
// loaded, and a WAL never. An unloaded shard's object count sums sidecars, so
// without these it misses every write since the last regular flush.
//
// The counts have to be taken before the segment group shuts down, because
// they check the memtables' keys against the segments on disk.
type shutdownNetCount struct {
	memtable      memtable
	flushing      memtable
	commitlogSize int64
	// value counts the active memtable against the flushing one and the disk
	value int
	// flushingValue counts the flushing memtable against the disk alone
	flushingValue int
}

// takeShutdownNetCount counts the active memtable, and a memtable still being
// flushed, against the segments on disk. It returns nil for buckets that do
// not count net additions, and an error if a key could not be looked up: a
// count that takes such a key as new would be stored and trusted by every
// later load.
func (b *Bucket) takeShutdownNetCount(ctx context.Context) (*shutdownNetCount, error) {
	if !b.calcCountNetAdditions || b.strategy != StrategyReplace {
		return nil, nil
	}

	// a flush in progress would clear b.flushing after the count, which then
	// no longer describes the bucket
	if err := b.lockFlushAndSwitch(ctx); err != nil {
		return nil, err
	}
	defer b.flushAndSwitchMu.Unlock()

	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	b.waitForZeroWriters(b.active)

	segments, release := b.disk.getConsistentViewOfSegments()
	defer release()

	c := &shutdownNetCount{
		memtable:      b.active,
		flushing:      b.flushing,
		commitlogSize: b.active.commitlogSize(),
	}

	var flushing *countStats
	if b.flushing != nil {
		flushing = b.flushing.countStats()
		value, unanswered, err := b.memtableNetCountWithUnanswered(ctx, flushing, nil, segments)
		if err := b.checkShutdownNetCount(unanswered, err); err != nil {
			return nil, err
		}
		c.flushingValue = value
	}

	value, unanswered, err := b.memtableNetCountWithUnanswered(ctx, b.active.countStats(), flushing, segments)
	if err := b.checkShutdownNetCount(unanswered, err); err != nil {
		return nil, err
	}
	c.value = value
	return c, nil
}

func (b *Bucket) checkShutdownNetCount(unanswered int, err error) error {
	if err != nil {
		return err
	}
	if unanswered > 0 {
		return fmt.Errorf("%d keys could not be looked up on disk", unanswered)
	}
	return nil
}

// describes reports whether the count still describes the bucket's
// memtables: the WAL only grows, so an unchanged size means no write landed
// after the count.
func (c *shutdownNetCount) describes(active, flushing memtable) bool {
	return c != nil && c.memtable == active && c.flushing == flushing &&
		active.commitlogSize() == c.commitlogSize
}

// storeSegmentNetCount writes value as the count sidecar of the segment at
// segmentPath, under the name the segment loads it from.
func (b *Bucket) storeSegmentNetCount(segmentPath string, value int) error {
	return b.storeSidecarAtomically(countNetPathFor(segmentPath), func(path string) error {
		return storeCountNetOnDisk(path, value, nil)
	})
}

// storeWALNetCount writes value, the count of the active memtable, as the
// sidecar of the WAL at walPath. The WAL's size is recorded with it: the WAL
// only grows, so a reader takes the count only while the WAL beside it still
// has that size. Any write after the shutdown, or a flush of the WAL into a
// segment by a version that does not know this sidecar, voids it.
func (b *Bucket) storeWALNetCount(walPath string, value int) error {
	info, err := os.Stat(walPath)
	if err != nil {
		return err
	}
	return b.storeSidecarAtomically(walCountNetPathFor(walPath), func(path string) error {
		rw := byteops.NewReadWriter(make([]byte, byteops.Uint32Len+2*byteops.Uint64Len))
		rw.MoveBufferPositionForward(byteops.Uint32Len) // leave space for checksum
		rw.WriteUint64(uint64(value))
		rw.WriteUint64(uint64(info.Size()))
		return writeWithChecksum(rw, path, nil)
	})
}

// storeSidecarAtomically writes a sidecar through a temporary file. The cold
// count fails on a torn sidecar, so it is synced before it is renamed into
// place. The rename also leaves the old file, which a backup may have linked,
// untouched.
func (b *Bucket) storeSidecarAtomically(sidecar string, write func(path string) error) error {
	tmp := sidecar + ".tmp"
	if err := write(tmp); err != nil {
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

func walCountNetPathFor(walPath string) string {
	return strings.TrimSuffix(walPath, filepath.Ext(walPath)) + WALCountNetAdditionsFileSuffix
}

// readWALCountNet returns the count a WAL sidecar holds and the WAL size it
// was taken at.
func readWALCountNet(path string) (count, walSize int64, err error) {
	data, err := loadWithChecksum(path, byteops.Uint32Len+2*byteops.Uint64Len, nil)
	if err != nil {
		return 0, 0, err
	}
	return int64(binary.LittleEndian.Uint64(data[0:8])), int64(binary.LittleEndian.Uint64(data[8:16])), nil
}

// CountFromSidecars sums the object counts the count sidecars of an objects
// bucket hold, which is how an unloaded shard is counted. files lists the
// bucket directory dir with the file sizes. A WAL's sidecar counts only while
// that WAL is still there at the size the sidecar records.
func CountFromSidecars(dir string, files map[string]int64) (int64, error) {
	total := int64(0)
	for file := range files {
		path := filepath.Join(dir, file)
		switch {
		case strings.HasSuffix(file, CountNetAdditionsFileSuffix):
			count, err := ReadCountNetAdditionsFile(path)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", path, err)
			}
			total += count
		case strings.HasSuffix(file, MetadataFileSuffix):
			count, err := ReadObjectCountFromMetadataFile(path)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", path, err)
			}
			total += count
		case strings.HasSuffix(file, WALCountNetAdditionsFileSuffix):
			walSize, ok := files[strings.TrimSuffix(file, WALCountNetAdditionsFileSuffix)+".wal"]
			if !ok {
				continue
			}
			count, recordedWALSize, err := readWALCountNet(path)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", path, err)
			}
			if recordedWALSize == walSize {
				total += count
			}
		}
	}
	return total, nil
}
