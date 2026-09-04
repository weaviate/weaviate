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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
)

// A corrupt index must report an error, never lsmkv.NotFound: read as absence,
// a scan falls through to an older segment and answers with a stale value.
func TestBucketReadsOnCorruptSegmentIndexError(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	for _, read := range corruptIndexReads() {
		t.Run(read.name, func(t *testing.T) {
			dir := t.TempDir()
			newBucket := func() *Bucket {
				return openCorruptTestBucket(t, ctx, dir, logger, read.opts...)
			}

			keys := make([][]byte, 16)
			b := newBucket()
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				read.write(t, b, keys[i], i)
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, read.read(t, b, keys[0], 0), "healthy read before corrupting")
			require.NoError(t, b.Shutdown(ctx))

			corruptRootChildPointers(t, dir)

			b = newBucket()
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

			// probes descend through the root, so each meets the bad pointer
			// unless it matches the root key itself
			var errs int
			for i, key := range keys {
				err := read.read(t, b, key, i)
				if err == nil {
					continue
				}
				errs++
				require.NotErrorIs(t, err, lsmkv.NotFound,
					"a corrupt index must not read as an absent key")
			}
			require.NotZero(t, errs, "no probe reached the corrupt pointer")
		})
	}
}

// The journey the error exists for: a corrupt newer segment must not let Get
// answer from the older one.
func TestBucketGetOnCorruptNewerSegmentDoesNotServeStaleValue(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()

	newBucket := func() *Bucket {
		return openCorruptTestBucket(t, ctx, dir, logger, WithStrategy(StrategyReplace))
	}

	keys := make([][]byte, 16)
	b := newBucket()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("stale-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	for i, key := range keys {
		require.NoError(t, b.Put(key, []byte(fmt.Sprintf("current-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	value, err := b.Get(keys[0])
	require.NoError(t, err)
	require.Equal(t, "current-000", string(value), "the newer segment must win")
	require.NoError(t, b.Shutdown(ctx))

	corruptSegmentByAge(t, dir, false)

	b = newBucket()
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	var errs int
	for _, key := range keys {
		value, err := b.Get(key)
		if err == nil {
			require.NotContains(t, string(value), "stale",
				"a corrupt newer segment must not fall through to the older one")
			continue
		}
		errs++
		require.NotErrorIs(t, err, lsmkv.NotFound)
	}
	require.NotZero(t, errs, "no probe reached the corrupt pointer")
}

// A collection read walks every segment, so one unreadable segment fails it: a
// short set is indistinguishable from a complete one.
func TestBucketCollectionReadOverTwoSegmentsFailsOnCorruptOne(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	reads := []struct {
		name string
		read func(b *Bucket, key []byte) ([][]byte, error)
	}{
		{"SetList", func(b *Bucket, key []byte) ([][]byte, error) { return b.SetList(key) }},
		{"SetRawList", func(b *Bucket, key []byte) ([][]byte, error) { return b.SetRawList(key) }},
	}

	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			dir := t.TempDir()
			newBucket := func() *Bucket {
				return openCorruptTestBucket(t, ctx, dir, logger, WithStrategy(StrategySetCollection))
			}

			keys := make([][]byte, 16)
			b := newBucket()
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				require.NoError(t, b.SetAdd(keys[i], [][]byte{[]byte(fmt.Sprintf("old-%03d", i))}))
			}
			require.NoError(t, b.FlushAndSwitch())
			for i, key := range keys {
				require.NoError(t, b.SetAdd(key, [][]byte{[]byte(fmt.Sprintf("new-%03d", i))}))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			// the older of the two, so the newer one still holds every key
			corruptSegmentByAge(t, dir, true)

			b = newBucket()
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

			var errs int
			for _, key := range keys {
				values, err := read.read(b, key)
				if err == nil {
					// the only key a corrupt-root descent still answers is the
					// root's own, and it must carry both segments' entries
					require.Len(t, values, 2,
						"a read that succeeded returned a short set for %s", key)
					continue
				}
				errs++
				require.NotErrorIs(t, err, lsmkv.NotFound)
				require.Nil(t, values,
					"a read that cannot see every segment must not return a partial set")
			}
			require.Equal(t, len(keys)-1, errs,
				"every key below the corrupt root must fail the read")
		})
	}
}

// A flush must survive an unreadable lower segment: its commit log is already
// gone. An unresolved key inflates the count, never drops it.
func TestBucketFlushOverCorruptLowerSegment(t *testing.T) {
	ctx := context.Background()

	rounds := []struct {
		name string
		// same keys as round one, so the bloom filter passes and the count must
		// descend the corrupt segment
		write  func(t *testing.T, b *Bucket, key []byte, i int)
		verify func(t *testing.T, b *Bucket, keys [][]byte, count int)
	}{
		{
			name: "writes over it",
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.Put(key, []byte(fmt.Sprintf("v2-%03d", i))))
			},
			verify: func(t *testing.T, b *Bucket, keys [][]byte, count int) {
				for i, key := range keys {
					value, err := b.Get(key)
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("v2-%03d", i), string(value))
				}
				require.Greater(t, count, len(keys),
					"a key the lower segment could not answer for is counted as new, so "+
						"the total exceeds what the bucket holds")
			},
		},
		{
			name: "deletes over it",
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.Delete(key))
			},
			verify: func(t *testing.T, b *Bucket, keys [][]byte, count int) {
				require.Positive(t, count,
					"a delete the lower segment cannot confirm must not subtract, or the "+
						"count would fall short of what the bucket may still hold")
			},
		},
	}

	// the two files a segment's count can reach disk in
	sidecars := []struct {
		name          string
		writeMetadata bool
		suffix        string
	}{
		{name: "cna", writeMetadata: false, suffix: ".cna"},
		{name: "metadata", writeMetadata: true, suffix: ".metadata"},
	}

	for _, round := range rounds {
		for _, sidecar := range sidecars {
			t.Run(round.name+"/"+sidecar.name, func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				dir := t.TempDir()

				keys := make([][]byte, 16)
				b := countingBucket(t, ctx, dir, logger, sidecar.writeMetadata)
				for i := range keys {
					keys[i] = []byte(fmt.Sprintf("key-%03d", i))
					require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("v1-%03d", i))))
				}
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.Shutdown(ctx))

				repair := corruptRootChildPointers(t, dir)

				b = countingBucket(t, ctx, dir, logger, sidecar.writeMetadata)
				shutdown := func() {
					// a stuck flush never clears b.flushing and Shutdown polls it without a
					// deadline; bound it so a regression here fails in seconds, not by
					// hanging the package
					shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					require.NoError(t, b.Shutdown(shutdownCtx))
				}
				defer shutdown()

				for i, key := range keys {
					round.write(t, b, key, i)
				}
				require.NoError(t, b.FlushAndSwitch(),
					"a flush must not fail because a lower segment's index is unreadable")

				count, err := b.Count(ctx)
				require.NoError(t, err)
				round.verify(t, b, keys, count)

				// the log is the only thing that tells an operator the count is inflated
				var inflated int
				for _, entry := range hook.AllEntries() {
					if strings.Contains(entry.Message, "object count takes") {
						inflated++
						require.Equal(t, logrus.ErrorLevel, entry.Level)
					}
				}
				require.NotZero(t, inflated, "an incomplete count must say so")

				// and the segment is complete, sidecar included
				segments, err := filepath.Glob(filepath.Join(dir, "*.db"))
				require.NoError(t, err)
				require.Len(t, segments, 2,
					"expected the corrupt segment and the one flushed over it")
				sort.Strings(segments)
				newest := strings.TrimSuffix(segments[len(segments)-1], ".db")
				require.FileExists(t, newest+sidecar.suffix)

				// count read back after restart is what the flush wrote, not recomputed:
				// repairing first would let a recompute resolve every key and answer lower
				shutdown()
				repair()
				reopened := countingBucket(t, ctx, dir, logger, sidecar.writeMetadata)
				t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })
				b = reopened
				reloaded, err := reopened.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, count, reloaded,
					"the sidecar must report what the flush counted, not a fresh count")
			})
		}
	}
}

// Every read of a corrupt segment meets the same error, so it names itself once
// rather than once per read. An operator still needs the file.
func TestBucketReadOnCorruptSegmentNamesTheSegmentOnce(t *testing.T) {
	ctx := context.Background()

	for _, read := range corruptIndexReads() {
		t.Run(read.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			dir := t.TempDir()
			newBucket := func() *Bucket {
				return openCorruptTestBucket(t, ctx, dir, logger, read.opts...)
			}

			keys := make([][]byte, 16)
			b := newBucket()
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				read.write(t, b, keys[i], i)
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			corruptRootChildPointers(t, dir)

			b = newBucket()
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

			var failed int
			for i, key := range keys {
				if err := read.read(t, b, key, i); err != nil {
					failed++
					require.ErrorIs(t, err, lsmkv.ErrCorruptIndex)
				}
			}
			require.Greater(t, failed, 1, "several reads must meet the same corrupt segment")

			var warned int
			for _, entry := range hook.AllEntries() {
				if entry.Data["action"] != "lsmkv_corrupt_index" {
					continue
				}
				warned++
				require.Equal(t, logrus.ErrorLevel, entry.Level)
				require.Contains(t, entry.Data, "path", "the operator needs the file to repair")
			}
			require.NotZero(t, warned, "an unreadable segment must be reported at all")
			require.Equal(t, 1, warned, "one unreadable segment is one log line, not one per read")
		})
	}
}

// A cursor cannot fold the error into its state, so it panics rather than
// iterating a partial segment.
func TestBucketCursorOnCorruptSegmentPanics(t *testing.T) {
	ctx := context.Background()

	replaceOpts := []BucketOption{WithStrategy(StrategyReplace), WithSecondaryIndices(1)}
	writeReplace := func(t *testing.T, b *Bucket, key []byte, i int) {
		require.NoError(t, b.Put(key, []byte(fmt.Sprintf("v-%03d", i)),
			WithSecondaryKey(0, key)))
	}

	cursors := []struct {
		name  string
		opts  []BucketOption
		write func(t *testing.T, b *Bucket, key []byte, i int)
		walk  func(t *testing.T, b *Bucket, probe []byte)
	}{
		{
			// First/Next walk the payload in order; only Seek descends the index
			name: "Cursor/Seek", opts: replaceOpts, write: writeReplace,
			walk: func(t *testing.T, b *Bucket, probe []byte) {
				c := b.Cursor()
				defer c.Close()
				c.Seek(probe)
			},
		},
		{
			name: "CursorReplaceReusable/Seek", opts: replaceOpts, write: writeReplace,
			walk: func(t *testing.T, b *Bucket, probe []byte) {
				c := b.CursorReplaceReusable()
				defer c.Close()
				c.Seek(probe)
			},
		},
		{
			// a secondary index has no sequential order, so every call descends it
			name: "CursorWithSecondaryIndex/First", opts: replaceOpts, write: writeReplace,
			walk: func(t *testing.T, b *Bucket, probe []byte) {
				c := b.CursorWithSecondaryIndex(0)
				defer c.Close()
				c.First()
			},
		},
		{
			// the only index read the roaring-set cursor makes, via SeekPayloadStart
			name: "CursorRoaringSet/Seek",
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(i)}))
			},
			walk: func(t *testing.T, b *Bucket, probe []byte) {
				c := b.CursorRoaringSet()
				defer c.Close()
				c.Seek(probe)
			},
		},
	}

	for _, cursor := range cursors {
		t.Run(cursor.name, func(t *testing.T) {
			// one bucket per row, so each segment's corruptIndexReportOnce is unspent
			dir := t.TempDir()
			logger, hook := test.NewNullLogger()
			newBucket := func() *Bucket {
				return openCorruptTestBucket(t, ctx, dir, logger, cursor.opts...)
			}

			keys := make([][]byte, 16)
			b := newBucket()
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				cursor.write(t, b, keys[i], i)
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			corruptRootChildPointers(t, dir)

			b = newBucket()
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

			require.Panics(t, func() { cursor.walk(t, b, keys[8]) },
				"a cursor must not iterate past a segment it cannot read")

			var named int
			for _, entry := range hook.AllEntries() {
				if entry.Data["action"] == "lsmkv_corrupt_index" {
					named++
					require.Contains(t, entry.Data, "path",
						"the panic names a node offset; only the log names the file")
				}
			}
			require.Equal(t, 1, named,
				"a cursor that dies on a corrupt segment must name it first")
		})
	}
}

// A lazy load fails inside getConsistentViewOfSegments, holding the maintenance
// read lock and refs on earlier segments. Leak either and compaction blocks
// forever and Shutdown never drains.
func TestLazySegmentThatCannotLoadLeavesTheBucketDrainable(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()

	newBucket := func() *Bucket {
		return openCorruptTestBucket(t, ctx, dir, logger,
			WithStrategy(StrategyReplace), WithLazySegmentLoading(true))
	}

	keys := make([][]byte, 16)
	b := newBucket()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("v1-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())
	for i, key := range keys {
		require.NoError(t, b.Put(key, []byte(fmt.Sprintf("v2-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 2, "expected two flushed segments")
	sort.Strings(matches)
	setHeaderIndexStartPastFile(t, matches[len(matches)-1])

	b = newBucket()

	// the read that loads the segment, and the panic the load owes its caller
	require.Panics(t, func() { _, _ = b.Get(keys[0]) },
		"a segment that cannot be read must not answer as though it held nothing")

	// the compaction cycle takes this lock, so a read lock the panic unwound
	// through holds it off forever. Taking it is also what makes the counts below
	// a consistent read.
	require.True(t, b.disk.maintenanceLock.TryLock(),
		"the maintenance write lock is still held by the unwound read")
	refs := make([]int, len(b.disk.segments))
	for i, seg := range b.disk.segments {
		refs[i] = seg.getRefs()
	}
	b.disk.maintenanceLock.Unlock()

	// every reference the unwound read took is back. Asserted here rather than
	// through Shutdown alone: waitForReferenceCountToReachZero polls without a
	// deadline, so a stranded reference hangs the shutdown instead of failing it
	for i, count := range refs {
		require.Zero(t, count,
			"segment %d still carries a reference taken before the panic", i)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(shutdownCtx))
}

// countingBucket keeps the net-additions count, without which a flush consults
// the lower segments for nothing. writeMetadata picks the sidecar it lands in.
func countingBucket(t *testing.T, ctx context.Context, dir string, logger logrus.FieldLogger,
	writeMetadata bool,
) *Bucket {
	t.Helper()

	return openCorruptTestBucket(t, ctx, dir, logger,
		WithStrategy(StrategyReplace), WithCalcCountNetAdditions(true),
		WithWriteMetadata(writeMetadata))
}

func openCorruptTestBucket(t *testing.T, ctx context.Context, dir string,
	logger logrus.FieldLogger, opts ...BucketOption,
) *Bucket {
	t.Helper()

	b, err := tryOpenCorruptTestBucket(ctx, dir, logger, opts...)
	require.NoError(t, err)
	return b
}

// tryOpenCorruptTestBucket is openCorruptTestBucket without the assertion, for a
// segment that must not open at all.
func tryOpenCorruptTestBucket(ctx context.Context, dir string,
	logger logrus.FieldLogger, opts ...BucketOption,
) (*Bucket, error) {
	return NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...)
}

// corruptRootChildPointers points every index root's children past the end of
// the file, the shape a torn write leaves. The repair restores the bytes.
func corruptRootChildPointers(t *testing.T, dir string) (repair func()) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	before, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	corruptSegmentRootChildPointers(t, matches[0])

	return func() {
		require.NoError(t, os.WriteFile(matches[0], before, 0o644))
	}
}

// corruptSegmentByAge damages one of several segments. Names carry the flush
// timestamp, so they sort oldest-first.
func corruptSegmentByAge(t *testing.T, dir string, oldest bool) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Greater(t, len(matches), 1, "expected more than one flushed segment")

	sort.Strings(matches)
	if oldest {
		corruptSegmentRootChildPointers(t, matches[0])
		return
	}
	corruptSegmentRootChildPointers(t, matches[len(matches)-1])
}

func corruptSegmentRootChildPointers(t *testing.T, path string) {
	t.Helper()

	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
	require.NoError(t, err)

	// with secondary indices the header points at their offset table, and the
	// primary index starts after it. Damage every index the segment carries, so
	// a read descending any of them meets the corruption.
	starts := []uint64{header.IndexStart}
	if header.SecondaryIndices > 0 {
		starts[0] = header.IndexStart + uint64(header.SecondaryIndices)*8
		for i := uint64(0); i < uint64(header.SecondaryIndices); i++ {
			starts = append(starts, binary.LittleEndian.Uint64(contents[header.IndexStart+i*8:]))
		}
	}

	past := uint64(len(contents)) + 1
	for _, start := range starts {
		// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8]
		keyLen := binary.LittleEndian.Uint32(contents[start:])
		childBase := start + 4 + uint64(keyLen) + 16
		require.Less(t, childBase+16, uint64(len(contents)), "root node past the file")
		binary.LittleEndian.PutUint64(contents[childBase:], past)
		binary.LittleEndian.PutUint64(contents[childBase+8:], past)
	}

	require.NoError(t, os.WriteFile(path, contents, 0o644))
}

// Only the segment's own bounds tell a corrupt offset from a live one. Without
// them a read sizes its buffer from the damaged number: 1<<40 asks a terabyte.
func TestBucketGetOnCorruptPayloadRangeErrors(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name string
		end  uint64
	}{
		{name: "one byte past the data section", end: 0},
		{name: "a terabyte", end: 1 << 40},
		{name: "large enough to fail the allocation outright", end: 1 << 62},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			newBucket := func() *Bucket {
				return openCorruptTestBucket(t, ctx, dir, logger, WithStrategy(StrategyReplace))
			}

			keys := make([][]byte, 16)
			b := newBucket()
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("value-%03d", i))))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			rootKey := corruptRootPayloadEnd(t, dir, tc.end)

			b = newBucket()
			t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

			_, err := b.Get(rootKey)
			require.Error(t, err)
			require.ErrorIs(t, err, lsmkv.ErrCorruptIndex,
				"a payload range outside the segment must be refused, not sized into a read")
			require.NotErrorIs(t, err, lsmkv.NotFound)
		})
	}
}

// corruptRootPayloadEnd rewrites the root's payload end and returns the key that
// resolves to it. Zero means one byte past the data section.
func corruptRootPayloadEnd(t *testing.T, dir string, end uint64) []byte {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	contents, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
	require.NoError(t, err)

	// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8]
	start := header.IndexStart
	keyLen := binary.LittleEndian.Uint32(contents[start:])
	keyAt := start + 4
	endAt := keyAt + uint64(keyLen) + 8
	require.Less(t, endAt+8, uint64(len(contents)), "root node past the file")

	if end == 0 {
		end = header.IndexStart + 1 // the data section ends where the index begins
	}
	binary.LittleEndian.PutUint64(contents[endAt:], end)
	require.NoError(t, os.WriteFile(matches[0], contents, 0o644))

	return bytes.Clone(contents[keyAt : keyAt+uint64(keyLen)])
}

// An inverted index is bounded by its key region, not the whole data section:
// a node in the header gap must be refused, not read as no posting.
func TestInvertedSegmentIndexIsBoundedByItsKeyRegion(t *testing.T) {
	ctx := context.Background()
	logger, hook := test.NewNullLogger()
	dir := t.TempDir()

	newBucket := func() *Bucket {
		return openCorruptTestBucket(t, ctx, dir, logger, WithStrategy(StrategyInverted))
	}

	b := newBucket()
	for i := 0; i < 16; i++ {
		require.NoError(t, b.MapSet([]byte(fmt.Sprintf("term-%03d", i)),
			NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	rootKey, keysOffset := corruptRootValueStartIntoHeaderGap(t, dir)
	require.Greater(t, keysOffset, uint64(segmentindex.HeaderSize),
		"the fixture needs a gap between the segment header and the key region")

	b = newBucket()
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	view := b.GetConsistentView()
	defer view.ReleaseView()
	_, _, _, err := b.createDiskTermFromCV(ctx, view, 16, nil,
		[]string{rootKey}, "", 1, []int{1}, schema.BM25Config{K1: 1.2, B: 0.75})
	require.NoError(t, err, "the BM25 path reports no posting rather than failing")

	var warned int
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "lsmkv_corrupt_index" {
			warned++
		}
	}
	require.NotZero(t, warned,
		"a node addressing the header area must be refused, and the segment named")
}

// LoadHeaderInverted validates nothing, so a key region running into the index
// blob must fail to open rather than serve index bytes as payload.
func TestInvertedSegmentWithKeyRegionOutsideItsDataFailsToOpen(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	corruptions := []struct {
		name string
		// what the error must say, so one row cannot pass on the other's
		corrupt func(t *testing.T, dir string) string
	}{
		{name: "keys begin inside the header", corrupt: pullKeysOffsetIntoHeader},
		{name: "tombstones begin past the index", corrupt: pushTombstoneOffsetPastIndexStart},
	}

	for _, corruption := range corruptions {
		t.Run(corruption.name, func(t *testing.T) {
			dir := t.TempDir()
			newBucket := func() (*Bucket, error) {
				return tryOpenCorruptTestBucket(ctx, dir, logger, WithStrategy(StrategyInverted))
			}

			b, err := newBucket()
			require.NoError(t, err)
			for i := 0; i < 16; i++ {
				require.NoError(t, b.MapSet([]byte(fmt.Sprintf("term-%03d", i)),
					NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			wantMessage := corruption.corrupt(t, dir)

			_, err = newBucket()
			require.Error(t, err, "a key region outside the data section must not open")
			require.ErrorIs(t, err, lsmkv.ErrCorruptIndex)
			require.ErrorContains(t, err, wantMessage)
		})
	}
}

// pullKeysOffsetIntoHeader aims the key region at the segment header.
func pullKeysOffsetIntoHeader(t *testing.T, dir string) string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	contents, err := os.ReadFile(matches[0])
	require.NoError(t, err)

	// inverted header layout: [keysOffset:8][tombstoneOffset:8][propertyLengthsOffset:8]
	keysAt := uint64(segmentindex.HeaderSize)
	dataStart := uint64(segmentindex.HeaderSize) + segmentindex.HeaderInvertedSize
	require.Greater(t, binary.LittleEndian.Uint64(contents[keysAt:]), dataStart-1,
		"fixture must start with a key region inside the data section")

	binary.LittleEndian.PutUint64(contents[keysAt:], 0)
	require.NoError(t, os.WriteFile(matches[0], contents, 0o644))

	return "key region starts at 0, inside the header"
}

// pushTombstoneOffsetPastIndexStart overlaps the key region with the index blob.
func pushTombstoneOffsetPastIndexStart(t *testing.T, dir string) string {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	contents, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
	require.NoError(t, err)

	// inverted header layout: [keysOffset:8][tombstoneOffset:8][propertyLengthsOffset:8]
	tombstoneAt := uint64(segmentindex.HeaderSize) + 8
	before := binary.LittleEndian.Uint64(contents[tombstoneAt:])
	require.Less(t, before, header.IndexStart, "fixture must start inside the data section")

	binary.LittleEndian.PutUint64(contents[tombstoneAt:], header.IndexStart+8)
	require.NoError(t, os.WriteFile(matches[0], contents, 0o644))

	return fmt.Sprintf("ends outside the index at %d", header.IndexStart)
}

// corruptRootValueStartIntoHeaderGap points the root's value between the header
// and the key region — inside the data section, so only the inverted header's
// bounds reject it. Returns that node's key and the key region's start.
func corruptRootValueStartIntoHeaderGap(t *testing.T, dir string) (string, uint64) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	contents, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	header, err := segmentindex.ParseHeader(contents[:segmentindex.HeaderSize])
	require.NoError(t, err)
	inverted, err := segmentindex.LoadHeaderInverted(
		contents[segmentindex.HeaderSize : segmentindex.HeaderSize+segmentindex.HeaderInvertedSize])
	require.NoError(t, err)

	// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8]
	start := header.IndexStart
	keyLen := binary.LittleEndian.Uint32(contents[start:])
	valueAt := start + 4 + uint64(keyLen)
	require.Less(t, valueAt+16, uint64(len(contents)), "root node past the file")

	gap := uint64(segmentindex.HeaderSize) + 1
	binary.LittleEndian.PutUint64(contents[valueAt:], gap)
	binary.LittleEndian.PutUint64(contents[valueAt+8:], gap+8)
	require.NoError(t, os.WriteFile(matches[0], contents, 0o644))

	return string(contents[start+4 : start+4+uint64(keyLen)]), inverted.KeysOffset
}

// Checksum validation is opt-in and off here, so nothing else stops a cursor
// slicing on a header whose offsets do not fit the file.
func TestSegmentWithHeaderOutsideItsFileFailsToOpen(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	// each row reaches a different arm: an index start past the file is the one
	// PrimaryIndex slices on, and a zero start is the one that puts the data
	// section's end before its beginning
	corruptions := []struct {
		name    string
		corrupt func(t *testing.T, path string)
	}{
		{name: "index start past the file", corrupt: setHeaderIndexStartPastFile},
		{name: "index start before the header", corrupt: zeroHeaderIndexStart},
	}

	for _, corruption := range corruptions {
		t.Run(corruption.name, func(t *testing.T) {
			dir := t.TempDir()
			newBucket := func() (*Bucket, error) {
				return tryOpenCorruptTestBucket(ctx, dir, logger, WithStrategy(StrategyRoaringSet),
					WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			}

			b, err := newBucket()
			require.NoError(t, err)
			for i := 0; i < 8; i++ {
				require.NoError(t, b.RoaringSetAddList([]byte(fmt.Sprintf("key-%03d", i)), []uint64{uint64(i)}))
			}
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
			require.NoError(t, err)
			require.Len(t, matches, 1, "expected exactly one flushed segment")
			corruption.corrupt(t, matches[0])

			_, err = newBucket()
			require.Error(t, err, "a segment whose header does not fit its file must not open")
			require.ErrorIs(t, err, lsmkv.ErrCorruptIndex)
		})
	}
}

// zeroHeaderIndexStart puts the data section's end before its beginning.
func zeroHeaderIndexStart(t *testing.T, path string) {
	t.Helper()
	setHeaderIndexStart(t, path, 0)
}

// setHeaderIndexStartPastFile points the index start one byte beyond the file,
// which is what PrimaryIndex slices on.
func setHeaderIndexStartPastFile(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err)
	setHeaderIndexStart(t, path, uint64(info.Size())+1)
}

func setHeaderIndexStart(t *testing.T, path string, start uint64) {
	t.Helper()

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	// header layout: [level:2][version:2][secondaryIndices:2][strategy:2][indexStart:8]
	binary.LittleEndian.PutUint64(contents[8:16], start)
	require.NoError(t, os.WriteFile(path, contents, 0o644))
}

// corruptIndexReads is one read per bucket API that resolves through the disk
// index, and what it takes to reach it. Each returns the error the bucket API
// gives its caller.
func corruptIndexReads() []struct {
	name  string
	opts  []BucketOption
	write func(t *testing.T, b *Bucket, key []byte, i int)
	read  func(t *testing.T, b *Bucket, key []byte, i int) error
} {
	return []struct {
		name  string
		opts  []BucketOption
		write func(t *testing.T, b *Bucket, key []byte, i int)
		read  func(t *testing.T, b *Bucket, key []byte, i int) error
	}{
		{
			name: "Get", opts: []BucketOption{WithStrategy(StrategyReplace)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.Put(key, []byte(fmt.Sprintf("value-%03d", i))))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				value, err := b.Get(key)
				if err == nil {
					require.Equal(t, fmt.Sprintf("value-%03d", i), string(value))
				}
				return err
			},
		},
		{
			name: "Exists", opts: []BucketOption{WithStrategy(StrategyReplace)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.Put(key, []byte(fmt.Sprintf("value-%03d", i))))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				exists, err := b.Exists(key)
				if err == nil {
					require.True(t, exists, "a key the segment holds must exist")
				}
				return err
			},
		},
		{
			name: "GetBySecondary",
			opts: []BucketOption{WithStrategy(StrategyReplace), WithSecondaryIndices(1)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.Put(key, []byte(fmt.Sprintf("value-%03d", i)),
					WithSecondaryKey(0, key)))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				value, err := b.GetBySecondary(context.Background(), 0, key)
				if err == nil {
					require.Equal(t, fmt.Sprintf("value-%03d", i), string(value))
				}
				return err
			},
		},
		{
			name: "SetList", opts: []BucketOption{WithStrategy(StrategySetCollection)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.SetAdd(key, [][]byte{[]byte(fmt.Sprintf("v-%03d", i))}))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				values, err := b.SetList(key)
				if err == nil {
					require.Equal(t, [][]byte{[]byte(fmt.Sprintf("v-%03d", i))}, values)
				}
				return err
			},
		},
		{
			name: "SetRawList", opts: []BucketOption{WithStrategy(StrategySetCollection)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.SetAdd(key, [][]byte{[]byte(fmt.Sprintf("v-%03d", i))}))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				values, err := b.SetRawList(key)
				if err == nil {
					require.Equal(t, [][]byte{[]byte(fmt.Sprintf("v-%03d", i))}, values)
				}
				return err
			},
		},
		{
			name: "RoaringSetGet",
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(i)}))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				bm, release, err := b.RoaringSetGet(context.Background(), key)
				if err == nil {
					defer release()
					require.True(t, bm.Contains(uint64(i)))
				}
				return err
			},
		},
		{
			name: "MapList", opts: []BucketOption{WithStrategy(StrategyMapCollection)},
			write: func(t *testing.T, b *Bucket, key []byte, i int) {
				require.NoError(t, b.MapSet(key, MapPair{
					Key: []byte("k"), Value: []byte(fmt.Sprintf("v-%03d", i)),
				}))
			},
			read: func(t *testing.T, b *Bucket, key []byte, i int) error {
				pairs, err := b.MapList(context.Background(), key)
				if err == nil {
					require.Len(t, pairs, 1)
					require.Equal(t, fmt.Sprintf("v-%03d", i), string(pairs[0].Value))
				}
				return err
			},
		},
	}
}
