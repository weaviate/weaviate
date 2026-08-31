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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// TestBucketReadsOnCorruptSegmentIndexError pins what a segment whose index is
// corrupt does to a read. The descent reports corruption as an error rather
// than lsmkv.NotFound, and SegmentGroup must pass that on: reading it as
// absence would let the scan fall through to an older segment and answer with a
// stale version of the object, or report a key that exists as missing.
func TestBucketReadsOnCorruptSegmentIndexError(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	// every read that resolves through the disk index, and what it takes to
	// reach it. Each returns the error the bucket API gives its caller.
	reads := []struct {
		name      string
		strategy  string
		secondary bool
		write     func(t *testing.T, b *Bucket, key []byte, i int)
		read      func(t *testing.T, b *Bucket, key []byte, i int) error
	}{
		{
			name: "Get", strategy: StrategyReplace,
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
			name: "Exists", strategy: StrategyReplace,
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
			name: "GetBySecondary", strategy: StrategyReplace, secondary: true,
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
			name: "SetList", strategy: StrategySetCollection,
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
			name: "SetRawList", strategy: StrategySetCollection,
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
			name: "MapList", strategy: StrategyMapCollection,
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

	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := []BucketOption{WithStrategy(read.strategy)}
			if read.secondary {
				opts = append(opts, WithSecondaryIndices(1))
			}
			newBucket := func() *Bucket {
				b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					opts...)
				require.NoError(t, err)
				return b
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

// TestBucketGetOnCorruptNewerSegmentDoesNotServeStaleValue pins the journey the
// descent's error exists for. A key rewritten into a second segment resolves
// there; if the newer index is corrupt and the read reports absence, the scan
// continues into the older segment and answers with the value that key used to
// have — a stale read presented as current.
func TestBucketGetOnCorruptNewerSegmentDoesNotServeStaleValue(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()

	newBucket := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace))
		require.NoError(t, err)
		return b
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

// A collection read concatenates every segment's entries for a key, so it walks
// them all with no early exit. One unreadable segment therefore fails the whole
// read rather than returning the entries the intact segments hold: a short set
// cannot be told apart from a complete one by its caller.
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
				b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					WithStrategy(StrategySetCollection))
				require.NoError(t, err)
				return b
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

// A flush counts, for each key it holds, whether a lower segment already had
// it. That count is a metric sidecar, and the flushed segment is renamed into
// place and its commit log deleted before the count runs — so failing the flush
// when a lower segment cannot be read leaves those writes in a file no bucket
// registers, with no log to replay them from.
func TestBucketFlushOverCorruptLowerSegmentKeepsTheWrites(t *testing.T) {
	ctx := context.Background()
	logger, hook := test.NewNullLogger()
	dir := t.TempDir()

	keys := make([][]byte, 16)
	b := countingBucket(t, ctx, dir, logger)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("v1-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	corruptRootChildPointers(t, dir)

	b = countingBucket(t, ctx, dir, logger)
	t.Cleanup(func() {
		// a flush left half-finished never clears b.flushing, and Shutdown polls
		// for that until its context ends — so this bound is what keeps a
		// regression here reporting in seconds rather than stalling the package
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(shutdownCtx))
	})

	// the same keys, so the count has to consult the corrupt segment for each
	// one rather than being answered by the bloom filter
	for i, key := range keys {
		require.NoError(t, b.Put(key, []byte(fmt.Sprintf("v2-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch(),
		"a flush must not fail because a lower segment's index is unreadable")

	for i, key := range keys {
		value, err := b.Get(key)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("v2-%03d", i), string(value))
	}

	// A key the count could not resolve is left out rather than assumed new, so
	// the total may undercount — it must never exceed the keys that exist.
	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, count, len(keys),
		"a key whose lower segment could not be read must not be counted as an addition")

	// and the log is the only thing that tells an operator the number is short
	var approximate int
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "object count omits") {
			approximate++
			require.Equal(t, logrus.ErrorLevel, entry.Level)
		}
	}
	require.NotZero(t, approximate, "an incomplete count must say so")

	// and it must not reach disk at all. A sidecar that parses is loaded rather
	// than recomputed, and compaction folds it into the merged segment, so an
	// approximate one would outlive the segment that caused it.
	segments, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, segments, 2, "expected the corrupt segment and the one flushed over it")
	sort.Strings(segments)
	newest := strings.TrimSuffix(segments[len(segments)-1], ".db")

	for _, suffix := range []string{".cna", ".metadata"} {
		_, statErr := os.Stat(newest + suffix)
		require.ErrorIs(t, statErr, os.ErrNotExist,
			"%s persists a count that no later load recomputes", filepath.Base(newest+suffix))
	}
}

// A tombstone the count cannot resolve has to subtract rather than be skipped:
// the key it deletes may well be held below, and leaving it at zero would keep
// an object in the count after it was deleted.
func TestBucketFlushOverCorruptLowerSegmentDoesNotOvercount(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dir := t.TempDir()

	keys := make([][]byte, 16)
	b := countingBucket(t, ctx, dir, logger)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, b.Put(keys[i], []byte(fmt.Sprintf("v1-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	// the segment holding every key, so the count has to consult it for each
	// tombstone below and cannot answer any of them
	corruptRootChildPointers(t, dir)

	b = countingBucket(t, ctx, dir, logger)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(shutdownCtx))
	})

	for _, key := range keys {
		require.NoError(t, b.Delete(key))
	}
	require.NoError(t, b.FlushAndSwitch())

	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count,
		"every key was deleted, so no object may be left in the count")
}

// countingBucket opens a replace bucket that keeps the net-additions count.
// Without it a flush consults the lower segments for nothing.
func countingBucket(t *testing.T, ctx context.Context, dir string, logger logrus.FieldLogger) *Bucket {
	t.Helper()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithCalcCountNetAdditions(true))
	require.NoError(t, err)
	return b
}

// corruptRootChildPointers points both children of the root node past the end
// of the index, the shape a torn write leaves behind.
func corruptRootChildPointers(t *testing.T, dir string) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(dir, "*.db"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one flushed segment")

	corruptSegmentRootChildPointers(t, matches[0])
}

// corruptSegmentByAge damages one of several flushed segments, leaving the
// others able to answer for every key. Segment names carry the flush timestamp,
// so they sort oldest-first.
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
