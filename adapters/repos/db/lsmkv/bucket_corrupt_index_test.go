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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/lsmkv"
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
