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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// chunkTestThreshold is the chunk threshold openChunkTestBucket configures, so
// these tests chunk on a WAL of a few MB instead of the 200 MB default.
const chunkTestThreshold = 1024 * 1024

// chunkTestEntries and chunkTestPayload together put the WAL a few times over
// chunkTestThreshold, so a replay produces a run of chunks rather than two.
const (
	chunkTestEntries = 3_000
	chunkTestPayload = 1_200
)

type chunkedWALCase struct {
	strategy string
	// entries overrides chunkTestEntries for a strategy whose read is too costly
	// to run over that many keys. Its records carry proportionally more instead.
	entries int
	write   func(t testing.TB, b *Bucket, i, payload int)
	// batched because a roaringsetrange reader merges every segment it opens, so
	// one read per key would cost more than the replay under test
	read func(t testing.TB, b *Bucket, from, to, payload int) []string
}

func (tc chunkedWALCase) entryCount() int {
	if tc.entries > 0 {
		return tc.entries
	}
	return chunkTestEntries
}

func perKey(read func(t testing.TB, b *Bucket, i, payload int) string,
) func(t testing.TB, b *Bucket, from, to, payload int) []string {
	return func(t testing.TB, b *Bucket, from, to, payload int) []string {
		out := make([]string, 0, to-from)
		for i := from; i < to; i++ {
			out = append(out, read(t, b, i, payload))
		}
		return out
	}
}

func chunkedWALCases() []chunkedWALCase {
	// the padding is what carries a case over the chunk threshold
	paddedKey := func(i, payload int) []byte {
		return append([]byte(fmt.Sprintf("key-%06d-", i)), make([]byte, payload)...)
	}
	// a map value is capped at 65535 bytes and an inverted one is read back as a
	// (frequency, property length) pair, so both carry their padding in the row key
	writeRow := func(t testing.TB, b *Bucket, i, payload int) {
		require.NoError(t, b.MapSet(paddedKey(i, payload),
			NewMapPairFromDocIdAndTf(uint64(i), 1, float32(i%10+1), false)))
	}
	readRow := perKey(func(t testing.TB, b *Bucket, i, payload int) string {
		pairs, err := b.MapList(context.Background(), paddedKey(i, payload))
		require.NoError(t, err)

		rendered := make([]string, len(pairs))
		for j, pair := range pairs {
			rendered[j] = fmt.Sprintf("%d=%q", binary.BigEndian.Uint64(pair.Key), pair.Value)
		}
		return strings.Join(rendered, ",")
	})

	return []chunkedWALCase{
		{
			strategy: StrategyReplace,
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.Put(paddedKey(i, 0), paddedKey(i, payload)))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				value, err := b.Get(paddedKey(i, 0))
				require.NoError(t, err)
				return string(value)
			}),
		},
		{
			// append semantics and no newest-wins dedup, so a chunk replayed twice
			// shows up as a second value instead of being absorbed
			strategy: StrategySetCollection,
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.SetAdd(paddedKey(i, 0), [][]byte{paddedKey(i, payload)}))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				values, err := b.SetList(paddedKey(i, 0))
				require.NoError(t, err)
				return fmt.Sprintf("%q", values)
			}),
		},
		{strategy: StrategyMapCollection, write: writeRow, read: readRow},
		{strategy: StrategyInverted, write: writeRow, read: readRow},
		{
			// reports entries changed rather than bytes, so only the WAL trigger can
			// cut this one. Its point read merges every bit-slice layer of the bucket,
			// hence few keys and fat records.
			strategy: StrategyRoaringSetRange,
			entries:  150,
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.RoaringSetRangeAdd(uint64(i), rangeDocIDs(i, payload)...))
			},
			read: func(t testing.TB, b *Bucket, from, to, payload int) []string {
				reader := b.ReaderRoaringSetRange()
				defer reader.Close()

				out := make([]string, 0, to-from)
				for i := from; i < to; i++ {
					bm, release, err := reader.Read(context.Background(), uint64(i),
						filters.OperatorEqual)
					require.NoError(t, err)
					out = append(out, fmt.Sprintf("%v", bm.ToArray()))
					release()
				}
				return out
			},
		},
		{
			// a node costs several times the WAL record behind it, so this is where a
			// chunk count derived from WAL bytes goes wrong
			strategy: StrategyRoaringSet,
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.RoaringSetAddOne(paddedKey(i, payload), uint64(i)))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				bm, release, err := b.RoaringSetGet(context.Background(), paddedKey(i, payload))
				if errors.Is(err, lsmkv.NotFound) {
					return ""
				}
				require.NoError(t, err)
				defer release()

				return fmt.Sprintf("%v", bm.ToArray())
			}),
		},
	}
}

// rangeDocIDs pads where the other cases pad their key, a roaringsetrange key
// being a number. The floor makes a few hundred entries cross the threshold, the
// divisor keeps one oversized record just past it.
func rangeDocIDs(i, payload int) []uint64 {
	docIDs := make([]uint64, max(3000, payload/3))
	for j := range docIDs {
		docIDs[j] = uint64(i*len(docIDs) + j)
	}
	return docIDs
}

// openChunkTestBucket never flushes on its own, so everything written stays in the
// WAL for the next open to recover. A chunkThreshold of 0 leaves the bucket on the
// default, which no test WAL reaches.
func openChunkTestBucket(t testing.TB, dir, strategy string, chunkThreshold int,
	extra ...BucketOption,
) *Bucket {
	t.Helper()

	b, err := tryOpenChunkTestBucket(dir, strategy, chunkThreshold, extra...)
	require.NoError(t, err)

	return b
}

func tryOpenChunkTestBucket(dir, strategy string, chunkThreshold int,
	extra ...BucketOption,
) (*Bucket, error) {
	opts := []BucketOption{
		WithStrategy(strategy),
		WithMinWalThreshold(1 << 40),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	}
	if chunkThreshold > 0 {
		// either trigger can cut, and only the WAL one is in reach of roaringsetrange
		mb := chunkThreshold / (1024 * 1024)
		opts = append(opts,
			WithDynamicMemtableSizing(mb, mb, 1, 3600),
			WithWalThreshold(uint64(chunkThreshold)))
	}

	return NewBucketCreator().NewBucket(context.Background(), dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		append(opts, extra...)...)
}

func buildChunkTestWAL(t testing.TB, dir string, tc chunkedWALCase, entries, payload int) {
	t.Helper()

	buildChunkTestWALRange(t, dir, tc, 0, entries, payload)
}

func buildChunkTestWALRange(t testing.TB, dir string, tc chunkedWALCase, from, to, payload int) {
	t.Helper()

	b := openChunkTestBucket(t, dir, tc.strategy, 0)
	for i := from; i < to; i++ {
		tc.write(t, b, i, payload)
	}
	require.NoError(t, b.Shutdown(context.Background()))

	require.Len(t, filesWithExt(t, dir, ".wal"), 1, "everything written must still be in the WAL")
	require.Empty(t, filesWithExt(t, dir, ".db"), "nothing may have been flushed yet")
}

func filesWithExt(t testing.TB, dir, ext string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var matching []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ext {
			matching = append(matching, entry.Name())
		}
	}
	return matching
}

func readAllChunkTestEntries(t testing.TB, b *Bucket, tc chunkedWALCase, entries, payload int) []string {
	t.Helper()

	return tc.read(t, b, 0, entries, payload)
}

func TestRecoverFromWAL_ChunkedAboveThreshold(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			entries := tc.entryCount()

			chunkedDir, unchunkedDir := t.TempDir(), t.TempDir()
			buildChunkTestWAL(t, chunkedDir, tc, entries, chunkTestPayload)
			buildChunkTestWAL(t, unchunkedDir, tc, entries, chunkTestPayload)

			info, err := os.Stat(filepath.Join(chunkedDir, filesWithExt(t, chunkedDir, ".wal")[0]))
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(chunkTestThreshold),
				"the WAL has to exceed the chunk threshold for this test to mean anything")

			b := openChunkTestBucket(t, chunkedDir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Greater(t, len(filesWithExt(t, chunkedDir, ".db")), 1,
				"a WAL above the threshold must produce more than one segment")
			require.Empty(t, filesWithExt(t, chunkedDir, ".wal"),
				"the source WAL is gone once all of it has been written out")

			control := openChunkTestBucket(t, unchunkedDir, tc.strategy, 0)
			defer control.Shutdown(ctx)
			require.Empty(t, filesWithExt(t, unchunkedDir, ".db"),
				"the control recovery keeps the WAL as its active memtable")

			require.Equal(t,
				readAllChunkTestEntries(t, control, tc, entries, chunkTestPayload),
				readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload))
		})
	}
}

func TestRecoverFromWAL_BelowThresholdKeepsWAL(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			const entries = 10

			dir := t.TempDir()
			buildChunkTestWAL(t, dir, tc, entries, chunkTestPayload)
			wal := filesWithExt(t, dir, ".wal")[0]

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Empty(t, filesWithExt(t, dir, ".db"),
				"a WAL below the threshold is not written out as a segment")
			require.Equal(t, []string{wal}, filesWithExt(t, dir, ".wal"))
			require.Equal(t, filepath.Join(dir, wal), b.active.commitlogWalPath(),
				"the recovered memtable keeps writing into the WAL it was built from")

			for _, entry := range tc.read(t, b, 0, entries, chunkTestPayload) {
				require.NotEmpty(t, entry)
			}
		})
	}
}

// A chunked WAL never becomes the active memtable, whose next flush would
// overwrite chunk 0.
func TestRecoverFromWAL_ChunkedLastWALStartsFresh(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			entries := tc.entryCount()

			dir := t.TempDir()
			buildChunkTestWAL(t, dir, tc, entries, chunkTestPayload)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Zero(t, b.active.Size(), "the recovered bucket starts on an empty memtable")
			for _, segment := range filesWithExt(t, dir, ".db") {
				require.NotEqual(t, segmentID(segment), segmentID(b.active.Path()),
					"the active memtable would flush over this segment")
			}

			before := readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload)
			missing := tc.read(t, b, entries+1, entries+2, chunkTestPayload)[0]
			require.NotContains(t, before, missing,
				"a chunked WAL is consumed whole, tail included")

			tc.write(t, b, entries, chunkTestPayload)
			require.NoError(t, b.FlushAndSwitch())

			require.Equal(t, before,
				readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
				"an ordinary flush must not overwrite a chunk segment")
		})
	}
}

// Leftover chunks of an interrupted replay are dropped, not mounted alongside the
// replay that rewrites them.
func TestRecoverFromWAL_ChunkedReplayInterrupted(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			entries := tc.entryCount()

			dir := t.TempDir()
			buildChunkTestWAL(t, dir, tc, entries, chunkTestPayload)

			wal := filesWithExt(t, dir, ".wal")[0]
			walContents, err := os.ReadFile(filepath.Join(dir, wal))
			require.NoError(t, err)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			expected := readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload)
			segments := filesWithExt(t, dir, ".db")
			require.Greater(t, len(segments), 2, "need a run of chunks to walk")
			require.NoError(t, b.Shutdown(ctx))

			// the chunks are on disk, but the crash came before the WAL was unlinked
			require.NoError(t, os.WriteFile(filepath.Join(dir, wal), walContents, 0o644))

			b = openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Equal(t, segments, filesWithExt(t, dir, ".db"))
			require.Len(t, b.disk.segments, len(segments),
				"a leftover chunk the replay rewrites must not also be mounted as a segment")
			require.Equal(t, expected,
				readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
				"the leftover chunks must not be read on top of the replay that rewrites them")
		})
	}
}

// Records large enough that chunk boundaries fall inside one, which a split record
// would not survive.
func TestRecoverFromWAL_ChunkBoundariesStayEntryAligned(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			const entries, payload = 8, 400 * 1024

			dir := t.TempDir()
			buildChunkTestWAL(t, dir, tc, entries, payload)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Greater(t, len(filesWithExt(t, dir, ".db")), 1)
			for i, entry := range tc.read(t, b, 0, entries, payload) {
				require.NotEmpty(t, entry, "entry %d did not survive the replay", i)
			}
		})
	}
}

func TestRemoveSegmentsOfSurvivingWALs(t *testing.T) {
	const walTimestamp = 1771258130098421000

	segment := func(offset int) string {
		return fmt.Sprintf("segment-%d.db", walTimestamp+offset)
	}
	wal := fmt.Sprintf("segment-%d.wal", walTimestamp)

	tests := []struct {
		name      string
		files     map[string]int64
		remaining []string
	}{
		{
			name:      "removes the segment the WAL itself was written out to",
			files:     map[string]int64{wal: 3 << 20, segment(0): 1},
			remaining: nil,
		},
		{
			name: "removes the whole run of chunks",
			files: map[string]int64{
				wal: 3 << 20, segment(0): 1, segment(1): 1, segment(2): 1, segment(3): 1,
			},
			remaining: nil,
		},
		{
			name: "stops at the first gap",
			files: map[string]int64{
				wal: 4 << 20, segment(0): 1, segment(1): 1, segment(3): 1,
			},
			remaining: []string{segment(3)},
		},
		{
			// a memtable can hold several times the bytes of the WAL records behind
			// it, so the run is followed as far as it goes
			name: "follows a run longer than the WAL is bytes",
			files: map[string]int64{
				wal: 1, segment(0): 1, segment(1): 1, segment(2): 1,
			},
			remaining: nil,
		},
		{
			// matched on the id as written, so this pair is still recognised
			name: "removes the segment of a WAL whose name carries no number",
			files: map[string]int64{
				"segment-stray.wal": 3 << 20, "segment-stray.db": 1, segment(0): 1,
			},
			remaining: []string{segment(0)},
		},
		{
			name:      "leaves segments alone when no WAL survived",
			files:     map[string]int64{segment(0): 1, segment(1): 1},
			remaining: []string{segment(0), segment(1)},
		},
		{
			name: "matches a chunk carrying level and strategy in its name",
			files: map[string]int64{
				wal: 3 << 20,
				fmt.Sprintf("segment-%d.l0.s3.db", walTimestamp+1): 1,
			},
			remaining: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			files := make(map[string]int64, len(tt.files))
			for name, size := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644))
				files[name] = size
			}

			require.NoError(t, removeSegmentsOfSurvivingWALs(dir, files, nullLogger()))

			require.ElementsMatch(t, tt.remaining, filesWithExt(t, dir, ".db"))
			for _, name := range tt.remaining {
				require.Contains(t, files, name, "a kept segment must stay in the file list")
			}
		})
	}
}

// A damaged WAL recovers the same data chunked or not. Chunking changes how many
// segments come out, never what survives.
func TestRecoverFromWAL_DamagedWALRecoversTheSameEitherWay(t *testing.T) {
	ctx := context.Background()

	damages := []struct {
		name  string
		apply func(wal []byte) []byte
	}{
		{"truncate-1b", func(wal []byte) []byte { return wal[:len(wal)-1] }},
		{"truncate-mid-record", func(wal []byte) []byte { return wal[:len(wal)*6/10+13] }},
		{"flip-byte-mid", func(wal []byte) []byte {
			damaged := append([]byte(nil), wal...)
			damaged[len(damaged)/2] ^= 0xFF
			return damaged
		}},
	}

	for _, tc := range chunkedWALCases() {
		for _, dmg := range damages {
			t.Run(tc.strategy+"/"+dmg.name, func(t *testing.T) {
				entries := tc.entryCount()

				source := t.TempDir()
				buildChunkTestWAL(t, source, tc, entries, chunkTestPayload)
				walName := filesWithExt(t, source, ".wal")[0]
				intact, err := os.ReadFile(filepath.Join(source, walName))
				require.NoError(t, err)
				damaged := dmg.apply(intact)

				// both arms replay the same bytes, so only the chunking differs
				replay := func(chunkThreshold int) (values []string, absent, dir string) {
					dir = t.TempDir()
					require.NoError(t, os.WriteFile(filepath.Join(dir, walName), damaged, 0o666))

					b := openChunkTestBucket(t, dir, tc.strategy, chunkThreshold)
					defer b.Shutdown(ctx)

					return readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
						tc.read(t, b, entries+1, entries+2, chunkTestPayload)[0], dir
				}

				whole, absent, _ := replay(0)
				chunked, _, chunkedDir := replay(chunkTestThreshold)

				require.Equal(t, whole, chunked)
				require.NotEqual(t, absent, whole[0],
					"every damage here lands past the first entry, so it has to survive")
				require.NotEmpty(t, filesWithExt(t, chunkedDir, ".db"))
				require.Empty(t, filesWithExt(t, chunkedDir, ".wal"),
					"a damaged WAL is consumed like any other")
			})
		}
	}
}

// The older of two WALs is chunked out to segments, the newest becoming active.
func TestRecoverFromWAL_ChunkedWALIsNotTheLast(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			entries := tc.entryCount()

			const tail = 10

			dir, later := t.TempDir(), t.TempDir()
			buildChunkTestWAL(t, dir, tc, entries, chunkTestPayload)
			buildChunkTestWALRange(t, later, tc, entries, entries+tail,
				chunkTestPayload)

			// WAL names are nanosecond timestamps, so the second one sorts last
			newest := filesWithExt(t, later, ".wal")[0]
			require.Greater(t, newest, filesWithExt(t, dir, ".wal")[0])
			copyWAL(t, later, dir)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer b.Shutdown(ctx)

			require.Greater(t, len(filesWithExt(t, dir, ".db")), 1,
				"the older WAL is chunked out to segments")
			require.Equal(t, []string{newest}, filesWithExt(t, dir, ".wal"),
				"the newest WAL still becomes the active memtable")

			missing := tc.read(t, b, entries+tail+1, entries+tail+2, chunkTestPayload)[0]
			all := readAllChunkTestEntries(t, b, tc, entries+tail, chunkTestPayload)
			require.NotContains(t, all, missing, "both WALs have to be recovered")
		})
	}
}

func TestWALReplayMemtableThreshold(t *testing.T) {
	const mb = 1024 * 1024

	tests := []struct {
		name     string
		resizer  *memtableSizeAdvisor
		expected uint64
	}{
		{
			name:     "no resizer falls back to the constant",
			expected: defaultWALReplayMemtableThreshold,
		},
		{
			// the initial size is hardcoded to 10 MB, far below where a busy
			// bucket's memtable settles, so the max is what a chunk is measured on
			name:     "an active resizer gives its configured max",
			resizer:  newMemtableSizeAdvisor(memtableSizeAdvisorCfg{initial: 10 * mb, stepSize: 10 * mb, maxSize: 200 * mb, maxDuration: time.Minute}),
			expected: 200 * mb,
		},
		{
			name:     "an inactive resizer falls back to the constant",
			resizer:  newMemtableSizeAdvisor(memtableSizeAdvisorCfg{maxSize: 200 * mb}),
			expected: defaultWALReplayMemtableThreshold,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{logger: nullLogger(), memtableResizer: tt.resizer}
			require.Equal(t, tt.expected, b.walReplayMemtableThreshold())
		})
	}
}

// A chunk that cannot be written leaves the WAL where it is, still the only copy
// of what the replay has not made durable.
func TestRecoverFromWAL_ChunkWriteFailureKeepsWAL(t *testing.T) {
	tc := chunkedWALCases()[1]
	require.Equal(t, StrategySetCollection, tc.strategy)

	dir := t.TempDir()
	buildChunkTestWAL(t, dir, tc, chunkTestEntries, chunkTestPayload)
	wal := filesWithExt(t, dir, ".wal")[0]

	// totalValueSizeCollection consults this per key. Only the first chunk fails, so
	// the replay could go on to write its tail and unlink the WAL over what it lost.
	var writes atomic.Int64
	failWrite := WithShouldSkipKeyFunction(func(key []byte, ctx context.Context) (bool, error) {
		if writes.Add(1) == 1 {
			return false, errors.New("no space left on device")
		}
		return false, nil
	})

	_, err := tryOpenChunkTestBucket(dir, tc.strategy, chunkTestThreshold, failWrite)
	require.ErrorContains(t, err, "no space left on device")

	require.Equal(t, []string{wal}, filesWithExt(t, dir, ".wal"))
	require.Empty(t, filesWithExt(t, dir, ".db"))
}

// A roaring-set node costs several times the WAL record behind it, so chunking can
// be due on a WAL far below any threshold measured in file bytes.
func TestRecoverFromWAL_MemtableTriggerFiresBelowTheWALSize(t *testing.T) {
	ctx := context.Background()
	const entries = 15_000

	// high enough that only the memtable trigger can cut this replay
	noWALTrigger := WithWalThreshold(1 << 40)
	key := func(i int) []byte { return []byte(fmt.Sprintf("k-%06d", i)) }

	dir := t.TempDir()
	b := openChunkTestBucket(t, dir, StrategyRoaringSet, 0, noWALTrigger)
	for i := range entries {
		require.NoError(t, b.RoaringSetAddOne(key(i), uint64(i)))
	}
	require.NoError(t, b.Shutdown(ctx))

	wal := filesWithExt(t, dir, ".wal")[0]
	info, err := os.Stat(filepath.Join(dir, wal))
	require.NoError(t, err)
	require.Less(t, info.Size(), int64(chunkTestThreshold),
		"the WAL has to stay under the threshold for this test to mean anything")

	b = openChunkTestBucket(t, dir, StrategyRoaringSet, chunkTestThreshold, noWALTrigger)
	defer b.Shutdown(ctx)

	require.Greater(t, len(filesWithExt(t, dir, ".db")), 1,
		"the memtable outgrew the threshold even though the WAL never did")

	for i := range entries {
		bm, release, err := b.RoaringSetGet(ctx, key(i))
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(i)}, bm.ToArray())
		release()
	}
}
