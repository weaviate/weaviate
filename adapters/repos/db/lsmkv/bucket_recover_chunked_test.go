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
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// chunkTestThreshold is the chunk threshold openChunkTestBucket configures, and
// the one size every fixture here derives from. Both triggers take bytes, so it
// can sit far below a production memtable and still cut a run.
const chunkTestThreshold = 32 * 1024

// chunkTestPayload is the per-record padding, and chunkTestEntries is how many of
// them it takes to reach chunkTestWALTarget: a WAL a few times the threshold, so a
// replay cuts a run rather than once. Deriving the count keeps that ratio when the
// threshold moves — a fixture sized in absolute terms silently stops meeting the
// premise its test asserts.
const (
	chunkTestPayload   = 1_200
	chunkTestWALTarget = 5 * chunkTestThreshold
	chunkTestEntries   = chunkTestWALTarget / chunkTestPayload

	// chunkTestUnderThreshold is a record count no strategy can carry past the
	// threshold, for the tests asserting a WAL stays whole. The fattest record here
	// is a roaringsetrange one, so the budget is measured in those.
	chunkTestUnderThreshold = 1
)

type chunkedWALCase struct {
	strategy string
	// entries overrides chunkTestEntries for a strategy whose read is too costly
	// to run over that many keys. Its records carry proportionally more instead.
	entries int
	write   func(t testing.TB, b *Bucket, i, payload int)
	// del retracts what write wrote, so a fixture can put the two in one WAL and a
	// cut between them. Reading entry i back after it must render as the empty
	// string on every strategy.
	del func(t testing.TB, b *Bucket, i, payload int)
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
	writeInvertedRow := func(t testing.TB, b *Bucket, i, payload int) {
		require.NoError(t, b.InvertedSet(paddedKey(i, payload), uint64(i), 1, float32(i%10+1)))
	}
	deleteRow := func(t testing.TB, b *Bucket, i, payload int) {
		mapKey := make([]byte, 8)
		binary.BigEndian.PutUint64(mapKey, uint64(i))
		require.NoError(t, b.MapDeleteKey(paddedKey(i, payload), mapKey))
	}
	deleteInvertedRow := func(t testing.TB, b *Bucket, i, payload int) {
		require.NoError(t, b.InvertedDeleteDoc(paddedKey(i, payload), uint64(i)))
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
			del: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.Delete(paddedKey(i, 0)))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				value, err := b.Get(paddedKey(i, 0))
				require.NoError(t, err)
				return string(value)
			}),
		},
		{
			// a chunk replayed twice appends twice, but SetList deduplicates rows of
			// 1000 values or fewer, as here, so only the segment counts catch it
			strategy: StrategySetCollection,
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.SetAdd(paddedKey(i, 0), [][]byte{paddedKey(i, payload)}))
			},
			del: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.SetDeleteSingle(paddedKey(i, 0), paddedKey(i, payload)))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				values, err := b.SetList(paddedKey(i, 0))
				require.NoError(t, err)
				// a missing key reads back as an empty list, which renders as "[]" and
				// would satisfy require.NotEmpty
				if len(values) == 0 {
					return ""
				}
				return fmt.Sprintf("%q", values)
			}),
		},
		{strategy: StrategyMapCollection, write: writeRow, del: deleteRow, read: readRow},
		{strategy: StrategyInverted, write: writeInvertedRow, del: deleteInvertedRow, read: readRow},
		{
			// reports entries changed rather than bytes, so only the WAL trigger can
			// cut this one. Its point read merges every bit-slice layer of the bucket,
			// hence few keys and fat records.
			strategy: StrategyRoaringSetRange,
			entries:  chunkTestWALTarget / (rangeDocIDFloor * 8),
			write: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.RoaringSetRangeAdd(uint64(i), rangeDocIDs(i, payload)...))
			},
			del: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.RoaringSetRangeRemove(uint64(i), rangeDocIDs(i, payload)...))
			},
			read: func(t testing.TB, b *Bucket, from, to, payload int) []string {
				reader := b.ReaderRoaringSetRange()
				defer reader.Close()

				out := make([]string, 0, to-from)
				for i := from; i < to; i++ {
					bm, release, err := reader.Read(context.Background(), uint64(i),
						filters.OperatorEqual)
					require.NoError(t, err)
					// a missing key reads back empty, which renders as "[]" and would
					// satisfy require.NotEmpty
					if docIDs := bm.ToArray(); len(docIDs) > 0 {
						out = append(out, fmt.Sprintf("%v", docIDs))
					} else {
						out = append(out, "")
					}
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
			del: func(t testing.TB, b *Bucket, i, payload int) {
				require.NoError(t, b.RoaringSetRemoveOne(paddedKey(i, payload), uint64(i)))
			},
			read: perKey(func(t testing.TB, b *Bucket, i, payload int) string {
				bm, release, err := b.RoaringSetGet(context.Background(), paddedKey(i, payload))
				if errors.Is(err, lsmkv.NotFound) {
					return ""
				}
				require.NoError(t, err)
				defer release()

				// a missing key reads back empty rather than NotFound, and renders as
				// "[]", which would satisfy require.NotEmpty
				docIDs := bm.ToArray()
				if len(docIDs) == 0 {
					return ""
				}
				return fmt.Sprintf("%v", docIDs)
			}),
		},
	}
}

// rangeDocIDs pads where the other cases pad their key, a roaringsetrange key
// being a number. The floor is the smallest value that still produces a run.
// rangeDocIDFloor is the doc-id count a range record carries when the padding does
// not raise it, and so the size a range fixture's entry count divides.
const rangeDocIDFloor = 2_000

func rangeDocIDs(i, payload int) []uint64 {
	docIDs := make([]uint64, max(rangeDocIDFloor, payload/3))
	for j := range docIDs {
		docIDs[j] = uint64(i*len(docIDs) + j)
	}
	return docIDs
}

// closeChunkTestBucket asserts the shutdown, a path these tests otherwise leave
// unchecked. These buckets always take the WAL-reuse arm, so it covers the segment
// group's shutdown and the commit-log close rather than a memtable flush.
func closeChunkTestBucket(t testing.TB, ctx context.Context, b *Bucket) {
	t.Helper()
	require.NoError(t, b.Shutdown(ctx))
}

// openChunkTestBucket never flushes on its own, so everything written stays in the
// WAL for the next open to recover. A chunkThreshold of 0 leaves the bucket on the
// production default, which the unit fixtures stay below but BenchmarkWALReplay's
// does not.
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
	return tryOpenChunkTestBucketLogging(dir, strategy, chunkThreshold, nullLogger(), extra...)
}

func tryOpenChunkTestBucketLogging(dir, strategy string, chunkThreshold int,
	logger logrus.FieldLogger, extra ...BucketOption,
) (*Bucket, error) {
	opts := []BucketOption{
		WithStrategy(strategy),
		WithMinWalThreshold(1 << 40),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	}
	if chunkThreshold > 0 {
		// either trigger can cut, and only the WAL one is in reach of roaringsetrange.
		// They are set to the same size, so the WAL one usually gets there first: a
		// test of the memtable trigger has to raise WithWalThreshold out of the way.
		opts = append(opts,
			WithDynamicMemtableSizing(chunkThreshold, chunkThreshold, 1, 3600),
			WithWalThreshold(uint64(chunkThreshold)))
	}

	return NewBucketCreator().NewBucket(context.Background(), dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		append(opts, extra...)...)
}

// entriesWithAction gives the log entries a replay wrote under one action, so a
// test can assert on a line rather than on the state it describes.
func entriesWithAction(hook *logrustest.Hook, action string) []*logrus.Entry {
	var matching []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == action {
			matching = append(matching, entry)
		}
	}
	return matching
}

var (
	chunkTestWALMu sync.Mutex
	chunkTestWALs  = map[string]struct {
		name string
		data []byte
	}{}
)

// chunkTestWAL builds one WAL per strategy and shape and hands back its bytes.
// Driving the writes through the bucket API is what these tests spend their time
// on, and every caller wants the same file, so it is built once and copied.
func chunkTestWAL(t testing.TB, tc chunkedWALCase, entries, payload int) (string, []byte) {
	t.Helper()

	key := fmt.Sprintf("%s/%d/%d", tc.strategy, entries, payload)

	chunkTestWALMu.Lock()
	defer chunkTestWALMu.Unlock()

	if built, ok := chunkTestWALs[key]; ok {
		return built.name, built.data
	}

	dir := t.TempDir()
	buildChunkTestWALRange(t, dir, tc, 0, entries, payload)

	name := filesWithExt(t, dir, ".wal")[0]
	data, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)

	chunkTestWALs[key] = struct {
		name string
		data []byte
	}{name, data}

	return name, data
}

// chunkedWALCaseFor picks a case by strategy. A case inserted above one bound by
// index retargets it silently, leaving the test green on a strategy it never named.
func chunkedWALCaseFor(t testing.TB, strategy string) chunkedWALCase {
	t.Helper()

	for _, tc := range chunkedWALCases() {
		if tc.strategy == strategy {
			return tc
		}
	}

	t.Fatalf("no chunked WAL case for strategy %q", strategy)
	return chunkedWALCase{}
}

// newChunkTestDir gives the caller its own directory holding nothing but the WAL.
func newChunkTestDir(t testing.TB, tc chunkedWALCase, entries, payload int) string {
	t.Helper()

	dir := t.TempDir()
	name, data := chunkTestWAL(t, tc, entries, payload)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o666))

	return dir
}

// buildChunkTestWALWithDeletes puts the writes and the deletes that retract them
// in one WAL, so a cut lands between a row and its tombstone.
func buildChunkTestWALWithDeletes(t testing.TB, dir string, tc chunkedWALCase,
	entries, deleted, payload int,
) {
	t.Helper()

	b := openChunkTestBucket(t, dir, tc.strategy, 0)
	for i := range entries {
		tc.write(t, b, i, payload)
	}
	for i := range deleted {
		tc.del(t, b, i, payload)
	}
	require.NoError(t, b.Shutdown(context.Background()))

	require.Len(t, filesWithExt(t, dir, ".wal"), 1,
		"the writes and the deletes have to share one WAL")
	require.Empty(t, filesWithExt(t, dir, ".db"), "nothing may have been flushed yet")
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

			chunkedDir := newChunkTestDir(t, tc, entries, chunkTestPayload)
			unchunkedDir := newChunkTestDir(t, tc, entries, chunkTestPayload)

			info, err := os.Stat(filepath.Join(chunkedDir, filesWithExt(t, chunkedDir, ".wal")[0]))
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(chunkTestThreshold),
				"the WAL has to exceed the chunk threshold for this test to mean anything")

			b := openChunkTestBucket(t, chunkedDir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

			segments := filesWithExt(t, chunkedDir, ".db")
			require.Greater(t, len(segments), 1,
				"a WAL above the threshold must produce more than one segment")

			// a chunk covers at most the threshold in WAL bytes, so a run shorter
			// than this means a chunk grew past the heap bound it exists to keep
			minSegments := int(info.Size()/int64(chunkTestThreshold)) + 1
			require.GreaterOrEqual(t, len(segments), minSegments,
				"a chunk may not hold more WAL than the threshold")
			require.LessOrEqual(t, len(segments), 4*minSegments,
				"cutting far more often than the threshold implies is the same bound gone wrong")
			require.Empty(t, filesWithExt(t, chunkedDir, ".wal"),
				"the source WAL is gone once all of it has been written out")
			require.Empty(t, filesWithExt(t, chunkedDir, DeleteMarkerSuffix),
				"a committed run leaves nothing staged")

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
			const entries = chunkTestUnderThreshold

			dir := newChunkTestDir(t, tc, entries, chunkTestPayload)
			wal := filesWithExt(t, dir, ".wal")[0]

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

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

			dir := newChunkTestDir(t, tc, entries, chunkTestPayload)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

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

			dir := newChunkTestDir(t, tc, entries, chunkTestPayload)

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
			defer closeChunkTestBucket(t, ctx, b)

			require.Equal(t, segments, filesWithExt(t, dir, ".db"))
			require.Len(t, b.disk.segments, len(segments),
				"a leftover chunk the replay rewrites must not also be mounted as a segment")
			require.Equal(t, expected,
				readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
				"the leftover chunks must not be read on top of the replay that rewrites them")
		})
	}
}

// Records large enough that a cut on bytes alone would fall inside one, which a
// split record would not survive.
func TestRecoverFromWAL_ChunkBoundariesStayEntryAligned(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			const entries, payload = 8, 2 * chunkTestThreshold

			dir := newChunkTestDir(t, tc, entries, payload)
			unchunkedDir := newChunkTestDir(t, tc, entries, payload)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

			require.Greater(t, len(filesWithExt(t, dir, ".db")), 1)

			control := openChunkTestBucket(t, unchunkedDir, tc.strategy, 0)
			defer closeChunkTestBucket(t, ctx, control)

			require.Equal(t,
				readAllChunkTestEntries(t, control, tc, entries, payload),
				readAllChunkTestEntries(t, b, tc, entries, payload),
				"a record the cut falls inside must read back as it does unchunked")
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
		name  string
		files map[string]int64
		// listed in the file map but never written, the shape an external tool that
		// removed a file behind the process leaves behind
		absent    []string
		remaining []string
		// derived files expected to survive; a discarded segment must leave none
		remainingDerived []string
		// the order deletions must happen in, where the case checks it: chunk 0 goes
		// last, so an interrupted walk leaves a prefix and not a suffix
		wantDeletionOrder []string
	}{
		{
			name:      "removes the segment the WAL itself was written out to",
			files:     map[string]int64{wal: 3 << 20, segment(0): 1},
			remaining: nil,
		},
		{
			// a zero-length WAL is unlinked with nothing replayed, so a segment of
			// its id came from an earlier run and is committed data
			name:      "keeps the segment of a zero-length WAL",
			files:     map[string]int64{wal: 0, segment(0): 1},
			remaining: []string{segment(0)},
		},
		{
			name: "removes the whole run of chunks",
			files: map[string]int64{
				wal: 3 << 20, segment(0): 1, segment(1): 1, segment(2): 1, segment(3): 1,
			},
			remaining:         nil,
			wantDeletionOrder: []string{segment(3), segment(2), segment(1), segment(0)},
		},
		{
			name: "stops at the first gap",
			files: map[string]int64{
				wal: 4 << 20, segment(0): 1, segment(1): 1, segment(3): 1,
			},
			remaining: []string{segment(3)},
		},
		{
			// an ordinary shutdown of a small bucket leaves a WAL and no segment, and
			// the ids above it belong to whatever else is in the directory
			name:      "walks no further when the WAL wrote no chunk 0",
			files:     map[string]int64{wal: 3 << 20, segment(1): 1},
			remaining: []string{segment(1)},
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
			// the id prefix has to end at the dot, or a longer id beginning with the
			// same digits is taken for a chunk of this WAL
			name: "leaves a segment whose id merely starts with the WAL's",
			files: map[string]int64{
				"segment-1.wal": 3 << 20, "segment-1.db": 1, "segment-12.db": 1,
			},
			remaining: []string{"segment-12.db"},
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
			// the map is a snapshot, so a file already gone is the outcome wanted and
			// must not fail the bucket open
			name:      "tolerates a listed file the disk no longer has",
			files:     map[string]int64{wal: 3 << 20, segment(0): 1},
			absent:    []string{fmt.Sprintf("segment-%d.bloom", walTimestamp)},
			remaining: nil,
		},
		{
			name:      "leaves segments alone when no WAL survived",
			files:     map[string]int64{segment(0): 1, segment(1): 1},
			remaining: []string{segment(0), segment(1)},
		},
		{
			name: "matches a chunk carrying level and strategy in its name",
			files: map[string]int64{
				wal: 3 << 20, segment(0): 1,
				fmt.Sprintf("segment-%d.l0.s3.db", walTimestamp+1): 1,
			},
			remaining: nil,
		},
		{
			name: "takes the derived files of a discarded chunk with it",
			files: map[string]int64{
				wal: 3 << 20, segment(0): 1, segment(1): 1,
				fmt.Sprintf("segment-%d.bloom", walTimestamp):               1,
				fmt.Sprintf("segment-%d.cna", walTimestamp):                 1,
				fmt.Sprintf("segment-%d.bloom", walTimestamp+1):             1,
				fmt.Sprintf("segment-%d.metadata", walTimestamp+1):          1,
				fmt.Sprintf("segment-%d.secondary.0.bloom", walTimestamp+1): 1,
			},
			remaining:        nil,
			remainingDerived: nil,
		},
		{
			name: "leaves a live segment alone when the WAL id carries a leading zero",
			files: map[string]int64{
				"segment-01771258130098421000.wal": 3 << 20,
				"segment-01771258130098421000.db":  1,
				"segment-1771258130098421001.db":   1,
			},
			remaining: []string{"segment-1771258130098421001.db"},
		},
		{
			name: "does not walk past the end of the id space",
			files: map[string]int64{
				"segment-9223372036854775807.wal": 3 << 20,
				"segment-9223372036854775807.db":  1,
				"segment--9223372036854775808.db": 1,
			},
			remaining: []string{"segment--9223372036854775808.db"},
		},
		{
			// writeSegmentInfoIntoFileName is on by default in production and off in
			// this package, so a sidecar there carries a level and a strategy between
			// the id and its extension
			name: "takes the derived files named with a level and strategy",
			files: map[string]int64{
				wal: 3 << 20, segment(0): 1,
				fmt.Sprintf("segment-%d.l0.s3.bloom", walTimestamp):             1,
				fmt.Sprintf("segment-%d.l0.s3.cna", walTimestamp):               1,
				fmt.Sprintf("segment-%d.l0.s3.metadata", walTimestamp):          1,
				fmt.Sprintf("segment-%d.l0.s3.secondary.0.bloom", walTimestamp): 1,
			},
			remaining:        nil,
			remainingDerived: nil,
		},
		{
			// a name merely ending in .wal is not a WAL: reading it as one would
			// discard the live segment at that id and the ids above it
			name: "leaves the live segments a segment-<T>.db.wal name points at",
			files: map[string]int64{
				segment(0) + ".wal": 3 << 20, segment(0): 1, segment(1): 1,
			},
			remaining: []string{segment(0), segment(1)},
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
			for _, name := range tt.absent {
				files[name] = 1
			}

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.InfoLevel)
			require.NoError(t, removeSegmentsOfSurvivingWALs(dir, files, logger))

			if tt.wantDeletionOrder != nil {
				var deleted []string
				for _, entry := range hook.AllEntries() {
					if path, ok := entry.Data["path"].(string); ok {
						deleted = append(deleted, filepath.Base(path))
					}
				}
				require.Equal(t, tt.wantDeletionOrder, deleted)
			}

			require.ElementsMatch(t, tt.remaining, filesWithExt(t, dir, ".db"))
			for _, name := range tt.remaining {
				require.Contains(t, files, name, "a kept segment must stay in the file list")
			}

			var derived []string
			for _, ext := range []string{".bloom", ".cna", ".metadata"} {
				derived = append(derived, filesWithExt(t, dir, ext)...)
			}
			require.ElementsMatch(t, tt.remainingDerived, derived,
				"a discarded segment must not leave derived files nothing reads")
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

				walName, intact := chunkTestWAL(t, tc, entries, chunkTestPayload)
				damaged := dmg.apply(intact)

				replay := func(wal []byte, chunkThreshold int) (values []string, absent, dir string) {
					dir = t.TempDir()
					require.NoError(t, os.WriteFile(filepath.Join(dir, walName), wal, 0o666))

					b := openChunkTestBucket(t, dir, tc.strategy, chunkThreshold)
					defer closeChunkTestBucket(t, ctx, b)

					return readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
						tc.read(t, b, entries+1, entries+2, chunkTestPayload)[0], dir
				}

				// both arms replay the same bytes, so only the chunking differs
				whole, absent, _ := replay(damaged, 0)
				chunked, _, chunkedDir := replay(damaged, chunkTestThreshold)

				require.Equal(t, whole, chunked)

				// guards against a damage function that costs nothing
				pristine, _, _ := replay(intact, 0)
				require.NotEqual(t, pristine, whole, "the damage has to cost some entries")
				require.NotEqual(t, absent, whole[0],
					"every damage here lands past the first entry, so it has to survive")
				require.Greater(t, len(filesWithExt(t, chunkedDir, ".db")), 1,
					"a single segment means the chunked arm never cut and the two arms "+
						"ran the same code")
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

			const tail = chunkTestUnderThreshold

			dir := newChunkTestDir(t, tc, entries, chunkTestPayload)
			later := t.TempDir()
			buildChunkTestWALRange(t, later, tc, entries, entries+tail,
				chunkTestPayload)

			// WAL names are nanosecond timestamps, so the second one sorts last
			newest := filesWithExt(t, later, ".wal")[0]
			require.Greater(t, newest, filesWithExt(t, dir, ".wal")[0])
			copyWAL(t, later, dir)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

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

func TestWALReplayMaxMemtableSize(t *testing.T) {
	const mb = 1024 * 1024

	tests := []struct {
		name      string
		resizer   *memtableSizeAdvisor
		threshold uint64
		expected  uint64
	}{
		{
			// the bucket's own threshold is what it flushes at under load, which a
			// replay has none of, so chunking ignores it even when it is set
			name:      "no resizer cuts at the configured max, not the bucket threshold",
			threshold: 64 * mb,
			expected:  defaultWALReplayMaxMemtableSize,
		},
		{
			name:      "an active resizer gives its configured max",
			resizer:   newMemtableSizeAdvisor(memtableSizeAdvisorCfg{initial: 10 * mb, stepSize: 10 * mb, maxSize: 200 * mb, maxDuration: time.Minute}),
			threshold: 10 * mb,
			expected:  200 * mb,
		},
		{
			name:      "an inactive resizer falls back to the constant",
			resizer:   newMemtableSizeAdvisor(memtableSizeAdvisorCfg{maxSize: 200 * mb}),
			threshold: 10 * mb,
			expected:  defaultWALReplayMaxMemtableSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				logger: nullLogger(), memtableResizer: tt.resizer,
				memtableThreshold: tt.threshold,
			}
			require.Equal(t, tt.expected, b.walReplayMaxMemtableSize())
		})
	}
}

// writeSegmentTo appends the extension itself, so a path already carrying one
// would write segment-<id>.db.db.
func TestWriteSegmentToRejectsAPathCarryingAnExtension(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-1771258130098421000.db")

	cl, err := newLazyCommitLogger(path, StrategyReplace)
	require.NoError(t, err)
	mt, err := newMemtable(cl, nil, nullLogger(), nil,
		memtableConfig{path: path, strategy: StrategyReplace})
	require.NoError(t, err)
	require.NoError(t, mt.put([]byte("key"), []byte("value")))

	_, err = mt.writeSegmentTo(path, "")
	require.ErrorContains(t, err, `already carries the extension ".db"`)
	require.Empty(t, filesWithExt(t, dir, ".db"),
		"a rejected path must leave no segment behind")
}

// blockChunkWrite makes chunk n's segment impossible to create by putting a
// directory where its file goes, which fails for any uid and for every strategy.
// blockChunkWrite returns the path it blocked, so a caller can assert the failure
// names that chunk rather than re-deriving the name.
func blockChunkWrite(t testing.TB, dir, walName string, chunk int) string {
	t.Helper()

	baseID, err := parseSegmentTimestamp(walName)
	require.NoError(t, err)
	path := chunkSegmentPath(dir, baseID, chunk)
	require.NoError(t, os.Mkdir(path+".db.tmp", 0o755))
	return path
}

// A chunk that cannot be written leaves the WAL where it is, still the only copy
// of what the replay has not made durable.
// Whichever write fails, the WAL stays the only copy and nothing it produced is
// left behind: a committed name an older binary would mount, or staged bytes
// sitting on a full disk for the whole crashloop.
func TestRecoverFromWAL_WriteFailureKeepsTheWAL(t *testing.T) {
	tc := chunkedWALCaseFor(t, StrategySetCollection)

	// the fixture is deterministic, so a twin directory says which index the tail takes
	twin := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
	b := openChunkTestBucket(t, twin, tc.strategy, chunkTestThreshold)
	tail := len(filesWithExt(t, twin, ".db")) - 1
	closeChunkTestBucket(t, context.Background(), b)
	require.Greater(t, tail, 2, "the cases below have to name three different writes")

	tests := []struct {
		name  string
		chunk int
		// empty means the failure names the blocked chunk's own path
		wantErr string
	}{
		// the first chunk fails, so the replay could go on to write its tail and
		// unlink the WAL over what it lost
		{name: "first chunk", chunk: 0},
		// fails once a run is already staged, which is the state an older binary
		// would mount as committed data
		{name: "chunk with a run already staged", chunk: 2},
		{name: "tail", chunk: -1, wantErr: "write the tail of write-ahead-log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := tt.chunk
			if chunk < 0 {
				chunk = tail
			}

			dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
			wal := filesWithExt(t, dir, ".wal")[0]
			blocked := blockChunkWrite(t, dir, wal, chunk)

			wantErr := tt.wantErr
			if wantErr == "" {
				wantErr = fmt.Sprintf("write chunk %q of write-ahead-log", blocked)
			}

			_, err := tryOpenChunkTestBucket(dir, tc.strategy, chunkTestThreshold)
			require.ErrorContains(t, err, wantErr)
			require.ErrorIs(t, err, syscall.EISDIR,
				"the wrap still reads the same when the cause is dropped on the way up")

			require.Equal(t, []string{wal}, filesWithExt(t, dir, ".wal"),
				"the WAL is the only copy until the run is renamed into place")
			require.Empty(t, filesWithExt(t, dir, ".db"),
				"no chunk may carry a name an older binary mounts as a committed segment")
			require.Empty(t, filesWithExt(t, dir, DeleteMarkerSuffix),
				"the abandoned chunks go now, not on the next start: this return aborts "+
					"init, so their bytes would sit on the full disk for the whole crashloop")
		})
	}
}

// A roaring-set node costs several times the WAL record behind it, so chunking can
// be due on a WAL far below any threshold measured in file bytes.
func TestRecoverFromWAL_MemtableTriggerFiresWithoutTheWALOne(t *testing.T) {
	ctx := context.Background()

	noWALTrigger := WithWalThreshold(1 << 40)

	tests := []struct {
		strategy      string
		entries       int
		walStaysUnder bool
	}{
		// counts measured to carry Memtable.Size() past chunkTestThreshold, whose
		// per-entry cost is a node rather than a record length and so does not divide
		// out of the fixture. A threshold change fails the segment-count assertion
		// below rather than passing quietly.
		{strategy: StrategyRoaringSet, entries: 4_000 * chunkTestThreshold / (1024 * 1024), walStaysUnder: true},
		{strategy: StrategySetCollection, entries: 60_000 * chunkTestThreshold / (1024 * 1024)},
		{strategy: StrategyMapCollection, entries: 40_000 * chunkTestThreshold / (1024 * 1024)},
		{strategy: StrategyInverted, entries: 40_000 * chunkTestThreshold / (1024 * 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			tc := chunkedWALCaseFor(t, tt.strategy)

			dir := t.TempDir()
			b := openChunkTestBucket(t, dir, tt.strategy, 0, noWALTrigger)
			for i := range tt.entries {
				tc.write(t, b, i, 0)
			}
			require.NoError(t, b.Shutdown(ctx))

			if tt.walStaysUnder {
				info, err := os.Stat(filepath.Join(dir, filesWithExt(t, dir, ".wal")[0]))
				require.NoError(t, err)
				require.Less(t, info.Size(), int64(chunkTestThreshold),
					"this one cuts on a WAL a threshold of that size would never reach")
			}

			unchunkedDir := t.TempDir()
			copyWAL(t, dir, unchunkedDir)

			b = openChunkTestBucket(t, dir, tt.strategy, chunkTestThreshold, noWALTrigger)
			defer closeChunkTestBucket(t, ctx, b)

			require.Greater(t, len(filesWithExt(t, dir, ".db")), 1,
				"Memtable.Size() has to grow during a replay for cutChunkIfFull to read it")

			control := openChunkTestBucket(t, unchunkedDir, tt.strategy, 0, noWALTrigger)
			defer closeChunkTestBucket(t, ctx, control)

			require.Equal(t,
				tc.read(t, control, 0, tt.entries, 0),
				tc.read(t, b, 0, tt.entries, 0),
				"cutting on held bytes must not change what the replay produces")
		})
	}
}

func TestRecoverFromWAL_SecondaryKeysDoNotDelayTheCut(t *testing.T) {
	const (
		// the cache holds roughly key+value+secondary per entry, so this many cross
		// chunkThreshold several times over
		entries      = 6 * chunkTestThreshold / 96
		secondaryLen = 64
	)
	const chunkThreshold = chunkTestThreshold

	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%011d", i)) }
	secondary := func(i int) []byte {
		return append([]byte(fmt.Sprintf("sec-%011d", i)), make([]byte, secondaryLen-15)...)
	}

	dir := t.TempDir()

	b := openChunkTestBucket(t, dir, StrategyReplace, 0, WithSecondaryIndices(1))
	for i := 0; i < entries; i++ {
		require.NoError(t, b.Put(key(i), key(i), WithSecondaryKey(0, secondary(i))))
	}
	require.NoError(t, b.Shutdown(context.Background()))
	require.Len(t, filesWithExt(t, dir, ".wal"), 1)

	// the WAL trigger would cut on bytes read and hide which accounting the
	// memtable trigger used, so only the memtable trigger is left armed
	b = openChunkTestBucket(t, dir, StrategyReplace, chunkThreshold,
		WithSecondaryIndices(1), WithWalThreshold(1<<40))
	defer closeChunkTestBucket(t, context.Background(), b)

	segments := filesWithExt(t, dir, ".db")
	require.GreaterOrEqual(t, len(segments), 3,
		"every cache drain past the threshold must cut, or the replay holds the whole WAL")
	require.LessOrEqual(t, len(segments), 6,
		"cutting more often than the cache crossed the threshold means the bound moved")

	for i := 0; i < entries; i++ {
		value, err := b.Get(key(i))
		require.NoError(t, err)
		require.Equal(t, key(i), value)
	}
}

// A replay that fails staging its tail must leave no chunk under a committed name,
// which a binary without the cleanup walk would mount.
// A failure renaming a chunk into place abandons the chunks the commit had not
// reached yet, and leaves the ones it had. The operator learns how many from the
// log or not at all.
func TestRecoverFromWAL_CommitFailureDiscardsTheRun(t *testing.T) {
	tc := chunkedWALCaseFor(t, StrategySetCollection)

	dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
	wal := filesWithExt(t, dir, ".wal")[0]
	baseID, err := parseSegmentTimestamp(wal)
	require.NoError(t, err)

	// a directory at chunk 1's committed name fails its rename once chunk 0's has
	// succeeded. GetFileWithSizes skips directories, so it reaches neither the mount
	// loop nor the cleanup walk.
	require.NoError(t, os.Mkdir(chunkSegmentPath(dir, baseID, 1)+".db", 0o755))

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)
	_, err = tryOpenChunkTestBucketLogging(dir, tc.strategy, chunkTestThreshold, logger)
	require.ErrorContains(t, err, "commit the replay of write-ahead-log")

	require.Equal(t, []string{wal}, filesWithExt(t, dir, ".wal"),
		"the WAL is unlinked only once every chunk is committed")
	require.Empty(t, filesWithExt(t, dir, DeleteMarkerSuffix),
		"the chunks the failed commit never reached go now, not on the next start")

	abandoned := entriesWithAction(hook, "lsm_recover_from_active_wal_abandoned")
	require.Len(t, abandoned, 1)
	require.Equal(t, 1, abandoned[0].Data["committed_chunks"],
		"chunk 0 was renamed before chunk 1 failed, and the count is what says so")
	require.NotContains(t, abandoned[0].Message, "aside",
		"moving the WAL aside is what makes the committed chunks permanent")
}

// Both flags change what sg.add does with a segment, and a replay is the one
// caller that adds segments outside a flush or a compaction.
func TestRecoverFromWAL_ChunkedUnderSegmentLoadingFlags(t *testing.T) {
	ctx := context.Background()

	opts := []struct {
		name string
		opt  BucketOption
	}{
		{name: "lazy", opt: WithLazySegmentLoading(true)},
		{name: "inmemory", opt: WithKeepSegmentsInMemory(true)},
	}

	for _, strategy := range []string{StrategySetCollection, StrategyRoaringSetRange} {
		tc := chunkedWALCaseFor(t, strategy)
		entries := tc.entryCount()

		for _, tt := range opts {
			t.Run(strategy+"/"+tt.name, func(t *testing.T) {
				dir := newChunkTestDir(t, tc, entries, chunkTestPayload)
				controlDir := newChunkTestDir(t, tc, entries, chunkTestPayload)

				b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold, tt.opt)
				defer closeChunkTestBucket(t, ctx, b)
				require.Greater(t, len(filesWithExt(t, dir, ".db")), 1,
					"the log has to have been cut for the mount path to be exercised")

				control := openChunkTestBucket(t, controlDir, tc.strategy, 0, tt.opt)
				defer closeChunkTestBucket(t, ctx, control)

				require.Equal(t,
					readAllChunkTestEntries(t, control, tc, entries, chunkTestPayload),
					readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
					"a run mounted under this flag reads back as an unchunked replay does")
			})
		}
	}
}

// A crash between staging and commit leaves chunks under the delete marker. The
// cleanup walk skips them, testing for .db, so the mount loop's marker sweep is
// the only thing that clears them before a replay runs beside them.
func TestRecoverFromWAL_StagedChunkOfACrashedRunIsSweptAway(t *testing.T) {
	ctx := context.Background()
	tc := chunkedWALCaseFor(t, StrategySetCollection)

	dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
	wal := filesWithExt(t, dir, ".wal")[0]
	baseID, err := parseSegmentTimestamp(wal)
	require.NoError(t, err)

	// an earlier run that was killed between stage and commit, at ids this WAL
	// never reaches
	orphan := chunkSegmentPath(dir, baseID-1000, 0) + ".db" + DeleteMarkerSuffix
	require.NoError(t, os.WriteFile(orphan, []byte("not a segment"), 0o666))

	control := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)

	b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
	defer closeChunkTestBucket(t, ctx, b)

	require.Empty(t, filesWithExt(t, dir, DeleteMarkerSuffix),
		"a staged chunk is removed rather than opened as a segment")
	require.NoFileExists(t, orphan)

	unchunked := openChunkTestBucket(t, control, tc.strategy, 0)
	defer closeChunkTestBucket(t, ctx, unchunked)

	require.Equal(t,
		readAllChunkTestEntries(t, unchunked, tc, chunkTestEntries, chunkTestPayload),
		readAllChunkTestEntries(t, b, tc, chunkTestEntries, chunkTestPayload),
		"an orphaned marker beside the log changes nothing about what is recovered")
}

// A recovery of several write-ahead-logs commits them one at a time, so one that
// fails must cost neither the logs already recovered nor the ones not yet reached.
func TestRecoverFromWAL_PartialFailureAcrossWALs(t *testing.T) {
	tc := chunkedWALCaseFor(t, StrategySetCollection)

	const half = chunkTestEntries / 2

	dir := t.TempDir()
	buildChunkTestWALRange(t, dir, tc, 0, half, chunkTestPayload)

	later := t.TempDir()
	buildChunkTestWALRange(t, later, tc, half, chunkTestEntries, chunkTestPayload)
	copyWAL(t, later, dir)

	wals := filesWithExt(t, dir, ".wal")
	require.Len(t, wals, 2)

	// the second log cannot write its first chunk, so the first is already
	// replayed and unlinked by the time the recovery gives up
	blocked := blockChunkWrite(t, dir, wals[1], 0)

	_, err := tryOpenChunkTestBucket(dir, tc.strategy, chunkTestThreshold)
	require.ErrorContains(t, err, blocked)

	require.Equal(t, []string{wals[1]}, filesWithExt(t, dir, ".wal"),
		"the recovered log is gone and the failed one waits for the next start")
	require.NotEmpty(t, filesWithExt(t, dir, ".db"),
		"a log already committed is not rolled back by a later one failing")
	require.Empty(t, filesWithExt(t, dir, DeleteMarkerSuffix),
		"the failed log's staged chunks go now, not on the next start")
}

// A tombstone that lands in a later chunk than the row it retracts is the state
// only a chunked replay produces: an unchunked one holds both in one memtable.
func TestRecoverFromWAL_DeletesSurviveChunking(t *testing.T) {
	ctx := context.Background()

	for _, tc := range chunkedWALCases() {
		t.Run(tc.strategy, func(t *testing.T) {
			entries := tc.entryCount()
			deleted := entries / 10
			require.Greater(t, deleted, 0, "the fixture has to delete something")

			chunkedDir := t.TempDir()
			buildChunkTestWALWithDeletes(t, chunkedDir, tc, entries, deleted, chunkTestPayload)
			unchunkedDir := t.TempDir()
			buildChunkTestWALWithDeletes(t, unchunkedDir, tc, entries, deleted, chunkTestPayload)

			b := openChunkTestBucket(t, chunkedDir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)
			require.Greater(t, len(filesWithExt(t, chunkedDir, ".db")), 1,
				"a single segment means the chunked arm never cut and both arms ran the same code")

			control := openChunkTestBucket(t, unchunkedDir, tc.strategy, 0)
			defer closeChunkTestBucket(t, ctx, control)

			require.Equal(t,
				readAllChunkTestEntries(t, control, tc, entries, chunkTestPayload),
				readAllChunkTestEntries(t, b, tc, entries, chunkTestPayload),
				"a delete must suppress the same rows whether or not the replay cut")

			// guards against a delete phase that retracts nothing, which would make
			// the comparison above hold for the wrong reason
			for i, got := range tc.read(t, b, 0, deleted, chunkTestPayload) {
				require.Empty(t, got, "entry %d was deleted in the same WAL", i)
			}
			require.NotEmpty(t, tc.read(t, b, deleted, deleted+1, chunkTestPayload)[0],
				"the first entry past the delete range has to survive")
		})
	}
}

// A node restarts with one write-ahead-log per bucket, so only a replay that
// chunked is worth a line above Debug.
func TestRecoverFromWAL_ReportsOnlyAReplayThatChunked(t *testing.T) {
	ctx := context.Background()
	tc := chunkedWALCaseFor(t, StrategySetCollection)

	for _, tt := range []struct {
		name    string
		entries int
		want    int
	}{
		{name: "a small WAL is replayed quietly", entries: chunkTestUnderThreshold, want: 0},
		{name: "a WAL that chunked says how many segments", entries: chunkTestEntries, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := newChunkTestDir(t, tc, tt.entries, chunkTestPayload)

			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.InfoLevel)
			b, err := tryOpenChunkTestBucketLogging(dir, tc.strategy, chunkTestThreshold, logger)
			require.NoError(t, err)
			defer closeChunkTestBucket(t, ctx, b)

			require.Len(t, entriesWithAction(hook, "lsm_recover_from_active_wal_success"), tt.want)
		})
	}
}

// The refusal count is what the log line reports, so it has to survive past the
// first refusal.
func TestDrainReplaceCacheCountsEveryRefusal(t *testing.T) {
	dir := t.TempDir()
	b := openChunkTestBucket(t, dir, StrategyReplace, 0, WithSecondaryIndices(1))
	defer closeChunkTestBucket(t, context.Background(), b)

	mt, ok := b.active.(*Memtable)
	require.True(t, ok, "the parser drains into a concrete memtable")

	p := newCommitLoggerParser(StrategyReplace, nil, mt)
	cache := newReplaceCache()
	for i := range 3 {
		// no secondary key reaches createSecondaryKeys, so the bucket's one index
		// slot stays nil and the memtable refuses the entry
		key := []byte(fmt.Sprintf("refused-%d", i))
		cache.nodes[string(key)] = segmentReplaceNode{
			primaryKey:          key,
			value:               []byte("v"),
			secondaryIndexCount: 1,
		}
	}

	p.storeReplaceCache(cache)

	require.Equal(t, 3, p.refusedEntries,
		"the log line reports this, and a chunked replay refuses once per chunk")
	require.Error(t, p.memtableRejectErr, "the first refusal still latches, so the WAL is written out")
}

// An entry the memtable refuses costs that entry, not the file holding it.
func TestRecoverFromWAL_RefusedEntryCostsThatEntryOnly(t *testing.T) {
	dir := t.TempDir()

	b := openChunkTestBucket(t, dir, StrategyReplace, 0, WithSecondaryIndices(1))
	require.NoError(t, b.Put([]byte("refused"), []byte("v"), WithSecondaryKey(0, []byte{})))
	require.NoError(t, b.Put([]byte("kept"), []byte("v"), WithSecondaryKey(0, []byte("secondary"))))
	require.NoError(t, b.Shutdown(context.Background()))

	b = openChunkTestBucket(t, dir, StrategyReplace, 0, WithSecondaryIndices(1))
	defer closeChunkTestBucket(t, context.Background(), b)

	require.Empty(t, filesWithExt(t, dir, ".wal"),
		"a refused entry disposes of the WAL the same way any other replay does")
	require.Len(t, filesWithExt(t, dir, ".db"), 1,
		"the entries the memtable did accept are written out")

	value, err := b.Get([]byte("kept"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value, "the entries around the refused one are recovered")
}

// A key written again after a chunk boundary must read back as the later value,
// and a delete after one must win. This is what the segment ordering is for.
func TestRecoverFromWAL_LaterChunkWinsOverEarlier(t *testing.T) {
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%06d", i)) }
	padding := make([]byte, chunkTestPayload)

	dir := t.TempDir()

	b := openChunkTestBucket(t, dir, StrategyReplace, 0)
	for i := 0; i < chunkTestEntries; i++ {
		require.NoError(t, b.Put(key(i), append([]byte("first-"), padding...)))
	}
	// rewritten after the first pass filled several chunks, so every one of these
	// is written to a later chunk than the value it replaces
	for i := 0; i < chunkTestEntries; i += 3 {
		require.NoError(t, b.Put(key(i), append([]byte("second-"), padding...)))
	}
	for i := 1; i < chunkTestEntries; i += 3 {
		require.NoError(t, b.Delete(key(i)))
	}
	require.NoError(t, b.Shutdown(context.Background()))

	b = openChunkTestBucket(t, dir, StrategyReplace, chunkTestThreshold)
	defer closeChunkTestBucket(t, context.Background(), b)

	require.Greater(t, len(filesWithExt(t, dir, ".db")), 1, "the WAL was replayed as a run")

	for i := 0; i < chunkTestEntries; i++ {
		value, err := b.Get(key(i))
		require.NoError(t, err)

		switch i % 3 {
		case 0:
			require.Equal(t, "second-", string(value[:7]), "the later write must win")
		case 1:
			require.Nil(t, value, "the later delete must win")
		default:
			require.Equal(t, "first-", string(value[:6]), "an untouched key keeps its value")
		}
	}
}

// A replay interrupted between two renames must leave a committed prefix, never
// a suffix: the cleanup walk breaks at the first missing id, so chunks above a
// gap would be mounted beside a WAL that is about to be replayed again.
func TestRecoveredRunCommit(t *testing.T) {
	const walTimestamp = 1771258130098421000

	segment := func(dir string, chunk int) string {
		return filepath.Base(chunkSegmentPath(dir, walTimestamp, chunk)) + ".db"
	}

	tests := []struct {
		name   string
		chunks int
		// perturbs the staged run before the commit is attempted
		setup      func(t *testing.T, dir string, run *recoveredRun)
		wantErr    string
		wantDB     []int
		wantStaged []int
		after      func(t *testing.T, dir string)
	}{
		{
			// commit renames only; mounting has to wait until the WAL is gone
			name: "renames every chunk and mounts none", chunks: 3,
			wantDB: []int{0, 1, 2},
		},
		{
			name: "stops at the first chunk it cannot rename", chunks: 4,
			setup: func(t *testing.T, dir string, run *recoveredRun) {
				require.NoError(t, os.Remove(run.stagedPaths[2]))
			},
			wantErr: "commit recovered segment",
			// nothing above the gap: the committed ids stay a run from the WAL's own
			wantDB: []int{0, 1}, wantStaged: []int{3},
		},
		{
			name: "refuses an id a live segment already holds", chunks: 2,
			setup: func(t *testing.T, dir string, run *recoveredRun) {
				require.NoError(t, os.WriteFile(chunkSegmentPath(dir, walTimestamp, 1)+".db",
					[]byte("live"), 0o666))
			},
			wantErr: "a segment already exists at that id",
			// chunk 1's own staged file is never renamed, so it stays where it is
			wantDB: []int{0, 1}, wantStaged: []int{1},
			after: func(t *testing.T, dir string) {
				surviving, err := os.ReadFile(chunkSegmentPath(dir, walTimestamp, 1) + ".db")
				require.NoError(t, err)
				require.Equal(t, "live", string(surviving),
					"the segment already at that id must not be renamed over")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			run := &recoveredRun{dir: dir, baseSegmentID: walTimestamp}

			for chunk := range tt.chunks {
				staged := chunkSegmentPath(dir, walTimestamp, chunk) + ".db" + DeleteMarkerSuffix
				require.NoError(t, os.WriteFile(staged, []byte("x"), 0o666))
				run.stagedPaths = append(run.stagedPaths, staged)
			}
			if tt.setup != nil {
				tt.setup(t, dir, run)
			}

			var err error
			require.NotPanics(t, func() { err = run.commit() })
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}

			// nil rather than an empty slice: filesWithExt returns nil for no match
			var wantDB []string
			for _, chunk := range tt.wantDB {
				wantDB = append(wantDB, segment(dir, chunk))
			}
			require.Equal(t, wantDB, filesWithExt(t, dir, ".db"))

			var wantStaged []string
			for _, chunk := range tt.wantStaged {
				wantStaged = append(wantStaged, segment(dir, chunk)+DeleteMarkerSuffix)
			}
			require.Equal(t, wantStaged, filesWithExt(t, dir, DeleteMarkerSuffix))

			if tt.after != nil {
				tt.after(t, dir)
			}
		})
	}
}

// The WAL is unlinked between commit and mount, so commit must not reach the
// segment group. The nil sg is the seam: reaching it panics.
// Every chunk after the first must be stamped with a corpus that includes the
// chunks before it, or its block bounds describe a corpus no reader scores against.
func TestRecoveredRunStagesARunningAverage(t *testing.T) {
	dir := t.TempDir()
	// an empty group answers (0, 0), so a stage that consults it per chunk rather
	// than carrying the run's own pair fails here by assertion and not by panic
	run := &recoveredRun{
		sg: &SegmentGroup{}, dir: dir,
		baseSegmentID: 1771258130098421000, chunkingAllowed: true,
	}

	// a corpus already on disk, which the first chunk must be stamped with
	run.avgPropLength, run.propLengthCount = 10, 100

	stamps := make([]float64, 0, 2)
	counts := make([]uint64, 0, 2)
	for chunk, propLen := range []float32{2, 2} {
		mt := newInvertedStageTestMemtable(t, filepath.Join(dir, fmt.Sprintf("mt-%d", chunk)),
			chunk*10, propLen)

		require.NoError(t, run.stage(mt))
		stamps = append(stamps, run.avgPropLength)
		counts = append(counts, run.propLengthCount)
	}

	require.Equal(t, []uint64{110, 120}, counts,
		"each chunk's rows must join the corpus the next chunk is stamped with")
	require.Less(t, stamps[1], stamps[0],
		"a second chunk of short rows must pull the average further down, not repeat the first")

	// what the same two flushes produce in sequence outside a replay
	require.InDelta(t, (10*100+2*10)/110.0, stamps[0], 1e-9)
	require.InDelta(t, (10*100+2*10+2*10)/120.0, stamps[1], 1e-9)
}

// newInvertedStageTestMemtable fills a memtable with ten inverted rows of one
// property length, each under its own doc id so none is deduplicated away.
func newInvertedStageTestMemtable(t *testing.T, path string, firstDocID int, propLen float32) *Memtable {
	t.Helper()

	cl, err := newLazyCommitLogger(path, StrategyInverted)
	require.NoError(t, err)

	mt, err := newMemtable(cl, nil, nullLogger(), nil,
		memtableConfig{path: path, strategy: StrategyInverted})
	require.NoError(t, err)

	for i := range 10 {
		require.NoError(t, mt.appendInverted([]byte(fmt.Sprintf("term-%d", i)), invertedPair{
			docID:       uint64(firstDocID + i),
			tfBits:      math.Float32bits(1),
			propLenBits: math.Float32bits(propLen),
		}))
	}

	return mt
}

// A file that merely ends in .wal names no segment: segmentID cuts it at the first
// dot. The cleanup walk declines such a name, so a replay must not derive chunk ids
// from it either — nothing would ever clean them up.
// A name the cleanup walk would decline must not be chunked: chunks at ids
// derived from it would sit at names nothing ever visits to remove.
func TestRecoverFromWAL_NameThatCannotNameItsChunksIsReplayedWhole(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		// rename puts the fixture's WAL under a name that cannot derive chunk ids,
		// and returns the name it now carries
		rename func(t *testing.T, dir, wal string) string
	}{
		{
			name: "a name merely ending in .wal", strategy: StrategySetCollection,
			rename: func(t *testing.T, dir, wal string) string {
				renamed := strings.TrimSuffix(wal, ".wal") + ".db.wal"
				require.False(t, isSegmentWALName(renamed),
					"the walk has to decline this name for the case to mean anything")
				require.NoError(t, os.Rename(filepath.Join(dir, wal),
					filepath.Join(dir, renamed)))
				return renamed
			},
		},
		{
			// a leading zero, which ParseInt normalizes away and FormatInt does not
			// restore, so the ids a chunk would take are not the ids the walk visits
			name: "an id that does not round-trip", strategy: StrategyReplace,
			rename: func(t *testing.T, dir, wal string) string {
				const renamed = "segment-01771258130098421000.wal"
				require.NoError(t, os.Rename(filepath.Join(dir, wal),
					filepath.Join(dir, renamed)))
				return renamed
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tc := chunkedWALCaseFor(t, tt.strategy)

			dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
			renamed := tt.rename(t, dir, filesWithExt(t, dir, ".wal")[0])

			b, err := tryOpenChunkTestBucket(dir, tc.strategy, chunkTestThreshold)
			require.NoError(t, err)
			// adopted rather than written out, so it never reaches a flush: mt.path
			// may already carry .db, which writeSegmentTo would refuse
			defer b.Shutdown(ctx)

			require.Empty(t, filesWithExt(t, dir, ".db"),
				"a chunk at an id derived from this name would never be cleaned up")
			require.Equal(t, []string{renamed}, filesWithExt(t, dir, ".wal"),
				"an uncut replay keeps its WAL as the active memtable")

			// the same bytes under a name that does derive chunk ids, which is cut
			control := openChunkTestBucket(t,
				newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload),
				tc.strategy, chunkTestThreshold)
			defer control.Shutdown(ctx)

			require.Equal(t,
				tc.read(t, control, 0, chunkTestEntries, chunkTestPayload),
				tc.read(t, b, 0, chunkTestEntries, chunkTestPayload),
				"the uncut replay holds what the cut one holds")
		})
	}
}

// A staged chunk carries the delete marker, which the mount loop removes rather
// than opens.
func TestRecoveredRunStagesUnderTheDeleteMarker(t *testing.T) {
	dir := t.TempDir()
	run := &recoveredRun{
		sg: &SegmentGroup{}, dir: dir,
		baseSegmentID: 1771258130098421000, chunkingAllowed: true,
	}

	mt := newInvertedStageTestMemtable(t, filepath.Join(dir, "mt-0"), 0, 2)
	require.NoError(t, run.stage(mt))

	require.Len(t, run.stagedPaths, 1)
	require.True(t, strings.HasSuffix(run.stagedPaths[0], DeleteMarkerSuffix),
		"a staged chunk must not carry a name a start would mount as committed data")
	require.Empty(t, filesWithExt(t, dir, ".db"))
}

// Nothing reserves a chunk id, so one can already hold a live segment.
// A WAL whose name carries no segment id cannot name chunks, and the run that
// replays it stages nothing under that name. Its entries must still reach a
// segment before the WAL is unlinked.
func TestRecoverFromWAL_WALWithoutIDIsNotLost(t *testing.T) {
	ctx := context.Background()

	later := t.TempDir()

	tc := chunkedWALCaseFor(t, StrategyReplace)

	// above the threshold, so a WAL that could name its chunks would be cut here
	dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)

	named := filesWithExt(t, dir, ".wal")[0]
	require.NoError(t, os.Rename(filepath.Join(dir, named),
		filepath.Join(dir, "no-id.wal")))

	// "no-id" sorts first, so the WAL that carries an id is the one adopted as active
	b := openChunkTestBucket(t, later, StrategyReplace, 0)
	require.NoError(t, b.Put([]byte("later"), []byte("value")))
	require.NoError(t, b.Shutdown(ctx))
	copyWAL(t, later, dir)
	require.Greater(t, filesWithExt(t, dir, ".wal")[1], "no-id.wal")

	b = openChunkTestBucket(t, dir, StrategyReplace, chunkTestThreshold)
	defer closeChunkTestBucket(t, ctx, b)

	require.Equal(t, []string{"no-id.db"}, filesWithExt(t, dir, ".db"),
		"one segment, not a run: a WAL with no id cannot name a chunk after its first")

	// the same WAL under its own name, which is cut
	control := openChunkTestBucket(t,
		newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload),
		tc.strategy, chunkTestThreshold)
	defer control.Shutdown(ctx)

	require.Equal(t,
		tc.read(t, control, 0, chunkTestEntries, chunkTestPayload),
		tc.read(t, b, 0, chunkTestEntries, chunkTestPayload),
		"replayed whole, the WAL with no id holds what the cut one holds")
}

func TestRecoverFromWAL_EmptyWALDoesNotDeleteTheCommittedRun(t *testing.T) {
	ctx := context.Background()
	tc := chunkedWALCaseFor(t, StrategyReplace)

	dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)

	b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
	expected := readAllChunkTestEntries(t, b, tc, chunkTestEntries, chunkTestPayload)
	committed := filesWithExt(t, dir, ".db")
	require.Greater(t, len(committed), 1, "need a committed run for the walk to delete")
	require.NoError(t, b.Shutdown(ctx))

	stem, _, _ := strings.Cut(committed[0], ".")
	require.NoError(t, os.WriteFile(filepath.Join(dir, stem+".wal"), nil, 0o666))

	b = openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
	defer closeChunkTestBucket(t, ctx, b)

	require.Equal(t, committed, filesWithExt(t, dir, ".db"),
		"an empty WAL replays nothing, so the segments at its id are not provisional")
	require.Empty(t, filesWithExt(t, dir, ".wal"))
	require.Equal(t, expected,
		readAllChunkTestEntries(t, b, tc, chunkTestEntries, chunkTestPayload))
}

func TestRecoverFromWAL_AdoptsTheMemtableTheReplayEndedOn(t *testing.T) {
	ctx := context.Background()
	const (
		empties = 20_000
		kept    = 5
	)

	dir := t.TempDir()
	b := openChunkTestBucket(t, dir, StrategyRoaringSetRange, 0)
	// a range record with no values costs WAL bytes and adds nothing to the
	// memtable, so every chunk the WAL trigger cuts here stages no segment
	for i := range empties {
		require.NoError(t, b.RoaringSetRangeAdd(uint64(i)))
	}
	for i := range kept {
		require.NoError(t, b.RoaringSetRangeAdd(uint64(i), uint64(i)))
	}
	require.NoError(t, b.Shutdown(ctx))

	b = openChunkTestBucket(t, dir, StrategyRoaringSetRange, 0, WithWalThreshold(64*1024))
	defer closeChunkTestBucket(t, ctx, b)

	require.Empty(t, filesWithExt(t, dir, ".db"),
		"every chunk was empty, so the replay ends on the adopt arm")

	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	for i := range kept {
		bm, release, err := reader.Read(ctx, uint64(i), filters.OperatorEqual)
		require.NoError(t, err)
		require.Equal(t, []uint64{uint64(i)}, bm.ToArray(),
			"an entry written after a cut must reach the adopted memtable")
		release()
	}
}

func TestCutChunkWithoutChunkingIsANoop(t *testing.T) {
	require.NoError(t, (&commitloggerParser{}).cutChunk())
}

// Two chunked WALs in one recovery: their runs must not collide, and the second
// inherits what the first added to the group.
func TestRecoverFromWAL_TwoChunkedWALsInOneRecovery(t *testing.T) {
	ctx := context.Background()

	for _, strategy := range []string{StrategyReplace, StrategyInverted} {
		t.Run(strategy, func(t *testing.T) {
			tc := chunkedWALCaseFor(t, strategy)

			const half = chunkTestEntries / 2

			dir := t.TempDir()
			buildChunkTestWALRange(t, dir, tc, 0, half, chunkTestPayload)

			later := t.TempDir()
			buildChunkTestWALRange(t, later, tc, half, chunkTestEntries, chunkTestPayload)
			copyWAL(t, later, dir)

			third := t.TempDir()
			buildChunkTestWALRange(t, third, tc, chunkTestEntries, chunkTestEntries+10,
				chunkTestPayload)
			copyWAL(t, third, dir)

			require.Len(t, filesWithExt(t, dir, ".wal"), 3)

			unchunkedDir := t.TempDir()
			copyAllWALs(t, dir, unchunkedDir)

			b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
			defer closeChunkTestBucket(t, ctx, b)

			segments := filesWithExt(t, dir, ".db")
			require.Greater(t, len(segments), 3,
				"two WALs above the threshold produce two runs, not two segments")

			control := openChunkTestBucket(t, unchunkedDir, tc.strategy, 0)
			defer closeChunkTestBucket(t, ctx, control)

			require.Equal(t,
				readAllChunkTestEntries(t, control, tc, chunkTestEntries, chunkTestPayload),
				readAllChunkTestEntries(t, b, tc, chunkTestEntries, chunkTestPayload),
				"three WALs in one recovery must read back as they do unchunked")
		})
	}
}

// A WAL that crosses the threshold and also carries an entry the memtable will refuse
// must still commit its run, and the entries around the refused one must survive this
// open and the next.
func TestRecoverFromWAL_RefusedEntryDuringAChunkedReplay(t *testing.T) {
	ctx := context.Background()

	const (
		// a record is key, value and a padded secondary; this many put the WAL a few
		// times over the threshold, so the refused one lands mid-run
		entries = 3 * chunkTestThreshold / 140
		refused = entries / 2
	)
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }
	secondary := func(i int) []byte {
		if i == refused {
			return []byte{}
		}
		return append([]byte(fmt.Sprintf("sec-%08d", i)), make([]byte, 110)...)
	}

	dir := t.TempDir()
	b := openChunkTestBucket(t, dir, StrategyReplace, 0, WithSecondaryIndices(1))
	for i := range entries {
		require.NoError(t, b.Put(key(i), key(i), WithSecondaryKey(0, secondary(i))))
	}
	require.NoError(t, b.Shutdown(ctx))

	info, err := os.Stat(filepath.Join(dir, filesWithExt(t, dir, ".wal")[0]))
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(chunkTestThreshold),
		"the WAL has to cross the threshold, or no chunk is committed beside the refusal")

	b = openChunkTestBucket(t, dir, StrategyReplace, chunkTestThreshold,
		WithSecondaryIndices(1))

	require.Greater(t, len(filesWithExt(t, dir, ".db")), 1,
		"the chunks committed even though one entry never reached a memtable")
	require.Empty(t, filesWithExt(t, dir, ".wal"),
		"a refusal disposes of the WAL the way any other replay does")
	require.NoError(t, b.Shutdown(ctx))

	// the committed run has to survive the boot that follows, with no WAL left to
	// walk its ids from
	b = openChunkTestBucket(t, dir, StrategyReplace, chunkTestThreshold,
		WithSecondaryIndices(1))
	defer closeChunkTestBucket(t, ctx, b)

	for i := range entries {
		value, err := b.Get(key(i))
		require.NoError(t, err)
		if i == refused {
			continue
		}
		require.Equal(t, key(i), value, "entry %d was not the refused one", i)
	}
}

// Production writes the level and strategy into a segment's name. A cut chunk has
// to take that shape too, and the cleanup walk has to match it.
func TestRecoverFromWAL_ChunkedInSegmentInfoNamingMode(t *testing.T) {
	ctx := context.Background()
	tc := chunkedWALCaseFor(t, StrategyReplace)
	segInfo := WithWriteSegmentInfoIntoFileName(true)

	dir := newChunkTestDir(t, tc, chunkTestEntries, chunkTestPayload)
	walName, walBytes := chunkTestWAL(t, tc, chunkTestEntries, chunkTestPayload)

	b := openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold, segInfo)
	committed := filesWithExt(t, dir, ".db")
	require.Greater(t, len(committed), 1)
	for _, segment := range committed {
		require.Contains(t, segment, segmentExtraInfo(0, SegmentStrategyFromString(tc.strategy)),
			"a chunk carries the same infix a flushed segment would")
	}
	require.NoError(t, b.Shutdown(ctx))

	// the crash window: the run is on disk and the WAL was never unlinked
	require.NoError(t, os.WriteFile(filepath.Join(dir, walName), walBytes, 0o666))

	b = openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold, segInfo)
	defer closeChunkTestBucket(t, ctx, b)

	require.Equal(t, committed, filesWithExt(t, dir, ".db"),
		"the walk matched the infixed names, so the run was rewritten rather than mounted twice")
	require.Len(t, b.disk.segments, len(committed))
}

// A chunk of a replayed WAL bakes its block-max bounds against the average
// property length of the mounted corpus, not the average of its own rows.
func TestRecoverFromWAL_ChunkStampedWithMountedCorpusAverage(t *testing.T) {
	ctx := context.Background()

	const (
		corpusDocs    = 1_000
		corpusPropLen = 50_000
		probeTerm     = "probeterm"
	)

	tc := chunkedWALCaseFor(t, StrategyInverted)

	dir := t.TempDir()

	b := openChunkTestBucket(t, dir, tc.strategy, 0)
	for i := range corpusDocs {
		require.NoError(t, b.InvertedSet([]byte(fmt.Sprintf("corpus-%06d", i)),
			uint64(1_000_000+i), 1, corpusPropLen))
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))
	require.Len(t, filesWithExt(t, dir, ".db"), 1,
		"the corpus has to be a mounted segment for the replay to read an average off it")

	later := t.TempDir()
	source := openChunkTestBucket(t, later, tc.strategy, 0)
	for i := range chunkTestEntries {
		tc.write(t, source, i, chunkTestPayload)
	}
	// with the default k1 and b the impact argmax of this pair flips at an average
	// property length of exactly 591: the short posting wins below it, the frequent
	// one above. The corpus sits far above and the WAL's own rows far below.
	require.NoError(t, source.InvertedSet([]byte(probeTerm), 900_001, 2, 1))
	require.NoError(t, source.InvertedSet([]byte(probeTerm), 900_002, 3, 100))
	require.NoError(t, source.Shutdown(ctx))
	copyWAL(t, later, dir)

	b = openChunkTestBucket(t, dir, tc.strategy, chunkTestThreshold)
	defer closeChunkTestBucket(t, ctx, b)

	require.Greater(t, len(filesWithExt(t, dir, ".db")), 2,
		"the WAL has to be replayed as a run of chunks beside the corpus segment")

	avg, _ := b.GetAveragePropertyLength()
	view := b.GetConsistentView()
	defer view.ReleaseView()

	var blocks []terms.BlockEntry
	for _, segment := range view.Disk {
		sbm := segment.newSegmentBlockMax(nil, []byte(probeTerm), 0, 1, 1, nil, nil, nil,
			avg, schema.BM25Config{K1: float64(config.DefaultBM25k1), B: float64(config.DefaultBM25b)})
		if sbm == nil {
			continue
		}
		blocks = append(blocks, sbm.blockEntries...)
	}

	require.Len(t, blocks, 1, "the probe postings belong to one chunk, and it has to be on disk")
	require.EqualValues(t, 3, blocks[0].MaxImpactTf,
		"a chunk scored against its own rows bakes the short posting as its block maximum")
	require.EqualValues(t, 100, blocks[0].MaxImpactPropLength,
		"and its property length with it, so the pair the reader rebuilds the bound from is wrong")
}
