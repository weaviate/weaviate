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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// fiveSortedKeys is the fixture whose median "foobar" becomes the BST root,
// which the van Emde Boas writer emits first and so places at offset 0.
func fiveSortedKeys() []Key {
	return []Key{
		{Key: []byte("aaa"), ValueStart: 0, ValueEnd: 2},
		{Key: []byte("abc"), ValueStart: 2, ValueEnd: 5},
		{Key: []byte("foobar"), ValueStart: 5, ValueEnd: 18},
		{Key: []byte("zzz"), ValueStart: 18, ValueEnd: 35},
		{Key: []byte("zzzz"), ValueStart: 35, ValueEnd: 102},
	}
}

// rootChildOffset returns the offset of the root's left child field, the right
// child following 8 bytes later. Node layout:
// [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8].
func rootChildOffset(data []byte) int {
	return 4 + int(binary.LittleEndian.Uint32(data[0:4])) + 16
}

// A corrupt or truncated on-disk index must never crash the node: every read
// path (Get, Seek/Next, AllKeys) has to return NotFound or an error instead of
// panicking on an out-of-range slice.
func TestDiskTreeCorruptDataNeverPanics(t *testing.T) {
	// Sorted keys; the median ("foobar") becomes the BST root, which the van Emde
	// Boas writer emits first and therefore places at offset 0.
	keys := fiveSortedKeys()
	var buf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&buf, keys)
	require.NoError(t, err)
	valid := buf.Bytes()
	require.Greater(t, len(valid), TREE_KEY_STORE_OVERHEAD)

	// The corruption cases below only mean something if the intact blob resolves.
	for _, k := range keys {
		node, err := NewDiskTree(valid).Get(k.Key)
		require.NoError(t, err)
		require.Equal(t, uint64(k.ValueStart), node.Start)
		require.Equal(t, uint64(k.ValueEnd), node.End)
	}

	// queries span match, descend-left and descend-right branches plus misses.
	queries := [][]byte{
		[]byte("aaa"), []byte("abc"), []byte("foobar"), []byte("zzz"), []byte("zzzz"),
		[]byte("a"), []byte("m"), []byte("zzzzz"), []byte(""),
	}

	t.Run("every truncation of the buffer", func(t *testing.T) {
		for trunc := 0; trunc <= len(valid); trunc++ {
			dTree := NewDiskTree(valid[:trunc])
			require.NotPanics(t, func() {
				_, _ = dTree.AllKeys()
			}, "AllKeys panicked at truncation=%d", trunc)
			for _, q := range queries {
				var getErr error
				require.NotPanics(t, func() {
					_, getErr = dTree.Get(q)
				}, "Get panicked at truncation=%d query=%q", trunc, q)
				var offsetsErr error
				require.NotPanics(t, func() {
					_, _, offsetsErr = dTree.GetOffsets(q)
				}, "GetOffsets panicked at truncation=%d query=%q", trunc, q)
				var contains bool
				require.NotPanics(t, func() {
					contains, _ = dTree.Contains(q)
				}, "Contains panicked at truncation=%d query=%q", trunc, q)
				// a truncated index must not answer NotFound, which a caller reads
				// as "no such key" — the tree it describes is unknowable, not empty
				var seekErr, nextErr error
				require.NotPanics(t, func() {
					_, seekErr = dTree.Seek(q)
				}, "Seek panicked at truncation=%d query=%q", trunc, q)
				require.NotPanics(t, func() {
					_, nextErr = dTree.Next(q)
				}, "Next panicked at truncation=%d query=%q", trunc, q)
				// a probe above every key runs off the right spine, where NotFound
				// is the honest answer rather than a swallowed corruption
				if trunc > 0 && trunc < len(valid) && bytes.Compare(q, []byte("zzzz")) <= 0 {
					require.NotErrorIs(t, seekErr, lsmkv.NotFound,
						"Seek reported absence at truncation=%d query=%q", trunc, q)
					require.NotErrorIs(t, nextErr, lsmkv.NotFound,
						"Next reported absence at truncation=%d query=%q", trunc, q)
					// Get answers NotFound for any key the tree does not hold, so only
					// a key it does hold can expose a swallowed corruption
					if slices.ContainsFunc(keys, func(k Key) bool { return bytes.Equal(k.Key, q) }) {
						require.NotErrorIs(t, getErr, lsmkv.NotFound,
							"Get reported a stored key absent at truncation=%d query=%q", trunc, q)
						require.NotErrorIs(t, offsetsErr, lsmkv.NotFound,
							"GetOffsets reported a stored key absent at truncation=%d query=%q", trunc, q)
						// Contains folds absence into (false, nil), so a swallowed
						// corruption leaves no error to inspect at all
						require.False(t, contains && getErr != nil,
							"Contains and Get disagree at truncation=%d query=%q", trunc, q)
					}
				}
			}
		}
	})

	t.Run("corrupt keyLen larger than the buffer returns an error", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		binary.LittleEndian.PutUint32(corrupt[0:4], 0xFFFFFFFF)
		dTree := NewDiskTree(corrupt)

		var getErr, allErr error
		require.NotPanics(t, func() {
			_, getErr = dTree.Get([]byte("foobar"))
			_, allErr = dTree.AllKeys()
			_, _ = dTree.Seek([]byte("foobar"))
			_, _ = dTree.Next([]byte("foobar"))
		})
		require.Error(t, getErr)
		require.Error(t, allErr)
	})

	t.Run("corrupt child pointers do not panic", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		// root node at offset 0: [keyLen:4][key][start:8][end:8][left:8][right:8].
		childBase := rootChildOffset(corrupt)
		binary.LittleEndian.PutUint64(corrupt[childBase:], 0xFFFFFFFFFFFFFFFC)   // left child
		binary.LittleEndian.PutUint64(corrupt[childBase+8:], 0xFFFFFFFFFFFFFFF0) // right child
		dTree := NewDiskTree(corrupt)

		require.NotPanics(t, func() {
			_, _ = dTree.Get([]byte("aaa"))   // descends left into the bad pointer
			_, _ = dTree.Get([]byte("zzzzz")) // descends right into the bad pointer
			_, _ = dTree.Seek([]byte("aaa"))
			_, _ = dTree.Next([]byte("zzzzz"))
		})
	})
}

// fuzzReadDeadline bounds one input's worth of read calls. The reads follow
// child pointers taken straight from the data, so a pointer that leads back to
// an already-visited node keeps the descent going forever. Go's fuzzer has no
// hang detection, so without this the run would sit on such an input until the
// whole test binary times out.
const fuzzReadDeadline = 10 * time.Second

// FuzzDiskTreeRead feeds arbitrary bytes to every DiskTree read path. The bytes
// come off disk, so a torn write or a bad sector can produce any of them, and
// none of them may take the node down: reads must return NotFound or an error.
//
// TestDiskTreeCorruptDataNeverPanics covers the same ground for truncations of
// one blob and two hand-placed corruptions. The fuzzer reaches what those miss:
// corruption below the root, a child pointer landing inside a key rather than
// out of bounds, and node headers that no valid writer emits.
//
// The seed corpus runs on every `go test`; mutation needs an explicit run:
//
//	go test -run x -fuzz FuzzDiskTreeRead ./adapters/repos/db/lsmkv/segmentindex/
func FuzzDiskTreeRead(f *testing.F) {
	keys := fiveSortedKeys()
	var vebBuf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&vebBuf, keys)
	require.NoError(f, err)
	veb := vebBuf.Bytes()

	// The level-order writer is still reachable through Tree, and its root sits at
	// a different offset, so seed both layouts.
	var levelBuf bytes.Buffer
	levelTree := NewBalanced(primaryNodes(keys))
	_, err = levelTree.MarshalBinaryInto(&levelBuf)
	require.NoError(f, err)

	corruptKeyLen := bytes.Clone(veb)
	binary.LittleEndian.PutUint32(corruptKeyLen[0:4], 0xFFFFFFFF)

	// Root child pointers a descent cannot follow: one negative, one past the
	// end of the buffer.
	corruptChildren := bytes.Clone(veb)
	childBase := rootChildOffset(veb)
	binary.LittleEndian.PutUint64(corruptChildren[childBase:], 0xFFFFFFFFFFFFFFFC)
	binary.LittleEndian.PutUint64(corruptChildren[childBase+8:], uint64(len(veb))+1)

	// Root's left child points back at the root, the shape that makes a descent
	// loop rather than run out of buffer.
	cyclic := bytes.Clone(veb)
	binary.LittleEndian.PutUint64(cyclic[childBase:], 0)

	// a segment written with checksums leaves a 4-byte trailer on the index blob
	// when no secondary index bounds it
	withChecksumTail := append(bytes.Clone(veb), 0xDE, 0xAD, 0xBE, 0xEF)

	blobs := [][]byte{
		nil,
		{},
		{0x01, 0x02, 0x03},
		veb,
		withChecksumTail,
		cyclic,
		levelBuf.Bytes(),
		veb[:len(veb)-1],
		veb[:len(veb)/2],
		veb[:TREE_KEY_STORE_OVERHEAD],
		veb[:4],
		corruptKeyLen,
		corruptChildren,
	}
	// Queries covering a match, both descent directions, and both ends of the
	// key range.
	queries := [][]byte{nil, []byte("aaa"), []byte("foobar"), []byte("m"), []byte("zzzzz")}

	for _, blob := range blobs {
		for _, query := range queries {
			f.Add(blob, query)
		}
	}

	f.Fuzz(func(t *testing.T, data, query []byte) {
		tree := NewDiskTree(data)

		var (
			allKeys     [][]byte
			allKeysErr  error
			eachKeys    [][]byte
			keyCount    int
			getErr      error
			getFound    bool
			contains    bool
			containsErr error
			ranged      [][]byte
			rangedErr   error
		)
		requireTerminates(t, func() {
			var node Node
			node, getErr = tree.Get(query)
			getFound = getErr == nil
			gStart, gEnd, gErr := tree.GetOffsets(query)
			// GetOffsets is Get's descent without the key, so the two cannot
			// disagree on a blob, however malformed
			assert.Equal(t, getErr == nil, gErr == nil, "Get and GetOffsets disagree")
			if getFound && gErr == nil {
				assert.Equal(t, node.Start, gStart)
				assert.Equal(t, node.End, gEnd)
			}

			_, _ = tree.Seek(query)
			_, _ = tree.Next(query)
			contains, containsErr = tree.Contains(query)
			allKeys, allKeysErr = tree.AllKeys()
			keyCount = tree.KeyCount()
			tree.ForEachKey(func(key []byte) {
				eachKeys = append(eachKeys, key)
			})
			_ = tree.QuantileKeys(8)

			// the ranges must tile the blob, so walking them all visits every node
			for _, r := range tree.SplitNodeRanges(4) {
				if err := tree.ForEachNodeInRange(r[0], r[1], func(key []byte, _, _ uint64) error {
					ranged = append(ranged, key)
					return nil
				}); err != nil {
					rangedErr = err
					break
				}
			}
		})

		// Contains shares Get's descent, so their verdicts cannot diverge
		if getFound {
			assert.NoError(t, containsErr)
			assert.True(t, contains, "Contains missed a key Get found")
		} else if errors.Is(getErr, lsmkv.NotFound) {
			assert.NoError(t, containsErr)
			assert.False(t, contains, "Contains found a key Get reported absent")
		} else {
			assert.Error(t, containsErr, "Get errored but Contains did not")
		}

		if allKeysErr != nil {
			// AllKeys reports a node header it cannot parse, where KeyCount and
			// ForEachKey stop walking, so the three legitimately disagree here.
			return
		}
		// All three walk the blob sequentially with their own copy of the node-size
		// arithmetic, so they must agree on what the blob holds.
		assert.Equal(t, len(allKeys), keyCount, "KeyCount disagrees with AllKeys")
		assert.Equal(t, allKeys, eachKeys, "ForEachKey disagrees with AllKeys")

		// SplitNodeRanges tiles the blob and both walkers stop at the same tail, so
		// the ranges must reproduce AllKeys exactly — no node skipped at a boundary,
		// none visited twice.
		assert.NoError(t, rangedErr, "range walk rejected a blob AllKeys accepted")
		assert.Equal(t, allKeys, ranged, "ForEachNodeInRange disagrees with AllKeys")
	})
}

// requireTerminates fails the test if fn panics or exceeds fuzzReadDeadline,
// instead of taking the process down or hanging the fuzzer.
func requireTerminates(t *testing.T, fn func()) {
	t.Helper()

	type failure struct {
		value any
		stack []byte
	}

	done := make(chan *failure, 1)
	go func() {
		var caught *failure
		defer func() {
			if r := recover(); r != nil {
				caught = &failure{value: r, stack: debug.Stack()}
			}
			done <- caught
		}()
		fn()
	}()

	// A timer rather than time.After: fuzzing runs this millions of times and
	// stopping the timer releases it right away instead of at the deadline.
	timer := time.NewTimer(fuzzReadDeadline)
	defer timer.Stop()

	select {
	case caught := <-done:
		if caught != nil {
			t.Fatalf("read panicked: %v\n%s", caught.value, caught.stack)
		}
	case <-timer.C:
		t.Fatalf("read did not terminate within %s", fuzzReadDeadline)
	}
}

// TestDiskTreeCorruptChildPointerErrors pins that a child pointer past the
// buffer errors from every read path, where NotFound would make a caller drop
// keys that are there.
func TestDiskTreeCorruptChildPointerErrors(t *testing.T) {
	keys := fiveSortedKeys()
	valid := marshalTree(t, keys)

	childBase := rootChildOffset(valid)

	branches := []struct {
		name   string
		offset int
		probe  []byte
		child  uint64
		// wantErr names the arm the row is there to reach, where require.Error
		// alone would be satisfied by any of them
		wantErr string
	}{
		{name: "left child past the buffer", offset: childBase, probe: []byte("aaa"), child: uint64(len(valid)) + 1},
		{name: "right child past the buffer", offset: childBase + 8, probe: []byte("zzzz"), child: uint64(len(valid)) + 1},
		// in range, but landing inside the root's own key rather than on a node
		{name: "left child mid-node", offset: childBase, probe: []byte("aaa"), child: 5},
		{name: "right child mid-node", offset: childBase + 8, probe: []byte("zzzz"), child: 5},
		// negative, but not the -1 the writers store for an absent child: read as
		// a leaf, these would report a live key as absent
		{name: "left child bit 63", offset: childBase, probe: []byte("aaa"), child: 1 << 63},
		{name: "right child bit 63", offset: childBase + 8, probe: []byte("zzzz"), child: 1 << 63},
		{name: "left child all ones but one", offset: childBase, probe: []byte("aaa"), child: 0xFFFFFFFFFFFFFFF0},
		// -4 wraps pos+4 back under dataLen, so the bounds check lets it through
		// and only the sign check stops it reading at an enormous offset
		{
			name: "left child just below the sentinel", offset: childBase,
			probe: []byte("aaa"), child: 0xFFFFFFFFFFFFFFFC,
			wantErr: "has child pointer -4",
		},
		{
			name: "right child just below the sentinel", offset: childBase + 8,
			probe: []byte("zzzz"), child: 0xFFFFFFFFFFFFFFFC,
			wantErr: "has child pointer -4",
		},
	}

	for _, branch := range branches {
		t.Run(branch.name, func(t *testing.T) {
			corrupt := bytes.Clone(valid)
			binary.LittleEndian.PutUint64(corrupt[branch.offset:], branch.child)
			tree := NewDiskTree(corrupt)

			reads := []struct {
				name string
				read func() error
			}{
				{"Get", func() error { _, err := tree.Get(branch.probe); return err }},
				{"GetOffsets", func() error { _, _, err := tree.GetOffsets(branch.probe); return err }},
				{"Contains", func() error { _, err := tree.Contains(branch.probe); return err }},
				{"Seek", func() error { _, err := tree.Seek(branch.probe); return err }},
				{"Next", func() error { _, err := tree.Next(branch.probe); return err }},
			}
			for _, read := range reads {
				t.Run(read.name, func(t *testing.T) {
					err := read.read()
					require.Error(t, err)
					require.NotErrorIs(t, err, lsmkv.NotFound,
						"a corrupt index must not read as an absent key")
					if branch.wantErr != "" {
						require.ErrorContains(t, err, branch.wantErr)
					}
				})
			}
		})
	}
}

// TestDiskTreeShortBufferErrors pins that a buffer too short to hold one node
// errors rather than reporting NotFound, which a caller reads as "no such key".
// Only an entirely empty buffer is an empty tree.
func TestDiskTreeShortBufferErrors(t *testing.T) {
	reads := []struct {
		name string
		read func(t *DiskTree) error
	}{
		{"Get", func(t *DiskTree) error { _, err := t.Get([]byte("aaa")); return err }},
		{"GetOffsets", func(t *DiskTree) error { _, _, err := t.GetOffsets([]byte("aaa")); return err }},
		{"Contains", func(t *DiskTree) error { _, err := t.Contains([]byte("aaa")); return err }},
		{"Seek", func(t *DiskTree) error { _, err := t.Seek([]byte("aaa")); return err }},
		{"Next", func(t *DiskTree) error { _, err := t.Next([]byte("aaa")); return err }},
	}

	for _, size := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			tree := NewDiskTree(make([]byte, size))
			for _, read := range reads {
				t.Run(read.name, func(t *testing.T) {
					err := read.read(tree)
					require.Error(t, err)
					require.NotErrorIs(t, err, lsmkv.NotFound)
				})
			}
		})
	}

	t.Run("empty buffer is an empty tree", func(t *testing.T) {
		tree := NewDiskTree(nil)
		for _, read := range reads {
			if read.name == "Contains" {
				continue // folds an absent key into (false, nil) by contract
			}
			t.Run(read.name, func(t *testing.T) {
				require.ErrorIs(t, read.read(tree), lsmkv.NotFound)
			})
		}

		t.Run("Contains", func(t *testing.T) {
			contains, err := tree.Contains([]byte("aaa"))
			require.NoError(t, err)
			require.False(t, contains)
		})
	})
}

// TestDiskTreeReversedPayloadRangeErrors pins that a node whose payload range
// runs backwards fails the read. Callers size their reads with end-start, so
// answering with it would wrap the subtraction to near 2^64 and panic the
// allocation — a crash where an error belongs.
func TestDiskTreeReversedPayloadRangeErrors(t *testing.T) {
	keys := fiveSortedKeys()
	valid := marshalTree(t, keys)

	// the root's own start/end sit right after its key
	rootValue := rootChildOffset(valid) - 16
	corrupt := bytes.Clone(valid)
	start := binary.LittleEndian.Uint64(corrupt[rootValue:])
	require.NotZero(t, start, "fixture must give the root a non-zero start")
	binary.LittleEndian.PutUint64(corrupt[rootValue+8:], start-1)

	tree := NewDiskTree(corrupt)
	probe := []byte("foobar") // the root key, so every mode ends on this node
	// Next never answers with the probe itself, so it needs one just below
	below := []byte("foobaq")

	reads := []struct {
		name string
		read func() error
	}{
		{"Get", func() error { _, err := tree.Get(probe); return err }},
		{"GetOffsets", func() error { _, _, err := tree.GetOffsets(probe); return err }},
		{"Seek", func() error { _, err := tree.Seek(probe); return err }},
		{"Next", func() error { _, err := tree.Next(below); return err }},
		// a caller that skips a lower segment's row on this answer would drop a
		// row no read path will serve
		{"Contains", func() error { _, err := tree.Contains(probe); return err }},
	}
	for _, read := range reads {
		t.Run(read.name, func(t *testing.T) {
			err := read.read()
			require.Error(t, err)
			require.NotErrorIs(t, err, lsmkv.NotFound)
		})
	}
}

// TestDiskTreeErrorsReportTheirBounds pins what a failing check tells the reader
// about itself. A message naming the buffer where the check compared the space
// left inside it sends whoever reads the log looking in the wrong place.
func TestDiskTreeErrorsReportTheirBounds(t *testing.T) {
	valid := marshalTree(t, fiveSortedKeys())

	tests := []struct {
		name  string
		blob  func() []byte
		probe string
		// boundsPair marks the messages that name an offset and the bytes left
		// beside it, which is what the arithmetic below can check
		boundsPair bool
		contains   []string
	}{
		{
			// truncated so the root's trailer is short of a right child
			name:       "child field short of its 32 bytes",
			blob:       func() []byte { return valid[:len(valid)-1] },
			probe:      "zzzzz",
			boundsPair: true,
			contains:   []string{"out of range", "bytes available", "need 32"},
		},
		{
			name: "key length past the buffer",
			blob: func() []byte {
				corrupt := bytes.Clone(valid)
				binary.LittleEndian.PutUint32(corrupt[0:4], 1<<30)
				return corrupt
			},
			probe:      "aaa",
			boundsPair: true,
			contains:   []string{"node key at", "bytes available"},
		},
		{
			name: "a child pointing back at the root",
			blob: func() []byte {
				cyclic := bytes.Clone(valid)
				binary.LittleEndian.PutUint64(cyclic[rootChildOffset(cyclic):], 0)
				return cyclic
			},
			probe:    "aaa",
			contains: []string{"cyclic child pointers", "past", "nodes at offset", "buffer"},
		},
		{
			// the root sits at offset 0, so a message naming anything else has
			// measured from somewhere other than the node it is reporting on
			name: "a child pointer that is neither valid nor the leaf sentinel",
			blob: func() []byte {
				corrupt := bytes.Clone(valid)
				binary.LittleEndian.PutUint64(corrupt[rootChildOffset(corrupt):], 0xFFFFFFFFFFFFFFFC)
				return corrupt
			},
			probe:    "aaa",
			contains: []string{"node at 0 has child pointer -4"},
		},
	}

	// an offset and a count of bytes left have to be measured from the same
	// place, so that adding them lands on the end of the buffer
	pair := regexp.MustCompile(`at (\d+).*\((\d+) bytes available`)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blob := test.blob()
			_, err := NewDiskTree(blob).Get([]byte(test.probe))
			require.Error(t, err)
			for _, want := range test.contains {
				require.Contains(t, err.Error(), want)
			}

			m := pair.FindStringSubmatch(err.Error())
			require.Equal(t, test.boundsPair, m != nil,
				"%q does not match what the row declares about naming a bounds pair", err.Error())
			if m != nil {
				at, convErr := strconv.Atoi(m[1])
				require.NoError(t, convErr)
				avail, convErr := strconv.Atoi(m[2])
				require.NoError(t, convErr)
				require.Equal(t, len(blob), at+avail,
					"%q sends a reader %d bytes past the end of a %d-byte buffer",
					err.Error(), at+avail-len(blob), len(blob))
			}
		})
	}
}

// TestDiskTreeSeekAndNext checks Seek and Next against a linear scan of the same
// keys, over tree shapes that put the answer above, below and on the path the
// descent takes. Both on-disk layouts are covered.
func TestDiskTreeSeekAndNext(t *testing.T) {
	tests := []struct {
		name string
		keys []Key
	}{
		{name: "single key", keys: contiguousKeys([][]byte{[]byte("m")})},
		{name: "two keys", keys: contiguousKeys([][]byte{[]byte("a"), []byte("b")})},
		{
			name: "three keys, one full level",
			keys: contiguousKeys([][]byte{[]byte("a"), []byte("b"), []byte("c")}),
		},
		{
			name: "prefixes of one another",
			keys: contiguousKeys([][]byte{[]byte("a"), []byte("ab"), []byte("abc"), []byte("b")}),
		},
		{
			// the widths the 8-byte word compare applies to, and the widths it does
			// not, in one tree
			name: "mixed key widths",
			keys: contiguousKeys([][]byte{
				{0x01},
				{0x01, 0x02, 0x03, 0x04},
				{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
				{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
				bytes.Repeat([]byte{0xff}, 16),
			}),
		},
		{name: "docID keys", keys: docIDKeys(64)},
		{
			// int and date keys carry the high bit for every non-negative value,
			// so the 8-byte word compare has to read them unsigned
			name: "sortable int keys",
			keys: sortableIntKeys([]int64{
				math.MinInt64, -1 << 40, -257, -1, 0, 1, 257, 1 << 40, math.MaxInt64,
			}),
		},
		{name: "variable width keys", keys: variedKeys(50)},
	}

	for _, test := range tests {
		for _, layout := range treeLayouts(t, test.keys) {
			t.Run(test.name+"/"+layout.name, func(t *testing.T) {
				tree := NewDiskTree(layout.data)

				for _, probe := range seekProbes(test.keys) {
					t.Run(fmt.Sprintf("probe=%x", probe), func(t *testing.T) {
						assertSeek(t, tree, test.keys, probe)
						assertNext(t, tree, test.keys, probe)
					})
				}
			})
		}
	}
}

// TestDiskTreeSeekReturnsCopy pins that the returned key does not alias the tree
// data, which for a mmapped segment would let a caller corrupt the index and
// hand it a key that outlives the segment.
func TestDiskTreeSeekReturnsCopy(t *testing.T) {
	keys := variedKeys(10)
	tree := NewDiskTree(marshalTree(t, keys))

	for _, seek := range []struct {
		name string
		fn   func(key []byte) (Node, error)
	}{
		{"Seek", tree.Seek},
		{"Next", tree.Next},
	} {
		t.Run(seek.name, func(t *testing.T) {
			node, err := seek.fn(keys[0].Key)
			require.NoError(t, err)
			original := bytes.Clone(node.Key)

			node.Key[0] ^= 0xff

			again, err := seek.fn(keys[0].Key)
			require.NoError(t, err)
			require.Equal(t, original, again.Key)
		})
	}
}

// TestDiskTreeSeekAllocatesOnlyTheKey pins the descent as allocation-free — a
// per-level key copy costs correctness nothing, so it could return unnoticed.
func TestDiskTreeSeekAllocatesOnlyTheKey(t *testing.T) {
	keys := docIDKeys(4096)
	tree := NewDiskTree(marshalTree(t, keys))
	probe := keys[len(keys)/3].Key

	for _, seek := range []struct {
		name string
		fn   func(key []byte) (Node, error)
	}{
		{"Seek", tree.Seek},
		{"Next", tree.Next},
	} {
		t.Run(seek.name, func(t *testing.T) {
			_, err := seek.fn(probe)
			require.NoError(t, err)

			allocs := testing.AllocsPerRun(100, func() {
				_, _ = seek.fn(probe)
			})
			require.LessOrEqual(t, allocs, 1.0)
		})
	}

	offsets := []struct {
		name string
		fn   func(key []byte) (uint64, uint64, error)
	}{
		{"GetOffsets", tree.GetOffsets},
	}
	for _, read := range offsets {
		t.Run(read.name, func(t *testing.T) {
			_, _, err := read.fn(probe)
			require.NoError(t, err)

			allocs := testing.AllocsPerRun(100, func() {
				_, _, _ = read.fn(probe)
			})
			require.Zero(t, allocs, "%s materializes nothing", read.name)
		})
	}
}

func assertSeek(t *testing.T, tree *DiskTree, keys []Key, probe []byte) {
	t.Helper()

	want, found := firstAtOrAbove(keys, probe)
	node, err := tree.Seek(probe)
	if !found {
		require.ErrorIs(t, err, lsmkv.NotFound, "Seek past the last key")
		return
	}

	require.NoError(t, err, "Seek(%x)", probe)
	require.Equal(t, want.Key, node.Key, "Seek(%x) returned the wrong key", probe)
	require.Equal(t, uint64(want.ValueStart), node.Start)
	require.Equal(t, uint64(want.ValueEnd), node.End)
}

func assertNext(t *testing.T, tree *DiskTree, keys []Key, probe []byte) {
	t.Helper()

	want, found := firstAbove(keys, probe)
	node, err := tree.Next(probe)
	if !found {
		require.ErrorIs(t, err, lsmkv.NotFound, "Next past the last key")
		return
	}

	require.NoError(t, err, "Next(%x)", probe)
	require.Equal(t, want.Key, node.Key, "Next(%x) returned the wrong key", probe)
	require.Equal(t, uint64(want.ValueStart), node.Start)
	require.Equal(t, uint64(want.ValueEnd), node.End)
}

// firstAtOrAbove and firstAbove are the linear-scan answers Seek and Next have
// to reproduce.
func firstAtOrAbove(keys []Key, probe []byte) (Key, bool) {
	for _, key := range keys {
		if bytes.Compare(key.Key, probe) >= 0 {
			return key, true
		}
	}
	return Key{}, false
}

func firstAbove(keys []Key, probe []byte) (Key, bool) {
	for _, key := range keys {
		if bytes.Compare(key.Key, probe) > 0 {
			return key, true
		}
	}
	return Key{}, false
}

// seekProbes returns each key plus the probes just below, just above and one
// byte short of it, and four probes outside the key range. Derived probes
// collide across keys, and a repeat walks a descent already covered, so they
// are collapsed; the four are kept as written, nil and empty among them.
func seekProbes(keys []Key) [][]byte {
	probes := [][]byte{nil, {}, {0x00}, bytes.Repeat([]byte{0xff}, 17)}

	seen := make(map[string]struct{}, 4*len(keys))
	add := func(probe []byte) {
		if _, ok := seen[string(probe)]; ok {
			return
		}
		seen[string(probe)] = struct{}{}
		probes = append(probes, probe)
	}

	for _, key := range keys {
		add(key.Key)
		add(append(bytes.Clone(key.Key), 0x00))
		if len(key.Key) == 0 {
			continue
		}
		if last := key.Key[len(key.Key)-1]; last > 0 {
			below := bytes.Clone(key.Key)
			below[len(below)-1] = last - 1
			add(below)
		}
		if len(key.Key) > 1 {
			add(key.Key[:len(key.Key)-1])
		}
	}
	return probes
}

// sortableIntKeys returns the keys an int or date property produces: an int64
// flipped at bit 63 so the signed order survives an unsigned byte compare, as
// entities/inverted writes them. Every non-negative value sets the high bit,
// which the docID fixture never does.
func sortableIntKeys(values []int64) []Key {
	raw := make([][]byte, len(values))
	for i, v := range values {
		raw[i] = binary.BigEndian.AppendUint64(nil, uint64(v)^(1<<63))
	}
	return contiguousKeys(raw)
}

// contiguousKeys pairs each key with its own payload range, so a descent that
// lands on the wrong node shows up as a wrong Start rather than a wrong key.
func contiguousKeys(raw [][]byte) []Key {
	keys := make([]Key, len(raw))
	for i, key := range raw {
		keys[i] = Key{Key: key, ValueStart: i * 10, ValueEnd: (i + 1) * 10}
	}
	return keys
}

// treeLayouts serializes the same keys with both writers, whose node placement
// differs, so a descent is exercised against both on-disk orders.
func treeLayouts(t testing.TB, keys []Key) []struct {
	name string
	data []byte
} {
	t.Helper()

	var levelOrder bytes.Buffer
	balanced := NewBalanced(primaryNodes(keys))
	_, err := balanced.MarshalBinaryInto(&levelOrder)
	require.NoError(t, err)

	return []struct {
		name string
		data []byte
	}{
		{"van-Emde-Boas", marshalTree(t, keys)},
		{"level-order", levelOrder.Bytes()},
	}
}

// randomProbes draws count keys from keys in a fixed random order, spreading
// lookups across the index so its layout rather than a few permanently-hot
// pages drives the result. count must be a power of two for benchmarkReads.
func randomProbes(keys []Key, count, seed int) [][]byte {
	rng := rand.New(rand.NewSource(int64(seed)))
	probes := make([][]byte, count)
	for i := range probes {
		probes[i] = keys[rng.Intn(len(keys))].Key
	}
	return probes
}

func benchmarkReads(b *testing.B, probes [][]byte, read func(key []byte) error) {
	b.Helper()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := read(probes[i&(len(probes)-1)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiskTreeGet compares warm lookup latency between the two on-disk node
// orders the writers can produce: level order (Tree.MarshalBinaryInto) and van
// Emde Boas (MarshalSortedKeysFromKeys, the production layout). Both blobs are
// read through the same DiskTree, so the only difference is node placement. The
// whole index is resident in RAM, so this isolates the CPU-cache/TLB locality
// effect; the larger page-fault win under partial residency needs real I/O and
// is not modelled here.
//
// Keys are 8-byte big-endian docIDs, as the binary-quantized vector store
// writes them, and the win grows with n as the index outgrows the CPU caches.
//
// Run with: go test -run x -bench BenchmarkDiskTreeGet ./adapters/repos/db/lsmkv/segmentindex/
//
// For a single size anchor the sub-benchmark name — an unanchored n=100000 also
// matches n=1000000 and n=10000000:
//
//	go test -run x -bench 'BenchmarkDiskTreeGet/^n=100000$' ./adapters/repos/db/lsmkv/segmentindex/
func BenchmarkDiskTreeGet(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000, 10_000_000} {
		// Setup lives inside the sub-benchmark so selecting one n does not build
		// the blobs for the others (the 10M case costs GBs and minutes).
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := docIDKeys(n)

			// one probe order, shared across layouts for a fair comparison
			probes := randomProbes(keys, 65536, n)

			for _, l := range treeLayouts(b, keys) {
				tree := NewDiskTree(l.data)
				b.Run(l.name, func(b *testing.B) {
					benchmarkReads(b, probes, func(key []byte) error {
						_, err := tree.Get(key)
						return err
					})
				})
			}
		})
	}
}

// BenchmarkDiskTreeSeek measures the descent that positions every segment
// cursor. Both probe sets matter: a key the index holds takes the match path, a
// key between two it holds descends to a leaf and answers with the key above.
func BenchmarkDiskTreeSeek(b *testing.B) {
	for _, n := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			keys := docIDKeys(n)
			tree := NewDiskTree(marshalTree(b, keys))

			present := randomProbes(keys, 65536, n)
			// a 9-byte key sorts between the docID it extends and the next one, so
			// the descent can never match
			between := make([][]byte, len(present))
			for i, probe := range randomProbes(keys, 65536, n+1) {
				between[i] = append(bytes.Clone(probe), 0x01)
			}

			probeSets := []struct {
				name   string
				probes [][]byte
			}{{"present", present}, {"between", between}}

			for _, set := range probeSets {
				probes := set.probes
				b.Run("Seek/"+set.name, func(b *testing.B) {
					benchmarkReads(b, probes, func(key []byte) error {
						_, err := tree.Seek(key)
						return err
					})
				})
				b.Run("Next/"+set.name, func(b *testing.B) {
					benchmarkReads(b, probes, func(key []byte) error {
						_, err := tree.Next(key)
						return err
					})
				})
			}
		})
	}
}
