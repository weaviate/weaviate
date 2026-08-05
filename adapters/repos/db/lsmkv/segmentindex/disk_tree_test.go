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
	"math/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// A corrupt or truncated on-disk index must never crash the node: every read
// path (Get, Seek/Next, AllKeys) has to return NotFound or an error instead of
// panicking on an out-of-range slice.
func TestDiskTreeCorruptDataNeverPanics(t *testing.T) {
	// Sorted keys; the median ("foobar") becomes the BST root, which the van Emde
	// Boas writer emits first and therefore places at offset 0.
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: 1, ValueEnd: 2},
		{Key: []byte("abc"), ValueStart: 4, ValueEnd: 5},
		{Key: []byte("foobar"), ValueStart: 17, ValueEnd: 18},
		{Key: []byte("zzz"), ValueStart: 34, ValueEnd: 35},
		{Key: []byte("zzzz"), ValueStart: 100, ValueEnd: 102},
	}
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
				require.NotPanics(t, func() {
					_, _ = dTree.Get(q)
				}, "Get panicked at truncation=%d query=%q", trunc, q)
				require.NotPanics(t, func() {
					_, _ = dTree.Seek(q)
				}, "Seek panicked at truncation=%d query=%q", trunc, q)
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
		})
		require.Error(t, getErr)
		require.Error(t, allErr)
	})

	t.Run("corrupt child pointers do not panic", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		// root node at offset 0: [keyLen:4][key][start:8][end:8][left:8][right:8].
		keyLen := int(binary.LittleEndian.Uint32(corrupt[0:4]))
		childBase := 4 + keyLen + 16                                             // past keyLen + key + start + end
		binary.LittleEndian.PutUint64(corrupt[childBase:], 0xFFFFFFFFFFFFFFFF)   // left child
		binary.LittleEndian.PutUint64(corrupt[childBase+8:], 0xFFFFFFFFFFFFFFF0) // right child
		dTree := NewDiskTree(corrupt)

		require.NotPanics(t, func() {
			_, _ = dTree.Get([]byte("aaa"))   // descends left into the bad pointer
			_, _ = dTree.Get([]byte("zzzzz")) // descends right into the bad pointer
			_, _ = dTree.Seek([]byte("aaa"))
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
	keys := []Key{
		{Key: []byte("aaa"), ValueStart: 1, ValueEnd: 2},
		{Key: []byte("abc"), ValueStart: 4, ValueEnd: 5},
		{Key: []byte("foobar"), ValueStart: 17, ValueEnd: 18},
		{Key: []byte("zzz"), ValueStart: 34, ValueEnd: 35},
		{Key: []byte("zzzz"), ValueStart: 100, ValueEnd: 102},
	}
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

	// Root child pointers past the end of the buffer.
	corruptChildren := bytes.Clone(veb)
	childBase := 4 + int(binary.LittleEndian.Uint32(veb[0:4])) + 16
	binary.LittleEndian.PutUint64(corruptChildren[childBase:], 0xFFFFFFFFFFFFFFFF)
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
			_, getErr = tree.Get(query)
			getFound = getErr == nil
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

		// The range walk is stricter than the sequential ones: they stop at a
		// sub-node tail, it reports one. So it must succeed exactly when the nodes
		// account for every byte, and then visit the same nodes — a stray checksum
		// trailer left on the index blob is what makes the two diverge.
		consumed := 0
		for _, k := range allKeys {
			consumed += len(k) + TREE_KEY_STORE_OVERHEAD
		}
		if consumed == len(data) {
			assert.NoError(t, rangedErr, "range walk rejected a fully-parsed blob")
			assert.Equal(t, allKeys, ranged, "ForEachNodeInRange disagrees with AllKeys")
		} else {
			assert.Error(t, rangedErr, "range walk accepted a blob with a %d-byte tail",
				len(data)-consumed)
		}
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
			levelOrder := NewBalanced(primaryNodes(keys))
			var levelBuf bytes.Buffer
			_, err := levelOrder.MarshalBinaryInto(&levelBuf)
			require.NoError(b, err)

			var vebBuf bytes.Buffer
			_, err = MarshalSortedKeysFromKeys(&vebBuf, keys)
			require.NoError(b, err)

			// Fixed random lookup order, shared across layouts for a fair comparison.
			// A wide probe set spreads lookups across the index so its layout, rather
			// than a few permanently-hot pages, drives the result. Length is a power of
			// two for the cheap index mask below.
			rng := rand.New(rand.NewSource(int64(n)))
			probes := make([][]byte, 65536)
			for i := range probes {
				probes[i] = keys[rng.Intn(n)].Key
			}

			layouts := []struct {
				name string
				data []byte
			}{
				{"level-order", levelBuf.Bytes()},
				{"van-Emde-Boas", vebBuf.Bytes()},
			}
			for _, l := range layouts {
				tree := NewDiskTree(l.data)
				b.Run(l.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := tree.Get(probes[i&(len(probes)-1)]); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
